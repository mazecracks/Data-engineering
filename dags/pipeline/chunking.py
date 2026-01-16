import csv
import io
import json
from dataclasses import dataclass
from typing import List

from botocore.exceptions import ClientError
from config import settings as app_config

from .config import PipelineConfig
from .s3_client import get_s3, s3_exists, upload_fileobj
from .paths import s3_base_prefix


@dataclass(frozen=True)
class ChunkingResult:
    flow: str
    year: int
    source_key: str
    chunk_prefix: str
    chunk_keys: List[str]
    skipped: bool


def transformed_key(flow: str, year: int, transformed_prefix: str) -> str:
    flow_u = (flow or "").strip().upper()
    prefix = transformed_prefix.strip("/")
    return f"{prefix}/{flow_u}/{flow_u}_{int(year)}.csv"


def chunked_prefix(flow: str, transformed_prefix: str) -> str:
    flow_u = (flow or "").strip().upper()
    prefix = transformed_prefix.strip("/")
    return f"{prefix}/{flow_u}/chunked/"


def chunk_key(flow: str, year: int, chunk_index: int, transformed_prefix: str) -> str:
    """
    chunk_index is 1-based
    transformed/<FLOW>/chunked/<FLOW>_<YEAR>_CHUNK_<n>.csv
    """
    flow_u = (flow or "").strip().upper()
    return f"{chunked_prefix(flow_u, transformed_prefix=transformed_prefix)}{flow_u}_{int(year)}_CHUNK_{int(chunk_index)}.csv"


def chunking_success_marker(flow: str, year: int, transformed_prefix: str) -> str:
    """
    Marker for idempotency: if exists, we skip chunking for that flow/year.
    """
    flow_u = (flow or "").strip().upper()
    prefix = transformed_prefix.strip("/")
    return f"{prefix}/{flow_u}/chunked/{flow_u}_{int(year)}_CHUNKING_SUCCESS.json"


def _write_chunk(cfg: PipelineConfig, key_: str, header: List[str], rows: List[List[str]]) -> None:
    bio = io.StringIO()
    writer = csv.writer(bio)
    writer.writerow(header)
    writer.writerows(rows)
    bio.seek(0)

    upload_fileobj(
        cfg=cfg,
        key_=key_,
        fileobj=io.BytesIO(bio.getvalue().encode("utf-8")),
        content_type="text/csv",
        content_encoding=None,
    )


def chunk_one_year(cfg: PipelineConfig, flow: str, year: int) -> ChunkingResult:
    """
    Reads <base>/transformed/<FLOW>/<FLOW>_<YEAR>.csv from S3 and writes chunk files to:
      <base>/transformed/<FLOW>/chunked/<FLOW>_<YEAR>_CHUNK_<n>.csv

    - Streaming read (does not load full file into memory)
    - Skip if marker exists (idempotent)
    - Chunk size from settings.py: ROWS_PER_CHUNK
    """
    flow_u = (flow or "").strip().upper()
    rows_per_chunk = int(getattr(app_config, "ROWS_PER_CHUNK", 60000))

    # Resolve prefix with cfg.s3_prefix (e.g. "imf/transformed")
    logical = getattr(app_config, "S3_TRANSFORMED_PREFIX", "transformed")
    transformed_prefix = s3_base_prefix(cfg, logical)

    source_key = transformed_key(flow_u, year, transformed_prefix=transformed_prefix)
    marker_key = chunking_success_marker(flow_u, year, transformed_prefix=transformed_prefix)
    out_prefix = chunked_prefix(flow_u, transformed_prefix=transformed_prefix)

    # Idempotency: if chunking already done, skip
    if s3_exists(cfg, marker_key):
        return ChunkingResult(
            flow=flow_u,
            year=int(year),
            source_key=source_key,
            chunk_prefix=out_prefix,
            chunk_keys=[],
            skipped=True,
        )

    # Ensure source exists
    if not s3_exists(cfg, source_key):
        raise FileNotFoundError(f"Transformed source not found: s3://{cfg.bucket}/{source_key}")

    s3 = get_s3(cfg)
    obj = s3.get_object(Bucket=cfg.bucket, Key=source_key)

    text_stream = io.TextIOWrapper(obj["Body"], encoding="utf-8")
    reader = csv.reader(text_stream)

    try:
        header = next(reader)
    except StopIteration:
        raise ValueError(f"Empty CSV (no header): s3://{cfg.bucket}/{source_key}")

    chunk_rows: List[List[str]] = []
    chunk_index = 1
    written_keys: List[str] = []

    for row in reader:
        chunk_rows.append(row)

        if len(chunk_rows) >= rows_per_chunk:
            out_key = chunk_key(flow_u, year, chunk_index, transformed_prefix=transformed_prefix)
            _write_chunk(cfg, out_key, header, chunk_rows)
            written_keys.append(out_key)

            chunk_rows = []
            chunk_index += 1

    # Final partial chunk
    if chunk_rows:
        out_key = chunk_key(flow_u, year, chunk_index, transformed_prefix=transformed_prefix)
        _write_chunk(cfg, out_key, header, chunk_rows)
        written_keys.append(out_key)

    # Write chunking marker (json)
    marker_payload = {
        "flow": flow_u,
        "year": int(year),
        "source_key": source_key,
        "chunk_prefix": out_prefix,
        "rows_per_chunk": rows_per_chunk,
        "chunks_written": len(written_keys),
        "chunk_keys": written_keys,
    }
    marker_body = (json.dumps(marker_payload, indent=2) + "\n").encode("utf-8")

    upload_fileobj(
        cfg=cfg,
        key_=marker_key,
        fileobj=io.BytesIO(marker_body),
        content_type="application/json",
        content_encoding=None,
    )

    return ChunkingResult(
        flow=flow_u,
        year=int(year),
        source_key=source_key,
        chunk_prefix=out_prefix,
        chunk_keys=written_keys,
        skipped=False,
    )


def get_chunk_keys_from_marker(cfg: PipelineConfig, flow: str, year: int) -> List[str]:
    """
    Reads the chunking marker JSON and returns the chunk_keys list.
    Marker-driven = source of truth.
    """
    flow_u = (flow or "").strip().upper()

    logical = getattr(app_config, "S3_TRANSFORMED_PREFIX", "transformed")
    transformed_prefix = s3_base_prefix(cfg, logical)

    marker_key = chunking_success_marker(flow_u, year, transformed_prefix=transformed_prefix)

    s3 = get_s3(cfg)
    try:
        obj = s3.get_object(Bucket=cfg.bucket, Key=marker_key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            raise FileNotFoundError(
                f"Chunking marker not found for flow={flow_u} year={year}: s3://{cfg.bucket}/{marker_key}"
            )
        raise

    body = obj["Body"].read().decode("utf-8")
    payload = json.loads(body)

    chunk_keys = payload.get("chunk_keys")
    if not chunk_keys or not isinstance(chunk_keys, list):
        raise ValueError(f"Invalid chunking marker (missing chunk_keys list): s3://{cfg.bucket}/{marker_key}")

    return [str(k) for k in chunk_keys]
