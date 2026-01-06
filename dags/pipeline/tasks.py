import time
import io
import csv
import gzip
import tempfile
import datetime as dt
import xml.etree.ElementTree as ET
from requests.exceptions import RequestException

from .config import PipelineConfig
from .http_client import get_session
from .rate_limiter import rate_limit
from .transform import iter_series_obs_rows
from .s3_client import build_s3_key, s3_exists, upload_fileobj
from .logging_utils import log_json


def process_flow_year(cfg: PipelineConfig, flow_name: str, flowref: str, year: int, run_id: str) -> dict:
    """
    Single responsibility: (flow, year) -> download -> transform -> upload -> return result dict
    """
    t0 = time.perf_counter()
    s3_key = build_s3_key(cfg, flow_name, year)
    s3_uri = f"s3://{cfg.bucket}/{s3_key}"
    ingested_at = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

    # Idempotency
    if cfg.skip_if_exists:
        try:
            if s3_exists(cfg, s3_key):
                return {
                    "flow": flow_name, "year": year, "status": "skipped_exists",
                    "s3_uri": s3_uri, "rows": None,
                    "seconds": round(time.perf_counter() - t0, 3),
                }
        except Exception as e:
            log_json("warning", event="s3_head_failed", flow=flow_name, year=year, s3_key=s3_key, error=str(e))

    # Build URL
    url = f"{cfg.base_url}/{flowref}/{cfg.key}/{cfg.sub_key}?startPeriod={year}&endPeriod={year}"

    # Rate limit + fetch
    rate_limit(cfg.requests_per_second)
    try:
        resp = get_session().get(url, timeout=cfg.http_timeout_seconds)
        resp.raise_for_status()
    except RequestException as e:
        return {
            "flow": flow_name, "year": year, "status": "request_failed",
            "error": str(e), "url": url,
            "seconds": round(time.perf_counter() - t0, 3),
        }

    # Stream CSV to spooled file (memory-safe; spills to disk if large)
    spooled = tempfile.SpooledTemporaryFile(
        max_size=cfg.spooled_max_mb * 1024 * 1024,
        mode="w+b"
    )

    rows_written = 0
    try:
        try:
            row_iter = iter_series_obs_rows(resp.content)
        except ET.ParseError as e:
            return {
                "flow": flow_name, "year": year, "status": "invalid_xml",
                "error": str(e),
                "seconds": round(time.perf_counter() - t0, 3),
            }

        # Prime iterator (to get headers)
        try:
            first = next(row_iter)
        except StopIteration:
            return {
                "flow": flow_name, "year": year, "status": "no_data",
                "s3_uri": None, "rows": 0,
                "seconds": round(time.perf_counter() - t0, 3),
            }

        first = dict(first)
        first.update({"flow": flow_name, "year": year, "ingested_at": ingested_at})
        fieldnames = list(first.keys())

        # Wrap output stream (gzip optional)
        if cfg.compress_gzip:
            gz = gzip.GzipFile(fileobj=spooled, mode="wb")
            text = io.TextIOWrapper(gz, encoding="utf-8", newline="")
            content_encoding = "gzip"
        else:
            text = io.TextIOWrapper(spooled, encoding="utf-8", newline="")
            content_encoding = None

        writer = csv.DictWriter(text, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerow(first)
        rows_written += 1

        for r in row_iter:
            rr = dict(r)
            rr.update({"flow": flow_name, "year": year, "ingested_at": ingested_at})
            writer.writerow(rr)
            rows_written += 1

        text.flush()
        text.detach()
        if cfg.compress_gzip:
            gz.close()

        # Upload
        spooled.seek(0)
        upload_fileobj(
            cfg=cfg,
            key_=s3_key,
            fileobj=spooled,
            content_type="text/csv; charset=utf-8",
            content_encoding=content_encoding,
        )

        return {
            "flow": flow_name, "year": year, "status": "uploaded",
            "s3_uri": s3_uri, "rows": rows_written,
            "seconds": round(time.perf_counter() - t0, 3),
            "run_id": run_id,
        }

    except Exception as e:
        return {
            "flow": flow_name, "year": year, "status": "unexpected_failed",
            "error": str(e),
            "seconds": round(time.perf_counter() - t0, 3),
        }
    finally:
        try:
            spooled.close()
        except Exception:
            pass
