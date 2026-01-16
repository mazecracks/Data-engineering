# dags/pipeline/markers.py

import io
import json
from datetime import datetime, timezone
from typing import Optional

from .config import PipelineConfig
from .s3_client import s3_exists, upload_fileobj
from .paths import s3_base_prefix


def success_marker_key(cfg: PipelineConfig, flow: str, year: int, marker_prefix: str = "loaded") -> str:
    """
    Returns the key for: <base>/<marker_prefix>/<FLOW>/<YEAR>/_SUCCESS

    If cfg.s3_prefix="imf" and marker_prefix="loaded" => "imf/loaded/<FLOW>/<YEAR>/_SUCCESS"
    """
    flow_u = (flow or "").strip().upper()
    base = s3_base_prefix(cfg, marker_prefix)
    return f"{base.strip('/')}/{flow_u}/{int(year)}/_SUCCESS"


def is_loaded(cfg: PipelineConfig, flow: str, year: int, marker_prefix: str = "loaded") -> bool:
    """
    True if the success marker exists for (flow, year).
    """
    key_ = success_marker_key(cfg, flow, year, marker_prefix=marker_prefix)
    return s3_exists(cfg, key_)


def write_success_marker(
    cfg: PipelineConfig,
    flow: str,
    year: int,
    marker_prefix: str = "loaded",
    transformed_key: Optional[str] = None,
    chunked: Optional[bool] = None,
) -> str:
    """
    Writes a small _SUCCESS marker file to S3.

    Returns the marker key written.
    """
    key_ = success_marker_key(cfg, flow, year, marker_prefix=marker_prefix)

    payload = {
        "flow": (flow or "").strip().upper(),
        "year": int(year),
        "written_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    if transformed_key is not None:
        payload["transformed_key"] = transformed_key
    if chunked is not None:
        payload["chunked"] = bool(chunked)

    body = (json.dumps(payload, indent=2) + "\n").encode("utf-8")
    bio = io.BytesIO(body)

    upload_fileobj(
        cfg=cfg,
        key_=key_,
        fileobj=bio,
        content_type="application/json",
        content_encoding=None,
    )
    return key_
