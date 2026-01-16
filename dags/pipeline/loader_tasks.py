from __future__ import annotations

from typing import Dict, List

from config import settings as app_config
from pipeline.config import load_config
from pipeline.discovery import discover_transformed_files
from pipeline.markers import is_loaded
from pipeline.loader import load_one_transformed_file


def discover_pending_transformed_files(**context) -> List[Dict]:
    """
    Returns a list of dicts suitable for PythonOperator.expand(op_kwargs=...):

      [{"item": {"flow": "...", "year": 1948, "key": "transformed/BOP/BOP_1948.csv"}}, ...]
    """
    cfg = load_config()

    transformed_prefix = getattr(app_config, "S3_TRANSFORMED_PREFIX", "transformed")
    marker_prefix = getattr(app_config, "S3_MARKER_PREFIX", "loaded")

    flows = list(getattr(app_config, "flows", {}).keys())

    # discovery returns DiscoveredFile(flow, year, key)
    found = discover_transformed_files(cfg, flows=flows, transformed_prefix=transformed_prefix)

    pending: List[Dict] = []
    for f in found:
        if not is_loaded(cfg, f.flow, f.year, marker_prefix=marker_prefix):
            pending.append({"item": {"flow": f.flow, "year": f.year, "key": f.key}})

    return pending


def load_one_pending_file(item: Dict, **context) -> str:
    """
    Loads a single discovered file (flow/year). Chunking is handled internally
    by pipeline.loader.load_one_transformed_file().
    """
    cfg = load_config()
    return load_one_transformed_file(
        cfg=cfg,
        flow=item["flow"],
        year=int(item["year"]),
        transformed_key=item["key"],
    )
