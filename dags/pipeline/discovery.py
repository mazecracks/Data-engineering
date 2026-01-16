# dags/pipeline/discovery.py

import re
from dataclasses import dataclass
from typing import Iterable, List

from .s3_client import get_s3
from .config import PipelineConfig
from .paths import s3_base_prefix


@dataclass(frozen=True)
class DiscoveredFile:
    flow: str
    year: int
    key: str  # S3 key to the transformed CSV


def _list_keys_under_prefix(cfg: PipelineConfig, prefix: str) -> List[str]:
    """
    List all object keys under a prefix (handles pagination).
    Filters out folders and zero-size objects.
    """
    s3 = get_s3(cfg)
    keys: List[str] = []

    kwargs = {"Bucket": cfg.bucket, "Prefix": prefix}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in (resp.get("Contents") or []):
            key = obj["Key"]
            size = obj.get("Size", 0)

            if size == 0 or key.endswith("/"):
                continue

            keys.append(key)

        if resp.get("IsTruncated") and resp.get("NextContinuationToken"):
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        else:
            break

    keys.sort()
    return keys


def discover_transformed_files_for_flow(
    cfg: PipelineConfig,
    flow: str,
    transformed_prefix: str = "transformed",  # logical prefix, e.g. "transformed"
    only_ext: str = ".csv",
) -> List[DiscoveredFile]:
    """
    Discover ready-to-load transformed files for a flow.

    Expected structure (logical):
      transformed/<FLOW>/<FLOW>_<YEAR>.csv

    Actual structure in S3 respects cfg.s3_prefix:
      <cfg.s3_prefix>/transformed/<FLOW>/<FLOW>_<YEAR>.csv
    """
    flow_u = (flow or "").strip().upper()

    # prefix-aware base e.g. "imf/transformed"
    base = s3_base_prefix(cfg, transformed_prefix).strip("/")

    # list under e.g. "imf/transformed/BOP/"
    base_prefix = f"{base}/{flow_u}/"

    # Example filename: BOP_1948.csv
    pattern = re.compile(rf"^{re.escape(base_prefix)}{flow_u}_(\d{{4}})\.csv$", re.IGNORECASE)

    results: List[DiscoveredFile] = []
    for key in _list_keys_under_prefix(cfg, base_prefix):
        if only_ext and not key.lower().endswith(only_ext.lower()):
            continue

        m = pattern.match(key)
        if not m:
            continue

        year = int(m.group(1))
        results.append(DiscoveredFile(flow=flow_u, year=year, key=key))

    results.sort(key=lambda x: x.year)
    return results


def discover_transformed_files(
    cfg: PipelineConfig,
    flows: Iterable[str],
    transformed_prefix: str = "transformed",  # logical prefix
) -> List[DiscoveredFile]:
    """
    Discover files across multiple flows.
    """
    all_found: List[DiscoveredFile] = []
    for f in flows:
        all_found.extend(discover_transformed_files_for_flow(cfg, f, transformed_prefix=transformed_prefix))
    return all_found
