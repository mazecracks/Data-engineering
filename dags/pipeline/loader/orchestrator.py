# dags/pipeline/loader/orchestrator.py

from config import settings as app_config

from pipeline.config import PipelineConfig
from pipeline.markers import is_loaded, write_success_marker
from pipeline.presign import presign_get_object
from pipeline.paths import s3_base_prefix

from pipeline.chunking import chunk_one_year, get_chunk_keys_from_marker, transformed_key as build_transformed_key

from pipeline.loader.auth import get_de_params_from_env
from pipeline.loader.init import init_dataflow_with_retry
from pipeline.loader.import_file import import_from_s3_with_retry


def load_one_transformed_file(cfg: PipelineConfig, flow: str, year: int, transformed_key: str | None = None) -> str:
    """
    Unit of work per (flow, year):

    - Skip if load marker exists
    - If chunking required:
        - Ensure chunks exist (chunk_one_year is idempotent)
        - Load all chunks (fail year if any chunk fails)
      Else:
        - Load the single transformed CSV
    - Write load _SUCCESS marker only after successful load
    """
    flow_u = (flow or "").strip().upper()

    # logical prefixes from settings
    transformed_prefix_logical = getattr(app_config, "S3_TRANSFORMED_PREFIX", "transformed")
    marker_prefix_logical = getattr(app_config, "S3_MARKER_PREFIX", "loaded")

    # resolved prefixes respect cfg.s3_prefix (e.g. "imf/transformed", "imf/loaded")
    transformed_prefix = s3_base_prefix(cfg, transformed_prefix_logical)
    marker_prefix = marker_prefix_logical  # markers.py will resolve via cfg internally

    presign_expires = int(getattr(app_config, "PRESIGN_EXPIRES", 600))
    chunking_flows = set(getattr(app_config, "CHUNKING_FLOWS", set()))

    # If caller didn't pass key, derive it from (flow, year) using the resolved prefix
    if not transformed_key:
        transformed_key = build_transformed_key(flow_u, year, transformed_prefix=transformed_prefix)

    # Validate expected prefix (resolved, not logical)
    expected = transformed_prefix.strip("/") + "/"
    if not transformed_key.startswith(expected):
        raise ValueError(
            f"Unexpected transformed key '{transformed_key}' (expected prefix '{expected}')"
        )

    # 1) Skip if already loaded
    if is_loaded(cfg, flow_u, year, marker_prefix=marker_prefix_logical):
        return f"SKIP: already loaded (marker exists) flow={flow_u} year={year}"

    needs_chunking = flow_u in chunking_flows

    # 2) Init once per run of this task (safe: 409 treated as OK)
    de = get_de_params_from_env()
    init_dataflow_with_retry(de, max_attempts=3)

    if not needs_chunking:
        # ---- Non-chunk path: load single file ----
        s3_url = presign_get_object(cfg, transformed_key, expires_seconds=presign_expires)
        import_from_s3_with_retry(de, s3_url)

        marker_key = write_success_marker(
            cfg,
            flow_u,
            year,
            marker_prefix=marker_prefix_logical,
            transformed_key=transformed_key,
            chunked=False,
        )
        return f"OK: loaded (single) flow={flow_u} year={year}; marker={marker_key}"

    # ---- Chunking path ----
    chunk_result = chunk_one_year(cfg, flow_u, year)
    chunk_keys = get_chunk_keys_from_marker(cfg, flow_u, year)

    # Load all chunks; if any fails -> raise -> Airflow retries the whole year
    for ck in chunk_keys:
        chunk_url = presign_get_object(cfg, ck, expires_seconds=presign_expires)
        import_from_s3_with_retry(de, chunk_url)

    marker_key = write_success_marker(
        cfg,
        flow_u,
        year,
        marker_prefix=marker_prefix_logical,
        transformed_key=transformed_key,
        chunked=True,
    )

    if chunk_result.skipped:
        return f"OK: loaded (chunks already existed) flow={flow_u} year={year}; chunks={len(chunk_keys)}; marker={marker_key}"

    return f"OK: loaded (chunked now) flow={flow_u} year={year}; chunks={len(chunk_keys)}; marker={marker_key}"
