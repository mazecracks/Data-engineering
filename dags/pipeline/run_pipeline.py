import time
import uuid
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import load_config
from .logging_utils import log_json
from .manifest import write_manifest
from .tasks import process_flow_year


def _normalise_allow_list(selected_flows):
    if not selected_flows:
        return None
    allow = {f.strip().lower() for f in selected_flows if f and f.strip()}
    return allow if allow else None


def run_pipeline(selected_flows=None) -> dict:
    """
    DAG-friendly entrypoint.

    Example:
      run_pipeline(selected_flows=["bop"])
    """
    cfg = load_config()
    allow = _normalise_allow_list(selected_flows)

    planned = []
    for flow_name, flowref in cfg.flows.items():
        if allow is not None and flow_name.strip().lower() not in allow:
            continue
        for year in range(cfg.start_year, cfg.end_year + 1):
            planned.append((flow_name, flowref, year))

    run_id = str(uuid.uuid4())
    started_at = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    t0 = time.perf_counter()

    log_json(
        "info",
        event="run_started",
        run_id=run_id,
        planned_tasks=len(planned),
        selected_flows=list(allow) if allow is not None else None,
        years={"start": cfg.start_year, "end": cfg.end_year},
        max_workers=cfg.max_workers,
        rps=cfg.requests_per_second,
        bucket=cfg.bucket,
        prefix=cfg.s3_prefix,
    )

    results = []
    with ThreadPoolExecutor(max_workers=cfg.max_workers) as ex:
        fut_map = {
            ex.submit(process_flow_year, cfg, flow_name, flowref, year, run_id): (flow_name, year)
            for (flow_name, flowref, year) in planned
        }

        for fut in as_completed(fut_map):
            flow_name, year = fut_map[fut]
            res = fut.result()
            results.append(res)
            log_json("info", event="task_result", **res)

    seconds = time.perf_counter() - t0
    ended_at = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

    manifest_uri = write_manifest(cfg, run_id, results, started_at, ended_at, seconds)

    summary = {
        "total": len(results),
        "uploaded": sum(1 for r in results if r.get("status") == "uploaded"),
        "skipped_exists": sum(1 for r in results if r.get("status") == "skipped_exists"),
        "no_data": sum(1 for r in results if r.get("status") == "no_data"),
        "failed": sum(1 for r in results if "failed" in (r.get("status") or "")),
        "seconds": round(seconds, 3),
        "manifest_uri": manifest_uri,
        "run_id": run_id,
    }

    log_json("info", event="run_finished", **summary)

    return {"summary": summary, "results": results}
