# pipeline/manifest.py
import json
import boto3
from collections import defaultdict
from .config import PipelineConfig


def _summary(results: list[dict]) -> dict:
    return {
        "total": len(results),
        "uploaded": sum(1 for r in results if r.get("status") == "uploaded"),
        "skipped_exists": sum(1 for r in results if r.get("status") == "skipped_exists"),
        "no_data": sum(1 for r in results if r.get("status") == "no_data"),
        "failed": sum(1 for r in results if "failed" in (r.get("status") or "")),
    }


def write_manifests(
    cfg: PipelineConfig,
    run_id: str,
    results: list[dict],
    started_at: str,
    ended_at: str,
    seconds: float,
) -> dict:
    
    s3 = boto3.client("s3", region_name=cfg.aws_region)

    base_manifest = {
        "run_id": run_id,
        "started_at": started_at,
        "ended_at": ended_at,
        "seconds": round(seconds, 3),
        "bucket": cfg.bucket,
        "prefix": cfg.s3_prefix,
        "config": {
            "max_workers": cfg.max_workers,
            "requests_per_second": cfg.requests_per_second,
            "http_timeout_seconds": cfg.http_timeout_seconds,
            "skip_if_exists": cfg.skip_if_exists,
            "compress_gzip": cfg.compress_gzip,
        },
    }

    # -----------------------------
    # 1) Central manifest
    # -----------------------------
    central_manifest = {
        **base_manifest,
        "summary": _summary(results),
        "results": results,
    }

    central_key = f"manifests/{run_id}.json"
    if cfg.s3_prefix:
        central_key = f"{cfg.s3_prefix}/{central_key}"

    s3.put_object(
        Bucket=cfg.bucket,
        Key=central_key,
        Body=json.dumps(central_manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )

    central_uri = f"s3://{cfg.bucket}/{central_key}"

    # -----------------------------
    # 2) Per-flow manifests
    # -----------------------------
    by_flow: dict[str, list[dict]] = defaultdict(list)
    for r in results:
        flow = (r.get("flow") or "").strip().upper() or "UNKNOWN"
        by_flow[flow].append(r)

    per_flow_uris: dict[str, str] = {}

    for flow, flow_results in by_flow.items():
        folder = getattr(cfg, "flow_s3_folders", {}).get(flow)
        if not folder:
            raise ValueError(f"No S3 folder configured for flow '{flow}' (cfg.flow_s3_folders).")

        flow_manifest = {
            **base_manifest,
            "flow": flow,
            "flow_folder": folder,
            "summary": _summary(flow_results),
            "results": flow_results,
        }

        flow_key = f"{folder}/manifests/{run_id}.json"
        if cfg.s3_prefix:
            flow_key = f"{cfg.s3_prefix}/{flow_key}"

        s3.put_object(
            Bucket=cfg.bucket,
            Key=flow_key,
            Body=json.dumps(flow_manifest, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json; charset=utf-8",
        )

        per_flow_uris[flow] = f"s3://{cfg.bucket}/{flow_key}"

    return {"central": central_uri, "per_flow": per_flow_uris}
