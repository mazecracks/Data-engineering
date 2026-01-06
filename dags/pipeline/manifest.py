import json
import boto3
from .config import PipelineConfig


def write_manifest(cfg: PipelineConfig, run_id: str, results: list[dict], started_at: str, ended_at: str, seconds: float) -> str:
    manifest = {
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
        "summary": {
            "total": len(results),
            "uploaded": sum(1 for r in results if r.get("status") == "uploaded"),
            "skipped_exists": sum(1 for r in results if r.get("status") == "skipped_exists"),
            "no_data": sum(1 for r in results if r.get("status") == "no_data"),
            "failed": sum(1 for r in results if "failed" in (r.get("status") or "")),
        },
        "results": results,
    }

    key_ = f"manifests/{run_id}.json"
    if cfg.s3_prefix:
        key_ = f"{cfg.s3_prefix}/{key_}"

    s3 = boto3.client("s3", region_name=cfg.aws_region)
    s3.put_object(
        Bucket=cfg.bucket,
        Key=key_,
        Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
    return f"s3://{cfg.bucket}/{key_}"
