import os
from dataclasses import dataclass
from dotenv import load_dotenv
from config import settings as app_config

@dataclass(frozen=True)
class PipelineConfig:
    base_url: str
    key: str
    sub_key: str
    flows: dict
    start_year: int
    end_year: int

    aws_region: str
    bucket: str
    s3_prefix: str

    # Tier 2/3 controls
    max_workers: int
    requests_per_second: float
    http_timeout_seconds: int

    # Tier 1/3
    skip_if_exists: bool
    compress_gzip: bool
    spooled_max_mb: int


def load_config() -> PipelineConfig:
    load_dotenv()

    bucket = getattr(app_config, "BUCKET", None)
    if not bucket:
        raise ValueError("BUCKET is missing. Set it in config/settings.py (or your config source).")

    return PipelineConfig(
        base_url=app_config.base_url,
        key=app_config.key,
        sub_key=app_config.sub_key,
        flows=app_config.flows,
        start_year=int(app_config.startPeriod),
        end_year=int(app_config.endPeriod),
        aws_region=os.getenv("AWS_REGION", "eu-west-1"),
        bucket=bucket,
        s3_prefix=(getattr(app_config, "AWS_S3_PREFIX", "") or "").strip("/"),
        max_workers=int(os.getenv("MAX_WORKERS", "6")),
        requests_per_second=float(os.getenv("REQUESTS_PER_SECOND", "2")),
        http_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "20")),
        skip_if_exists=os.getenv("SKIP_IF_EXISTS", "true").lower() == "true",
        compress_gzip=os.getenv("COMPRESS_GZIP", "true").lower() == "true",
        spooled_max_mb=int(os.getenv("SPOOLED_MAX_MB", "50")),
    )
