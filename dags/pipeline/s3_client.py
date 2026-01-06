import threading
import boto3
from botocore.exceptions import ClientError

from .config import PipelineConfig

_thread_local = threading.local()


def get_s3(cfg: PipelineConfig):
    client = getattr(_thread_local, "s3", None)
    if client is None:
        # Uses default AWS credential chain (IAM role recommended for prod)
        client = boto3.client("s3", region_name=cfg.aws_region)
        _thread_local.s3 = client
    return client


def build_s3_key(cfg: PipelineConfig, flow_name: str, year: int) -> str:
    ext = "csv.gz" if cfg.compress_gzip else "csv"
    filename = f"{flow_name}_{year}.{ext}"
    key_ = f"{flow_name}/{filename}"
    return f"{cfg.s3_prefix}/{key_}" if cfg.s3_prefix else key_


def s3_exists(cfg: PipelineConfig, key_: str) -> bool:
    s3 = get_s3(cfg)
    try:
        s3.head_object(Bucket=cfg.bucket, Key=key_)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def upload_fileobj(cfg: PipelineConfig, key_: str, fileobj, content_type: str, content_encoding: str | None):
    s3 = get_s3(cfg)
    extra = {"ContentType": content_type}
    if content_encoding:
        extra["ContentEncoding"] = content_encoding
    s3.upload_fileobj(fileobj, cfg.bucket, key_, ExtraArgs=extra)
