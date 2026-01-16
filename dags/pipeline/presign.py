# dags/pipeline/presign.py

from .config import PipelineConfig
from .s3_client import get_s3


def presign_get_object(cfg: PipelineConfig, key_: str, expires_seconds: int) -> str:
    s3 = get_s3(cfg)
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": cfg.bucket, "Key": key_},
        ExpiresIn=int(expires_seconds),
    )
