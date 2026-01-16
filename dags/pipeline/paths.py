from config import settings as app_config
from .config import PipelineConfig


def s3_base_prefix(cfg: PipelineConfig, logical_prefix: str) -> str:
    """
    Turns a logical prefix like 'transformed' into the actual S3 prefix,
    respecting cfg.s3_prefix (e.g. 'imf').

    Examples:
      cfg.s3_prefix='imf', logical='transformed' -> 'imf/transformed'
      cfg.s3_prefix='',    logical='transformed' -> 'transformed'
    """
    lp = (logical_prefix or "").strip("/")

    if cfg.s3_prefix:
        return f"{cfg.s3_prefix.strip('/')}/{lp}".strip("/")
    return lp
