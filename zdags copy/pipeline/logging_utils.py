import json
import logging
import datetime as dt

logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(message)s"))
logger.handlers.clear()
logger.addHandler(handler)


def log_json(level: str, **fields) -> None:
    payload = {
        "ts": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "level": level.upper(),
        **fields,
    }
    msg = json.dumps(payload, ensure_ascii=False)
    getattr(logger, level.lower(), logger.info)(msg)
