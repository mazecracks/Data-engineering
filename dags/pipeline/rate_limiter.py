import time
import threading

_rate_lock = threading.Lock()
_last_request_ts = 0.0


def rate_limit(rps: float) -> None:
    """
    Global rate limiter shared by threads (simple min-interval gate).
    """
    global _last_request_ts
    if rps <= 0:
        return

    min_interval = 1.0 / rps
    with _rate_lock:
        now = time.perf_counter()
        wait = (_last_request_ts + min_interval) - now
        if wait > 0:
            time.sleep(wait)
        _last_request_ts = time.perf_counter()
