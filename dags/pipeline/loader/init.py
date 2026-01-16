# dags/pipeline/loader/init.py

import time

from ..http_client import get_session
from .auth import DataExplorerParams, build_headers

RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


def init_dataflow_with_retry(de: DataExplorerParams, max_attempts: int = 3) -> None:
    """
    Initialise dataflow on server. Treat 409 as already initialised.
    Raises RuntimeError on failure.
    """
    session = get_session()
    url = f"{de.base_url}/3/init/dataflow"
    files = {
        "dataspace": (None, de.dataspace),
        "dataflow": (None, de.dataflow),
    }

    for attempt in range(1, max_attempts + 1):
        r = session.post(
            url,
            files=files,
            headers=build_headers(de.token),
            timeout=(10, 180),
            verify=de.verify_tls,
        )

        code = r.status_code

        if code in (200, 201, 202):
            return

        if code == 409:
            return  # already initialised

        if code in RETRYABLE_STATUS and attempt < max_attempts:
            time.sleep(1.5 ** attempt)
            continue

        preview = (r.text or "")[:1200]
        raise RuntimeError(f"INIT FAILED ({code}): {preview}")
