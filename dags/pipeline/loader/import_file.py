# dags/pipeline/loader/import_file.py

import time

from ..http_client import get_session
from .auth import DataExplorerParams, build_headers

RETRY_IMPORT_ATTEMPTS = 3
RETRY_IMPORT_BACKOFF = 1.5
RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


def import_from_s3_with_retry(de: DataExplorerParams, s3_url: str) -> None:
    """
    Imports ONE file via filepath=presigned_url.
    Raises RuntimeError on permanent failure.
    """
    session = get_session()
    url = f"{de.base_url}/3/import/sdmxFile"
    files = {
        "dataspace": (None, de.dataspace),
        "dataflow": (None, de.dataflow),
        "validationType": (None, "0"),
        "sendEmail": (None, "1"),
        "filepath": (None, s3_url),
    }

    for attempt in range(1, RETRY_IMPORT_ATTEMPTS + 1):
        r = session.post(
            url,
            files=files,
            headers=build_headers(de.token),
            timeout=(10, 180),
            verify=de.verify_tls,
        )

        if r.ok:
            return

        code = r.status_code
        preview = (r.text or "")[:600].replace("\n", " ")

        if code in (401, 403):
            raise RuntimeError(f"Auth error ({code}). Refresh token. Body: {preview}")

        if code in RETRYABLE_STATUS and attempt < RETRY_IMPORT_ATTEMPTS:
            time.sleep(RETRY_IMPORT_BACKOFF ** attempt)
            continue

        raise RuntimeError(f"Import failed ({code}) after {attempt} attempt(s). Body: {preview}")
