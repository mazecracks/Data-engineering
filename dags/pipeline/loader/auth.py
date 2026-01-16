# dags/pipeline/loader/auth.py

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DataExplorerParams:
    base_url: str
    dataspace: str
    dataflow: str
    token: str
    verify_tls: bool


def get_de_params_from_env() -> DataExplorerParams:
    token = os.getenv("DATAEXPLORER_TOKEN")
    base_url = os.getenv("DATAEXPLORER_BASE_URL")
    dataspace = os.getenv("DATAEXPLORER_DATASPACE")
    dataflow = os.getenv("DATAEXPLORER_DATAFLOW")

    if not token:
        raise ValueError("Missing env var: DATAEXPLORER_TOKEN")
    if not base_url:
        raise ValueError("Missing env var: DATAEXPLORER_BASE_URL")
    if not dataspace:
        raise ValueError("Missing env var: DATAEXPLORER_DATASPACE")
    if not dataflow:
        raise ValueError("Missing env var: DATAEXPLORER_DATAFLOW")

    verify_tls = os.getenv("VERIFY_TLS", "true").lower() == "true"

    return DataExplorerParams(
        base_url=base_url.rstrip("/"),
        dataspace=dataspace,
        dataflow=dataflow,
        token=token,
        verify_tls=verify_tls,
    )


def build_headers(token: str) -> dict:
    # IMPORTANT: do not mutate shared session headers; pass per-request headers
    return {
        "Accept": "application/json",
        "Accept-Encoding": "identity",
        "Authorization": f"Bearer {token}",
    }
