"""HTTP helpers for Airflow task runtime."""

from __future__ import annotations

import os
from typing import Any


def post_cloud_function_json(url: str, payload: dict[str, Any], timeout: int) -> dict[str, Any]:
    """Call a private HTTP Cloud Function with an OIDC identity token."""
    import google.auth.transport.requests
    import requests
    from google.oauth2 import id_token

    headers = {"Content-Type": "application/json"}
    try:
        auth_request = google.auth.transport.requests.Request()
        token = id_token.fetch_id_token(auth_request, url)
        headers["Authorization"] = f"Bearer {token}"
    except Exception as exc:
        allow_unauthenticated = os.getenv("ALLOW_UNAUTHENTICATED_FUNCTIONS", "false").lower()
        if allow_unauthenticated not in {"1", "true", "yes"}:
            raise RuntimeError(
                "Could not create an identity token for the Cloud Function call. "
                "On GCP, run Airflow on the VM service account that has roles/run.invoker. "
                "For local unauthenticated testing only, set ALLOW_UNAUTHENTICATED_FUNCTIONS=true."
            ) from exc

    response = requests.post(url, json=payload, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()
