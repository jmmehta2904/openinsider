"""Cloud Function: fetch FRED macro observations into Cloud Storage."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone, timedelta
from typing import Any

import functions_framework
import requests
from google.cloud import storage

from config.settings import FRED_KEY, FRED_OBSERVATIONS_URL, GCS_BUCKET, require_setting


SERIES = {
    "sp500": "SP500",
    "vix": "VIXCLS",
    "fed_funds_rate": "FEDFUNDS",
    "treasury_10y": "DGS10",
}


def _json_response(payload: dict[str, Any], status: int = 200) -> tuple[str, int, dict[str, str]]:
    return json.dumps(payload, default=str), status, {"Content-Type": "application/json"}


@functions_framework.http
def fetch_macro(request):
    """Fetch recent macro indicators from FRED and land normalized JSON in GCS."""
    try:
        bucket_name = require_setting("GCS_BUCKET", GCS_BUCKET)
        fred_key = require_setting("FRED_KEY", FRED_KEY)
        body = request.get_json(silent=True) or {}

        as_of_date = body.get("as_of_date") or date.today().isoformat()
        days_back = int(body.get("days_back", 30))
        observation_start = body.get("observation_start") or (
            date.fromisoformat(as_of_date) - timedelta(days=days_back)
        ).isoformat()

        series_payload: dict[str, list[dict[str, Any]]] = {}
        latest_by_field: dict[str, dict[str, Any] | None] = {}
        failures: list[dict[str, str]] = []

        for field_name, series_id in SERIES.items():
            try:
                observations = _fetch_series(fred_key, series_id, observation_start)
                clean_observations = [
                    {
                        "macro_date": observation["date"],
                        "value": float(observation["value"]),
                        "series_id": series_id,
                    }
                    for observation in observations
                    if observation.get("value") not in (None, "", ".")
                ]
                series_payload[field_name] = clean_observations
                latest_by_field[field_name] = clean_observations[0] if clean_observations else None
            except Exception as exc:
                failures.append({"field": field_name, "series_id": series_id, "error": str(exc)})
                series_payload[field_name] = []
                latest_by_field[field_name] = None

        payload = {
            "as_of_date": as_of_date,
            "observation_start": observation_start,
            "series": series_payload,
            "latest": latest_by_field,
            "data_vendor": "fred",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "failures": failures,
        }

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        path = f"macro/{as_of_date}/macro.json"
        bucket.blob(path).upload_from_string(
            json.dumps(payload, indent=2, sort_keys=True),
            content_type="application/json",
        )

        return _json_response(
            {
                "status": "ok",
                "gcs_path": f"gs://{bucket_name}/{path}",
                "series": list(SERIES.values()),
                "failures": failures,
            }
        )
    except Exception as exc:
        return _json_response({"status": "error", "error": str(exc)}, 500)


def _fetch_series(api_key: str, series_id: str, observation_start: str) -> list[dict[str, Any]]:
    response = requests.get(
        FRED_OBSERVATIONS_URL,
        params={
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 90,
            "observation_start": observation_start,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("observations", [])
