"""Runtime settings for local development and deployed GCP services."""

from __future__ import annotations

import os

try:
    from dotenv import load_dotenv
except ImportError:  # Cloud Functions can run without python-dotenv.
    load_dotenv = None


if load_dotenv is not None:
    load_dotenv()


GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GCP_REGION = os.getenv("GCP_REGION", "us-central1")
GCS_BUCKET = os.getenv("GCS_BUCKET", "")
BQ_DATASET = os.getenv("BQ_DATASET", "sec_insider")
BQ_LOCATION = os.getenv("BQ_LOCATION", "us-central1")

FINNHUB_KEY = os.getenv("FINNHUB_KEY", "")
FRED_KEY = os.getenv("FRED_KEY", "")

SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "")
SEC_RATE_LIMIT_SECONDS = float(os.getenv("SEC_RATE_LIMIT_SECONDS", "0.2"))
SEC_TIMEOUT_SECONDS = int(os.getenv("SEC_TIMEOUT_SECONDS", "30"))

SEC_SUBMISSIONS_BASE_URL = "https://data.sec.gov/submissions"
SEC_ARCHIVES_BASE_URL = "https://www.sec.gov/Archives/edgar/data"

FINNHUB_BASE_URL = "https://finnhub.io/api/v1"
FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"


def require_setting(name: str, value: str) -> str:
    """Return a required setting or raise a clear startup/runtime error."""
    if not value:
        raise RuntimeError(f"Missing required setting: {name}")
    return value
