"""Cloud Function: fetch SEC EDGAR Form 4 filings into Cloud Storage."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import date, timedelta
from pathlib import PurePosixPath
from typing import Any

import functions_framework
import requests
from google.cloud import storage

from config.settings import (
    GCS_BUCKET,
    SEC_ARCHIVES_BASE_URL,
    SEC_RATE_LIMIT_SECONDS,
    SEC_SUBMISSIONS_BASE_URL,
    SEC_TIMEOUT_SECONDS,
    SEC_USER_AGENT,
    require_setting,
)
from config.watchlist import WATCHLIST


FORM_TYPES = {"4", "4/A"}


def _json_response(payload: dict[str, Any], status: int = 200) -> tuple[str, int, dict[str, str]]:
    return json.dumps(payload, default=str), status, {"Content-Type": "application/json"}


def _request_json(url: str) -> dict[str, Any]:
    response = requests.get(url, headers=_sec_headers(), timeout=SEC_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def _request_text(url: str) -> str:
    response = requests.get(url, headers=_sec_headers(), timeout=SEC_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


def _sec_headers() -> dict[str, str]:
    user_agent = require_setting("SEC_USER_AGENT", SEC_USER_AGENT)
    return {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
    }


def _raw_document_name(feed_primary_document: str) -> str:
    return PurePosixPath(feed_primary_document).name


def _build_archive_url(cik: str, accession_number: str, raw_document: str) -> str:
    cik_without_leading_zeroes = str(int(cik))
    accession_without_dashes = accession_number.replace("-", "")
    return (
        f"{SEC_ARCHIVES_BASE_URL}/"
        f"{cik_without_leading_zeroes}/{accession_without_dashes}/{raw_document}"
    )


def _build_viewer_url(cik: str, accession_number: str, feed_primary_document: str) -> str | None:
    if "/" not in feed_primary_document:
        return None
    cik_without_leading_zeroes = str(int(cik))
    accession_without_dashes = accession_number.replace("-", "")
    return (
        f"{SEC_ARCHIVES_BASE_URL}/"
        f"{cik_without_leading_zeroes}/{accession_without_dashes}/{feed_primary_document}"
    )


def _upload_text_if_absent(
    bucket: storage.Bucket,
    path: str,
    content: str,
    content_type: str,
) -> bool:
    blob = bucket.blob(path)
    if blob.exists():
        return False
    blob.upload_from_string(content, content_type=content_type)
    return True


@functions_framework.http
def fetch_filings(request):
    """Fetch recent watchlist Form 4 filings from SEC EDGAR and land raw XML in GCS."""
    try:
        bucket_name = require_setting("GCS_BUCKET", GCS_BUCKET)
        body = request.get_json(silent=True) or {}
        requested_tickers = body.get("tickers")
        days_back = int(body.get("days_back", 2))
        max_filings_per_ticker = body.get("max_filings_per_ticker")
        max_filings_per_ticker = int(max_filings_per_ticker) if max_filings_per_ticker else None

        watchlist = _select_watchlist(requested_tickers)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        since_date = date.today() - timedelta(days=days_back)
        fetched_documents: list[dict[str, Any]] = []
        skipped_existing = 0
        failed: list[dict[str, str]] = []

        for ticker, cik in watchlist.items():
            ticker_fetch_count = 0
            try:
                submissions_url = f"{SEC_SUBMISSIONS_BASE_URL}/CIK{cik}.json"
                submissions = _request_json(submissions_url)
                recent = submissions.get("filings", {}).get("recent", {})
                forms = recent.get("form", [])
                filing_dates = recent.get("filingDate", [])
                accession_numbers = recent.get("accessionNumber", [])
                primary_documents = recent.get("primaryDocument", [])
                report_dates = recent.get("reportDate", [])

                for idx, form_type in enumerate(forms):
                    if form_type not in FORM_TYPES:
                        continue
                    filing_date = date.fromisoformat(filing_dates[idx])
                    if filing_date < since_date:
                        continue
                    if max_filings_per_ticker and ticker_fetch_count >= max_filings_per_ticker:
                        break

                    accession_number = accession_numbers[idx]
                    feed_primary_document = primary_documents[idx]
                    raw_primary_document = _raw_document_name(feed_primary_document)
                    accession_compact = accession_number.replace("-", "")
                    raw_object_path = (
                        f"filings/{filing_date.isoformat()}/{ticker}/"
                        f"{accession_compact}/{raw_primary_document}"
                    )
                    metadata_object_path = raw_object_path.rsplit(".", 1)[0] + "_meta.json"

                    archive_url = _build_archive_url(cik, accession_number, raw_primary_document)
                    xml_text = _request_text(archive_url)
                    xml_sha256 = hashlib.sha256(xml_text.encode("utf-8")).hexdigest()

                    uploaded = _upload_text_if_absent(
                        bucket=bucket,
                        path=raw_object_path,
                        content=xml_text,
                        content_type="application/xml",
                    )
                    if not uploaded:
                        skipped_existing += 1
                        continue

                    metadata = {
                        "accession_number": accession_number,
                        "ticker": ticker,
                        "cik": cik,
                        "company_name": submissions.get("name"),
                        "form_type": form_type,
                        "filing_date": filing_date.isoformat(),
                        "report_date": report_dates[idx] if idx < len(report_dates) else None,
                        "feed_primary_document": feed_primary_document,
                        "raw_primary_document": raw_primary_document,
                        "sec_viewer_url": _build_viewer_url(cik, accession_number, feed_primary_document),
                        "sec_archive_url": archive_url,
                        "raw_gcs_path": f"gs://{bucket_name}/{raw_object_path}",
                        "metadata_gcs_path": f"gs://{bucket_name}/{metadata_object_path}",
                        "content_sha256": xml_sha256,
                        "fetched_at": date.today().isoformat(),
                    }
                    bucket.blob(metadata_object_path).upload_from_string(
                        json.dumps(metadata, indent=2, sort_keys=True),
                        content_type="application/json",
                    )
                    fetched_documents.append(metadata)
                    ticker_fetch_count += 1
                    time.sleep(SEC_RATE_LIMIT_SECONDS)
            except Exception as exc:
                failed.append({"ticker": ticker, "cik": cik, "error": str(exc)})

        return _json_response(
            {
                "status": "ok",
                "tickers_requested": list(watchlist),
                "documents_fetched": len(fetched_documents),
                "documents_skipped_existing": skipped_existing,
                "failures": failed,
                "documents": fetched_documents,
            }
        )
    except Exception as exc:
        return _json_response({"status": "error", "error": str(exc)}, 500)


def _select_watchlist(requested_tickers: list[str] | str | None) -> dict[str, str]:
    if not requested_tickers:
        return WATCHLIST

    if isinstance(requested_tickers, str):
        requested_tickers = [ticker.strip() for ticker in requested_tickers.split(",")]

    selected: dict[str, str] = {}
    unknown: list[str] = []
    for ticker in requested_tickers:
        normalized = ticker.upper()
        if normalized in WATCHLIST:
            selected[normalized] = WATCHLIST[normalized]
        else:
            unknown.append(normalized)

    if unknown:
        raise ValueError(f"Unknown watchlist ticker(s): {', '.join(unknown)}")
    return selected
