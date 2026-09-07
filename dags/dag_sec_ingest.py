"""Airflow DAG: fetch SEC Form 4 documents, parse XML, load BigQuery."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task


DEFAULT_ARGS = {
    "owner": "openinsider",
    "depends_on_past": False,
    "retries": int(os.getenv("AIRFLOW_TASK_RETRIES", "0")),
    "retry_delay": timedelta(minutes=int(os.getenv("AIRFLOW_TASK_RETRY_DELAY_MINUTES", "1"))),
    "email_on_failure": False,
}


def _setting(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def _table(table_name: str) -> str:
    project_id = _setting("GCP_PROJECT_ID")
    dataset = _setting("BQ_DATASET", "sec_insider")
    return f"{project_id}.{dataset}.{table_name}"


def _optional_int_setting(name: str) -> int | None:
    value = _setting(name).strip()
    return int(value) if value else None


def _csv_setting(name: str) -> list[str] | None:
    value = _setting(name).strip()
    if not value:
        return None
    return [item.strip().upper() for item in value.split(",") if item.strip()]


@dag(
    dag_id="sec_ingest",
    default_args=DEFAULT_ARGS,
    description="Fetch SEC Form 4 filings, preserve raw documents, parse transaction rows",
    schedule=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sec", "ingestion", "bronze"],
)
def sec_ingest():
    @task
    def trigger_fetch_filings() -> dict:
        from dags.http_utils import post_cloud_function_json

        url = _setting("FETCH_FILINGS_URL")
        if not url:
            raise RuntimeError("FETCH_FILINGS_URL is required")

        payload = {"days_back": int(_setting("SEC_INGEST_DAYS_BACK", "2"))}
        tickers = _csv_setting("SEC_INGEST_TICKERS")
        max_filings_per_ticker = _optional_int_setting("SEC_MAX_FILINGS_PER_TICKER")
        if tickers:
            payload["tickers"] = tickers
        if max_filings_per_ticker:
            payload["max_filings_per_ticker"] = max_filings_per_ticker

        return post_cloud_function_json(url, payload, timeout=620)

    @task
    def load_filing_document_metadata(fetch_result: dict) -> list[str]:
        from google.cloud import bigquery, storage

        from dags.bigquery_utils import insert_new_rows_by_key

        bucket_name = _setting("GCS_BUCKET")
        client = bigquery.Client(project=_setting("GCP_PROJECT_ID"))

        documents = fetch_result.get("documents", [])
        rows = [
            {
                "accession_number": document["accession_number"],
                "ticker": document["ticker"],
                "cik": document["cik"],
                "form_type": document["form_type"],
                "filing_date": document["filing_date"],
                "report_date": document.get("report_date"),
                "feed_primary_document": document.get("feed_primary_document"),
                "raw_primary_document": document.get("raw_primary_document"),
                "sec_viewer_url": document.get("sec_viewer_url"),
                "sec_archive_url": document.get("sec_archive_url"),
                "raw_gcs_path": document["raw_gcs_path"],
                "metadata_gcs_path": document.get("metadata_gcs_path"),
                "content_sha256": document.get("content_sha256"),
                "ingestion_run_id": _airflow_run_id(),
            }
            for document in documents
        ]
        inserted = insert_new_rows_by_key(
            client=client,
            table_id=_table("filing_documents_raw"),
            rows=rows,
            key_column="accession_number",
        )
        print(f"Inserted {inserted} new filing document metadata rows")

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        metadata_paths = []
        for document in documents:
            metadata_gcs_path = document.get("metadata_gcs_path")
            if metadata_gcs_path:
                metadata_paths.append(metadata_gcs_path.replace(f"gs://{bucket_name}/", ""))

        if metadata_paths:
            return metadata_paths

        today_prefix = datetime.now(timezone.utc).date().isoformat()
        return [
            blob.name
            for blob in bucket.list_blobs(prefix=f"filings/{today_prefix}/")
            if blob.name.endswith("_meta.json")
        ]

    @task
    def parse_and_load_transactions(metadata_paths: list[str]) -> list[str]:
        from google.cloud import bigquery, storage

        from dags.bigquery_utils import insert_new_rows_by_key
        from transforms.parse_form4 import parse_form4_xml

        if not metadata_paths:
            print("No metadata files available to parse")
            return []

        bucket_name = _setting("GCS_BUCKET")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        rows = []
        tickers = set()

        for metadata_path in metadata_paths:
            metadata = json.loads(bucket.blob(metadata_path).download_as_text())
            raw_object_path = metadata["raw_gcs_path"].replace(f"gs://{bucket_name}/", "")
            xml_text = bucket.blob(raw_object_path).download_as_text()
            parsed_rows = parse_form4_xml(
                xml_text=xml_text,
                ticker=metadata["ticker"],
                cik=metadata["cik"],
                accession_number=metadata["accession_number"],
                filing_date=metadata["filing_date"],
                raw_gcs_path=metadata["raw_gcs_path"],
                ingestion_run_id=_airflow_run_id(),
            )
            rows.extend(parsed_rows)
            if parsed_rows:
                tickers.add(metadata["ticker"])

        client = bigquery.Client(project=_setting("GCP_PROJECT_ID"))
        inserted = insert_new_rows_by_key(
            client=client,
            table_id=_table("filings_raw"),
            rows=rows,
            key_column="transaction_id",
        )
        print(f"Parsed {len(rows)} transaction rows; inserted {inserted} new rows")
        return sorted(tickers)

    fetch_result = trigger_fetch_filings()
    metadata_paths = load_filing_document_metadata(fetch_result)
    parse_and_load_transactions(metadata_paths)


def _airflow_run_id() -> str:
    return os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "manual")


sec_ingest()
