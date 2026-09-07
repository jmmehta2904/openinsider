"""Airflow DAG: fetch FRED macro context and load BigQuery."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

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
    return f"{_setting('GCP_PROJECT_ID')}.{_setting('BQ_DATASET', 'sec_insider')}.{table_name}"


@dag(
    dag_id="macro_context",
    default_args=DEFAULT_ARGS,
    description="Fetch FRED macro context and load the BigQuery macro table",
    schedule="0 5 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["macro", "fred", "silver"],
)
def macro_context():
    @task
    def trigger_fetch_macro() -> dict:
        from dags.http_utils import post_cloud_function_json

        url = _setting("FETCH_MACRO_URL")
        if not url:
            raise RuntimeError("FETCH_MACRO_URL is required")

        payload = {"days_back": int(_setting("MACRO_DAYS_BACK", "90"))}
        return post_cloud_function_json(url, payload, timeout=180)

    @task
    def load_macro_context(fetch_result: dict) -> int:
        from google.cloud import bigquery, storage

        from dags.bigquery_utils import replace_partition_rows

        gcs_path = fetch_result.get("gcs_path")
        if not gcs_path:
            raise RuntimeError(f"fetch_macro response did not include gcs_path: {fetch_result}")

        bucket_name = _setting("GCS_BUCKET")
        object_path = gcs_path.replace(f"gs://{bucket_name}/", "")
        payload = json.loads(storage.Client().bucket(bucket_name).blob(object_path).download_as_text())

        by_date: dict[str, dict[str, object]] = {}
        for field_name, observations in payload.get("series", {}).items():
            for observation in observations:
                macro_date = observation["macro_date"]
                by_date.setdefault(
                    macro_date,
                    {
                        "macro_date": macro_date,
                        "sp500": None,
                        "vix": None,
                        "fed_funds_rate": None,
                        "treasury_10y": None,
                        "ingestion_run_id": _airflow_run_id(),
                    },
                )
                by_date[macro_date][field_name] = observation["value"]

        client = bigquery.Client(project=_setting("GCP_PROJECT_ID"))
        inserted = 0
        for macro_date, row in sorted(by_date.items()):
            inserted += replace_partition_rows(
                client=client,
                table_id=_table("macro_context"),
                rows=[row],
                partition_column="macro_date",
                partition_value=macro_date,
            )
        print(f"Loaded {inserted} macro context rows")
        return inserted

    result = trigger_fetch_macro()
    load_macro_context(result)


def _airflow_run_id() -> str:
    return os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "manual")


macro_context()
