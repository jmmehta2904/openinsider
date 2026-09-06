"""Small BigQuery helpers used by Airflow DAG tasks."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from google.cloud import bigquery


def insert_new_rows_by_key(
    client: bigquery.Client,
    table_id: str,
    rows: Sequence[dict[str, Any]],
    key_column: str,
) -> int:
    """Insert rows whose key does not already exist in the target table."""
    if not rows:
        return 0

    keys = [row[key_column] for row in rows if row.get(key_column)]
    if not keys:
        return 0

    existing = set(_existing_keys(client, table_id, key_column, keys))
    rows_to_insert = [row for row in rows if row.get(key_column) not in existing]
    if not rows_to_insert:
        return 0

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        raise RuntimeError(f"BigQuery insert failed for {table_id}: {errors}")
    return len(rows_to_insert)


def replace_partition_rows(
    client: bigquery.Client,
    table_id: str,
    rows: Sequence[dict[str, Any]],
    partition_column: str,
    partition_value: str,
    extra_delete_predicate: str | None = None,
) -> int:
    """Delete a narrow date slice and insert replacement rows.

    This pattern is intentionally simple for the small free-tier workload. It
    makes repeated Airflow task retries idempotent for daily snapshot tables.
    """
    predicate = f"{partition_column} = @partition_value"
    if extra_delete_predicate:
        predicate = f"{predicate} AND ({extra_delete_predicate})"

    delete_job = client.query(
        f"DELETE FROM `{table_id}` WHERE {predicate}",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("partition_value", "DATE", partition_value)
            ]
        ),
    )
    delete_job.result()

    if not rows:
        return 0

    errors = client.insert_rows_json(table_id, list(rows))
    if errors:
        raise RuntimeError(f"BigQuery insert failed for {table_id}: {errors}")
    return len(rows)


def run_pipeline_audit(
    client: bigquery.Client,
    table_id: str,
    run_id: str,
    pipeline_name: str,
    task_name: str,
    status: str,
    started_at: str,
    ended_at: str | None = None,
    records_read: int | None = None,
    records_written: int | None = None,
    error_message: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    row = {
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "task_name": task_name,
        "status": status,
        "started_at": started_at,
        "ended_at": ended_at,
        "records_read": records_read,
        "records_written": records_written,
        "error_message": error_message,
        "metadata": json.dumps(metadata or {}),
    }
    errors = client.insert_rows_json(table_id, [row])
    if errors:
        raise RuntimeError(f"Pipeline audit insert failed: {errors}")


def _existing_keys(
    client: bigquery.Client,
    table_id: str,
    key_column: str,
    keys: Sequence[str],
) -> list[str]:
    query = f"""
        SELECT {key_column}
        FROM `{table_id}`
        WHERE {key_column} IN UNNEST(@keys)
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("keys", "STRING", list(set(keys)))
            ]
        ),
    )
    return [row[key_column] for row in job.result()]
