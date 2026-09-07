"""Small BigQuery helpers used by Airflow DAG tasks."""

from __future__ import annotations

import json
from collections.abc import Sequence
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

NUMERIC_COLUMNS = {
    "price_per_share",
    "total_value_usd",
    "open",
    "high",
    "low",
    "close",
    "prev_close",
    "sp500",
    "vix",
    "fed_funds_rate",
    "treasury_10y",
    "trade_close",
    "price_7d_after",
}

NUMERIC_SCALE = Decimal("0.000000001")


def insert_new_rows_by_key(
    client: Any,
    table_id: str,
    rows: Sequence[dict[str, Any]],
    key_column: str,
    key_type: str = "STRING",
) -> int:
    """Insert rows whose key does not already exist in the target table."""
    if not rows:
        return 0

    keys = [str(row[key_column]) for row in rows if row.get(key_column)]
    if not keys:
        return 0

    existing = set(_existing_keys(client, table_id, key_column, keys, key_type))
    rows_to_insert = [row for row in rows if row.get(key_column) not in existing]
    if not rows_to_insert:
        return 0

    errors = client.insert_rows_json(table_id, _normalize_rows_for_bigquery(rows_to_insert))
    if errors:
        raise RuntimeError(f"BigQuery insert failed for {table_id}: {errors}")
    return len(rows_to_insert)


def replace_partition_rows(
    client: Any,
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
    from google.cloud import bigquery

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

    errors = client.insert_rows_json(table_id, _normalize_rows_for_bigquery(rows))
    if errors:
        raise RuntimeError(f"BigQuery insert failed for {table_id}: {errors}")
    return len(rows)


def run_pipeline_audit(
    client: Any,
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
    errors = client.insert_rows_json(table_id, _normalize_rows_for_bigquery([row]))
    if errors:
        raise RuntimeError(f"Pipeline audit insert failed: {errors}")


def _existing_keys(
    client: Any,
    table_id: str,
    key_column: str,
    keys: Sequence[str],
    key_type: str = "STRING",
) -> list[str]:
    from google.cloud import bigquery

    query = f"""
        SELECT CAST({key_column} AS STRING) AS existing_key
        FROM `{table_id}`
        WHERE CAST({key_column} AS STRING) IN UNNEST(@keys)
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("keys", "STRING", list(set(keys)))
            ]
        ),
    )
    return [row["existing_key"] for row in job.result()]


def _normalize_rows_for_bigquery(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_normalize_row_for_bigquery(row) for row in rows]


def _normalize_row_for_bigquery(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    for column in NUMERIC_COLUMNS:
        if column in normalized:
            normalized[column] = _normalize_numeric_value(normalized[column])
    return normalized


def _normalize_numeric_value(value: Any) -> str | None:
    if value is None:
        return None
    try:
        numeric = Decimal(str(value)).quantize(NUMERIC_SCALE, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid NUMERIC value for BigQuery: {value!r}") from exc
    return format(numeric.normalize(), "f")
