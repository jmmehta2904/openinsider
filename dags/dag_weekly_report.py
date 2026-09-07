"""Airflow DAG: build weekly summary and export report files."""

from __future__ import annotations

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
    dag_id="weekly_report",
    default_args=DEFAULT_ARGS,
    description="Maintain weekly gold summary and export Parquet/CSV reports to GCS",
    schedule=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["reporting", "export", "gold"],
)
def weekly_report():
    @task
    def build_weekly_summary() -> str:
        from google.cloud import bigquery

        client = bigquery.Client(project=_setting("GCP_PROJECT_ID"))
        query = f"""
        MERGE `{_table("weekly_summary")}` target
        USING (
          SELECT
            DATE_TRUNC(f.filing_date, WEEK(MONDAY)) AS week_start_date,
            f.ticker,
            COUNT(DISTINCT f.accession_number) AS filing_count,
            COUNT(*) AS transaction_count,
            COUNTIF(f.transaction_code = 'P') AS buy_count,
            COUNTIF(f.transaction_code = 'S') AS sell_count,
            SUM(IF(f.transaction_code = 'P', f.total_value_usd, 0)) AS total_buy_value_usd,
            SUM(IF(f.transaction_code = 'S', f.total_value_usd, 0)) AS total_sell_value_usd,
            COUNT(DISTINCT a.alert_id) AS alert_count,
            MAX(a.suspicion_score) AS max_suspicion_score
          FROM `{_table("filings_raw")}` f
          LEFT JOIN `{_table("insider_alerts")}` a
            ON a.transaction_id = f.transaction_id
          WHERE f.filing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
          GROUP BY week_start_date, f.ticker
        ) source
        ON target.week_start_date = source.week_start_date
       AND target.ticker = source.ticker
        WHEN MATCHED THEN UPDATE SET
          filing_count = source.filing_count,
          transaction_count = source.transaction_count,
          buy_count = source.buy_count,
          sell_count = source.sell_count,
          total_buy_value_usd = source.total_buy_value_usd,
          total_sell_value_usd = source.total_sell_value_usd,
          alert_count = source.alert_count,
          max_suspicion_score = source.max_suspicion_score,
          generated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
          week_start_date, ticker, filing_count, transaction_count, buy_count,
          sell_count, total_buy_value_usd, total_sell_value_usd, alert_count,
          max_suspicion_score
        ) VALUES (
          source.week_start_date, source.ticker, source.filing_count,
          source.transaction_count, source.buy_count, source.sell_count,
          source.total_buy_value_usd, source.total_sell_value_usd,
          source.alert_count, source.max_suspicion_score
        )
        """
        job = client.query(query)
        job.result()
        week_start = datetime.utcnow().date().isoformat()
        print(f"Weekly summary complete; affected rows: {job.num_dml_affected_rows}")
        return week_start

    @task
    def export_weekly_report(_: str) -> None:
        from google.cloud import bigquery

        bucket = _setting("GCS_BUCKET")
        if not bucket:
            raise RuntimeError("GCS_BUCKET is required")

        client = bigquery.Client(project=_setting("GCP_PROJECT_ID"))
        export_prefix = datetime.utcnow().date().isoformat()
        query = f"""
        EXPORT DATA OPTIONS (
          uri = 'gs://{bucket}/reports/weekly/{export_prefix}/insider_summary_*.parquet',
          format = 'PARQUET',
          overwrite = true
        ) AS
        SELECT
          s.*,
          c.company_name,
          c.sector,
          c.industry
        FROM `{_table("weekly_summary")}` s
        LEFT JOIN `{_table("company_dim")}` c
          ON c.ticker = s.ticker
         AND c.is_active = TRUE
        WHERE s.week_start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
        ORDER BY s.week_start_date DESC, s.max_suspicion_score DESC
        """
        client.query(query).result()
        print(f"Exported weekly Parquet report to gs://{bucket}/reports/weekly/{export_prefix}/")

    week_start = build_weekly_summary()
    export_weekly_report(week_start)


weekly_report()
