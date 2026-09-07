"""Master Airflow DAG for the OpenInsider end-to-end pipeline."""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import DagRunState


DEFAULT_ARGS = {
    "owner": "openinsider",
    "depends_on_past": False,
    "retries": int(os.getenv("AIRFLOW_TASK_RETRIES", "0")),
    "retry_delay": timedelta(minutes=int(os.getenv("AIRFLOW_TASK_RETRY_DELAY_MINUTES", "1"))),
    "email_on_failure": False,
}


def trigger_child_dag(task_id: str, dag_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag_id,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        reset_dag_run=True,
    )


with DAG(
    dag_id="openinsider_pipeline",
    default_args=DEFAULT_ARGS,
    description="Orchestrate SEC ingestion, macro context, price/news enrichment, scoring, and reporting",
    schedule="30 23 * * 1-5",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["openinsider", "master", "orchestration"],
) as dag:
    sec_ingest = trigger_child_dag("run_sec_ingest", "sec_ingest")
    macro_context = trigger_child_dag("run_macro_context", "macro_context")
    price_enrichment = trigger_child_dag("run_price_enrichment", "price_enrichment")
    flag_suspicious = trigger_child_dag("run_flag_suspicious", "flag_suspicious")
    weekly_report = trigger_child_dag("run_weekly_report", "weekly_report")

    sec_ingest >> macro_context >> price_enrichment >> flag_suspicious >> weekly_report
