"""Airflow DAG: fetch Finnhub quote/news context and load BigQuery."""

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
    dag_id="price_enrichment",
    default_args=DEFAULT_ARGS,
    description="Fetch Finnhub quotes and news for tickers with recent SEC activity",
    schedule="@hourly",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["prices", "news", "silver"],
)
def price_enrichment():
    @task
    def get_recent_filing_tickers() -> list[str]:
        from google.cloud import bigquery

        client = bigquery.Client(project=_setting("GCP_PROJECT_ID"))
        query = f"""
            SELECT DISTINCT ticker
            FROM `{_table("filings_raw")}`
            WHERE filing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
              AND ticker IS NOT NULL
            ORDER BY ticker
        """
        tickers = [row["ticker"] for row in client.query(query).result()]
        print(f"Tickers with recent filings: {tickers}")
        return tickers

    @task
    def trigger_fetch_prices(tickers: list[str]) -> dict:
        from dags.http_utils import post_cloud_function_json

        if not tickers:
            return {"status": "skipped", "reason": "No recent filing tickers"}

        url = _setting("FETCH_PRICES_URL")
        if not url:
            raise RuntimeError("FETCH_PRICES_URL is required")

        return post_cloud_function_json(
            url,
            {"tickers": tickers, "news_days_back": 7},
            timeout=180,
        )

    @task
    def load_price_and_news(fetch_result: dict) -> dict:
        from google.cloud import bigquery, storage

        from dags.bigquery_utils import insert_new_rows_by_key, replace_partition_rows

        bucket_name = _setting("GCS_BUCKET")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        bq_client = bigquery.Client(project=_setting("GCP_PROJECT_ID"))

        price_rows = []
        news_rows = []
        price_date = datetime.utcnow().date().isoformat()

        for price_file in fetch_result.get("price_files", []):
            path = price_file["gcs_path"].replace(f"gs://{bucket_name}/", "")
            payload = json.loads(bucket.blob(path).download_as_text())
            price_date = payload["price_date"]
            price_rows.append(
                {
                    "ticker": payload["ticker"],
                    "price_date": payload["price_date"],
                    "open": payload.get("open"),
                    "high": payload.get("high"),
                    "low": payload.get("low"),
                    "close": payload.get("close"),
                    "prev_close": payload.get("prev_close"),
                    "volume": payload.get("volume"),
                    "pct_change": payload.get("pct_change"),
                    "news_count_7d": payload.get("news_count_7d"),
                    "ingestion_run_id": _airflow_run_id(),
                }
            )

        for news_file in fetch_result.get("news_files", []):
            path = news_file["gcs_path"].replace(f"gs://{bucket_name}/", "")
            payload = json.loads(bucket.blob(path).download_as_text())
            for article in payload.get("articles", []):
                published_at = article.get("published_at")
                news_rows.append(
                    {
                        "news_id": article["news_id"],
                        "ticker": article["ticker"],
                        "published_at": published_at,
                        "news_date": published_at[:10] if published_at else None,
                        "source": article.get("source"),
                        "category": article.get("category"),
                        "headline": article.get("headline"),
                        "summary": article.get("summary"),
                        "url": article.get("url"),
                        "image_url": article.get("image_url"),
                        "related": article.get("related"),
                        "ingestion_run_id": _airflow_run_id(),
                    }
                )

        inserted_prices = 0
        if price_rows:
            tickers = sorted({row["ticker"] for row in price_rows})
            quoted = ", ".join([f"'{ticker}'" for ticker in tickers])
            extra_predicate = f"ticker IN ({quoted})"
            inserted_prices = replace_partition_rows(
                client=bq_client,
                table_id=_table("prices_enriched"),
                rows=price_rows,
                partition_column="price_date",
                partition_value=price_date,
                extra_delete_predicate=extra_predicate,
            )
        inserted_news = insert_new_rows_by_key(
            client=bq_client,
            table_id=_table("company_news_raw"),
            rows=news_rows,
            key_column="news_id",
        )
        print(f"Loaded {inserted_prices} price rows and {inserted_news} news rows")
        return {"prices": inserted_prices, "news": inserted_news}

    tickers = get_recent_filing_tickers()
    fetch_result = trigger_fetch_prices(tickers)
    load_price_and_news(fetch_result)


def _airflow_run_id() -> str:
    return os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "manual")


price_enrichment()
