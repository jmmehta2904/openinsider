"""Airflow DAG: score insider trades and write alert rows."""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task


DEFAULT_ARGS = {
    "owner": "openinsider",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

SCORING_VERSION = "v1"
MIN_ALERT_SCORE = 0.40
LARGE_SELL_THRESHOLD = 500_000
LARGE_BUY_THRESHOLD = 1_000_000
PRICE_DROP_THRESHOLD = -5.0
PRICE_SURGE_THRESHOLD = 10.0
HIGH_VIX_THRESHOLD = 25.0
HIGH_NEWS_COUNT_THRESHOLD = 20


def _setting(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def _table(table_name: str) -> str:
    return f"{_setting('GCP_PROJECT_ID')}.{_setting('BQ_DATASET', 'sec_insider')}.{table_name}"


@dag(
    dag_id="flag_suspicious",
    default_args=DEFAULT_ARGS,
    description="Score enriched insider trading activity and maintain alert table",
    schedule="0 23 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["scoring", "alerts", "gold"],
)
def flag_suspicious():
    @task
    def compute_and_store_alerts() -> None:
        from google.cloud import bigquery

        client = bigquery.Client(project=_setting("GCP_PROJECT_ID"))
        query = f"""
        MERGE `{_table("insider_alerts")}` target
        USING (
          WITH filings AS (
            SELECT *
            FROM `{_table("filings_raw")}`
            WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND transaction_code IN ('P', 'S')
              AND total_value_usd >= 10000
              AND is_derivative = FALSE
          ),
          trade_prices AS (
            SELECT
              f.transaction_id,
              p.close AS trade_close
            FROM filings f
            LEFT JOIN `{_table("prices_enriched")}` p
              ON p.ticker = f.ticker
             AND p.price_date <= f.transaction_date
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY f.transaction_id
              ORDER BY p.price_date DESC
            ) = 1
          ),
          future_prices AS (
            SELECT
              f.transaction_id,
              p.close AS price_7d_after
            FROM filings f
            LEFT JOIN `{_table("prices_enriched")}` p
              ON p.ticker = f.ticker
             AND p.price_date >= DATE_ADD(f.transaction_date, INTERVAL 7 DAY)
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY f.transaction_id
              ORDER BY p.price_date ASC
            ) = 1
          ),
          macro AS (
            SELECT
              f.transaction_id,
              m.vix AS vix_on_trade_date
            FROM filings f
            LEFT JOIN `{_table("macro_context")}` m
              ON m.macro_date <= f.transaction_date
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY f.transaction_id
              ORDER BY m.macro_date DESC
            ) = 1
          ),
          news AS (
            SELECT
              f.transaction_id,
              COUNT(n.news_id) AS news_count_7d,
              ARRAY_AGG(
                STRUCT(n.headline AS headline, n.url AS url, n.published_at AS published_at)
                ORDER BY n.published_at DESC
                LIMIT 1
              )[SAFE_OFFSET(0)] AS top_news
            FROM filings f
            LEFT JOIN `{_table("company_news_raw")}` n
              ON n.ticker = f.ticker
             AND n.news_date BETWEEN DATE_SUB(f.transaction_date, INTERVAL 7 DAY)
                                 AND DATE_ADD(f.transaction_date, INTERVAL 1 DAY)
            GROUP BY f.transaction_id
          ),
          scored AS (
            SELECT
              TO_HEX(MD5(CONCAT(f.transaction_id, '|', '{SCORING_VERSION}'))) AS alert_id,
              f.transaction_id,
              f.accession_number,
              f.ticker,
              f.insider_name,
              f.insider_role,
              f.transaction_date,
              f.filing_date,
              f.transaction_code,
              f.total_value_usd,
              tp.trade_close,
              fp.price_7d_after,
              SAFE_DIVIDE(fp.price_7d_after - tp.trade_close, tp.trade_close) * 100 AS price_change_7d_pct,
              m.vix_on_trade_date,
              n.news_count_7d,
              n.top_news.headline AS top_news_headline,
              n.top_news.url AS top_news_url,
              ROUND(
                CASE WHEN f.transaction_code = 'S' AND f.total_value_usd >= {LARGE_SELL_THRESHOLD} THEN 0.30 ELSE 0 END
                + CASE WHEN f.transaction_code = 'P' AND f.total_value_usd >= {LARGE_BUY_THRESHOLD} THEN 0.25 ELSE 0 END
                + CASE WHEN f.transaction_code = 'S'
                         AND SAFE_DIVIDE(fp.price_7d_after - tp.trade_close, tp.trade_close) * 100 <= {PRICE_DROP_THRESHOLD}
                       THEN 0.25 ELSE 0 END
                + CASE WHEN f.transaction_code = 'P'
                         AND SAFE_DIVIDE(fp.price_7d_after - tp.trade_close, tp.trade_close) * 100 >= {PRICE_SURGE_THRESHOLD}
                       THEN 0.15 ELSE 0 END
                + CASE WHEN f.is_officer THEN 0.15 ELSE 0 END
                + CASE WHEN m.vix_on_trade_date >= {HIGH_VIX_THRESHOLD} THEN 0.10 ELSE 0 END
                + CASE WHEN n.news_count_7d >= {HIGH_NEWS_COUNT_THRESHOLD} THEN 0.10 ELSE 0 END,
                2
              ) AS suspicion_score,
              '{SCORING_VERSION}' AS scoring_version
            FROM filings f
            LEFT JOIN trade_prices tp USING (transaction_id)
            LEFT JOIN future_prices fp USING (transaction_id)
            LEFT JOIN macro m USING (transaction_id)
            LEFT JOIN news n USING (transaction_id)
          )
          SELECT
            *,
            CONCAT(
              CASE WHEN transaction_code = 'S' AND total_value_usd >= {LARGE_SELL_THRESHOLD} THEN 'Large sale. ' ELSE '' END,
              CASE WHEN transaction_code = 'P' AND total_value_usd >= {LARGE_BUY_THRESHOLD} THEN 'Large purchase. ' ELSE '' END,
              CASE WHEN price_change_7d_pct <= {PRICE_DROP_THRESHOLD} THEN 'Price dropped after trade. ' ELSE '' END,
              CASE WHEN price_change_7d_pct >= {PRICE_SURGE_THRESHOLD} THEN 'Price surged after trade. ' ELSE '' END,
              CASE WHEN vix_on_trade_date >= {HIGH_VIX_THRESHOLD} THEN 'High market volatility. ' ELSE '' END,
              CASE WHEN news_count_7d >= {HIGH_NEWS_COUNT_THRESHOLD} THEN 'High nearby news volume. ' ELSE '' END
            ) AS flag_reason
          FROM scored
          WHERE suspicion_score >= {MIN_ALERT_SCORE}
        ) source
        ON target.alert_id = source.alert_id
        WHEN MATCHED THEN UPDATE SET
          trade_close = source.trade_close,
          price_7d_after = source.price_7d_after,
          price_change_7d_pct = source.price_change_7d_pct,
          vix_on_trade_date = source.vix_on_trade_date,
          news_count_7d = source.news_count_7d,
          top_news_headline = source.top_news_headline,
          top_news_url = source.top_news_url,
          suspicion_score = source.suspicion_score,
          flag_reason = source.flag_reason,
          alerted_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
          alert_id, transaction_id, accession_number, ticker, insider_name,
          insider_role, transaction_date, filing_date, transaction_code,
          total_value_usd, trade_close, price_7d_after, price_change_7d_pct,
          vix_on_trade_date, news_count_7d, top_news_headline, top_news_url,
          suspicion_score, scoring_version, flag_reason, ingestion_run_id
        ) VALUES (
          source.alert_id, source.transaction_id, source.accession_number, source.ticker,
          source.insider_name, source.insider_role, source.transaction_date,
          source.filing_date, source.transaction_code, source.total_value_usd,
          source.trade_close, source.price_7d_after, source.price_change_7d_pct,
          source.vix_on_trade_date, source.news_count_7d, source.top_news_headline,
          source.top_news_url, source.suspicion_score, source.scoring_version,
          source.flag_reason, @run_id
        )
        """
        job = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("run_id", "STRING", _airflow_run_id())
                ]
            ),
        )
        job.result()
        print(f"Scoring complete; affected rows: {job.num_dml_affected_rows}")

    compute_and_store_alerts()


def _airflow_run_id() -> str:
    return os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "manual")


flag_suspicious()
