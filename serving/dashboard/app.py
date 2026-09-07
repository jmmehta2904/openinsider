from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery


PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
DATASET = os.getenv("BQ_DATASET", "sec_insider")
DEFAULT_LOOKBACK_DAYS = int(os.getenv("DASHBOARD_DEFAULT_LOOKBACK_DAYS", "90"))


st.set_page_config(
    page_title="OpenInsider Pipeline",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource(show_spinner=False)
def bigquery_client() -> bigquery.Client:
    if not PROJECT_ID:
        raise RuntimeError("GCP_PROJECT_ID is required")
    return bigquery.Client(project=PROJECT_ID)


def table_id(table_name: str) -> str:
    return f"{PROJECT_ID}.{DATASET}.{table_name}"


def query_dataframe(
    sql: str,
    parameters: list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter] | None = None,
) -> pd.DataFrame:
    job_config = bigquery.QueryJobConfig(query_parameters=parameters or [])
    return bigquery_client().query(sql, job_config=job_config).to_dataframe()


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        if df[column].map(lambda value: isinstance(value, Decimal)).any():
            df[column] = df[column].astype(float)
    return df


def money(value: Any) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"${float(value):,.0f}"


def pct(value: Any) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.2f}%"


def load_tickers(lookback_days: int) -> list[str]:
    df = query_dataframe(
        f"""
        SELECT DISTINCT ticker
        FROM `{table_id("filings_raw")}`
        WHERE filing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
        ORDER BY ticker
        """,
        [bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days)],
    )
    return df["ticker"].dropna().tolist()


def load_kpis(lookback_days: int, tickers: list[str]) -> pd.DataFrame:
    ticker_filter = "AND (@ticker_count = 0 OR ticker IN UNNEST(@tickers))"
    return query_dataframe(
        f"""
        SELECT
          COUNT(DISTINCT accession_number) AS filing_count,
          COUNT(*) AS transaction_count,
          COUNT(DISTINCT ticker) AS ticker_count,
          COUNT(DISTINCT insider_name) AS insider_count,
          COUNTIF(transaction_code = 'P') AS buy_count,
          COUNTIF(transaction_code = 'S') AS sell_count,
          SUM(CAST(total_value_usd AS FLOAT64)) AS total_value_usd,
          MAX(filing_date) AS latest_filing_date
        FROM `{table_id("filings_raw")}`
        WHERE filing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
          {ticker_filter}
        """,
        [
            bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days),
            bigquery.ScalarQueryParameter("ticker_count", "INT64", len(tickers)),
            bigquery.ArrayQueryParameter("tickers", "STRING", tickers),
        ],
    )


def load_recent_filings(lookback_days: int, tickers: list[str], limit: int) -> pd.DataFrame:
    return normalize_dataframe(
        query_dataframe(
            f"""
            SELECT
              filing_date,
              transaction_date,
              ticker,
              insider_name,
              insider_role,
              officer_title,
              transaction_code,
              transaction_acquired_disposed,
              shares,
              share_delta,
              price_per_share,
              total_value_usd,
              accession_number,
              sec_archive_url
            FROM `{table_id("filings_raw")}` f
            LEFT JOIN `{table_id("filing_documents_raw")}` d
              USING (accession_number, ticker, cik, filing_date)
            WHERE filing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
              AND (@ticker_count = 0 OR ticker IN UNNEST(@tickers))
            ORDER BY filing_date DESC, total_value_usd DESC
            LIMIT @limit
            """,
            [
                bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days),
                bigquery.ScalarQueryParameter("ticker_count", "INT64", len(tickers)),
                bigquery.ArrayQueryParameter("tickers", "STRING", tickers),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ],
        )
    )


def load_activity(lookback_days: int, tickers: list[str]) -> pd.DataFrame:
    return query_dataframe(
        f"""
        SELECT
          filing_date,
          ticker,
          COUNT(*) AS transactions,
          SUM(CAST(total_value_usd AS FLOAT64)) AS total_value_usd
        FROM `{table_id("filings_raw")}`
        WHERE filing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
          AND (@ticker_count = 0 OR ticker IN UNNEST(@tickers))
        GROUP BY filing_date, ticker
        ORDER BY filing_date
        """,
        [
            bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days),
            bigquery.ScalarQueryParameter("ticker_count", "INT64", len(tickers)),
            bigquery.ArrayQueryParameter("tickers", "STRING", tickers),
        ],
    )


def load_alerts(lookback_days: int, tickers: list[str], min_score: float) -> pd.DataFrame:
    return normalize_dataframe(
        query_dataframe(
            f"""
            SELECT
              alerted_at,
              ticker,
              insider_name,
              insider_role,
              transaction_date,
              transaction_code,
              total_value_usd,
              trade_close,
              price_7d_after,
              price_change_7d_pct,
              vix_on_trade_date,
              news_count_7d,
              suspicion_score,
              flag_reason,
              top_news_headline,
              top_news_url
            FROM `{table_id("insider_alerts")}`
            WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
              AND suspicion_score >= @min_score
              AND (@ticker_count = 0 OR ticker IN UNNEST(@tickers))
            ORDER BY suspicion_score DESC, alerted_at DESC
            LIMIT 100
            """,
            [
                bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days),
                bigquery.ScalarQueryParameter("min_score", "FLOAT64", min_score),
                bigquery.ScalarQueryParameter("ticker_count", "INT64", len(tickers)),
                bigquery.ArrayQueryParameter("tickers", "STRING", tickers),
            ],
        )
    )


def load_context_counts(lookback_days: int) -> pd.DataFrame:
    return query_dataframe(
        f"""
        SELECT 'price rows' AS metric, COUNT(*) AS records FROM `{table_id("prices_enriched")}`
        WHERE price_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
        UNION ALL
        SELECT 'news rows' AS metric, COUNT(*) AS records FROM `{table_id("company_news_raw")}`
        WHERE news_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
        UNION ALL
        SELECT 'macro rows' AS metric, COUNT(*) AS records FROM `{table_id("macro_context")}`
        WHERE macro_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
        """,
        [bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days)],
    )


st.title("OpenInsider Pipeline")
st.caption("SEC EDGAR Form 4 ingestion, BigQuery warehouse, scoring, and reporting")

with st.sidebar:
    st.header("Filters")
    lookback_days = st.slider("Lookback days", min_value=7, max_value=365, value=DEFAULT_LOOKBACK_DAYS)
    row_limit = st.slider("Rows", min_value=25, max_value=250, value=100, step=25)
    min_score = st.slider("Minimum alert score", min_value=0.0, max_value=1.0, value=0.4, step=0.05)

try:
    all_tickers = load_tickers(lookback_days)
    with st.sidebar:
        selected_tickers = st.multiselect("Tickers", options=all_tickers, default=all_tickers[:8])

    kpis = load_kpis(lookback_days, selected_tickers)
    kpi_row = kpis.iloc[0] if not kpis.empty else pd.Series(dtype=object)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Filings", f"{int(kpi_row.get('filing_count') or 0):,}")
    col2.metric("Transactions", f"{int(kpi_row.get('transaction_count') or 0):,}")
    col3.metric("Insiders", f"{int(kpi_row.get('insider_count') or 0):,}")
    col4.metric("Reported Value", money(kpi_row.get("total_value_usd")))

    activity_df = load_activity(lookback_days, selected_tickers)
    alerts_df = load_alerts(lookback_days, selected_tickers, min_score)
    filings_df = load_recent_filings(lookback_days, selected_tickers, row_limit)
    context_df = load_context_counts(lookback_days)

    tab_overview, tab_alerts, tab_filings, tab_context = st.tabs(
        ["Overview", "Alerts", "Filings", "Context"]
    )

    with tab_overview:
        left, right = st.columns([2, 1])
        with left:
            if activity_df.empty:
                st.info("No filing activity found for the selected filters.")
            else:
                fig = px.bar(
                    activity_df,
                    x="filing_date",
                    y="transactions",
                    color="ticker",
                    title="Filing Activity",
                    labels={"filing_date": "Filing date", "transactions": "Transactions"},
                )
                fig.update_layout(legend_title_text="Ticker")
                st.plotly_chart(fig, use_container_width=True)
        with right:
            mix = pd.DataFrame(
                {
                    "side": ["Buys", "Sells"],
                    "count": [
                        int(kpi_row.get("buy_count") or 0),
                        int(kpi_row.get("sell_count") or 0),
                    ],
                }
            )
            fig = px.pie(mix, names="side", values="count", title="Buy/Sell Mix", hole=0.45)
            st.plotly_chart(fig, use_container_width=True)

        if not filings_df.empty:
            top_value = (
                filings_df.groupby("ticker", as_index=False)["total_value_usd"]
                .sum()
                .sort_values("total_value_usd", ascending=False)
                .head(10)
            )
            fig = px.bar(
                top_value,
                x="ticker",
                y="total_value_usd",
                title="Top Tickers by Reported Transaction Value",
                labels={"total_value_usd": "Reported value"},
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab_alerts:
        if alerts_df.empty:
            st.info("No scored alerts match the selected filters.")
        else:
            display_alerts = alerts_df.copy()
            display_alerts["total_value_usd"] = display_alerts["total_value_usd"].map(money)
            display_alerts["price_change_7d_pct"] = display_alerts["price_change_7d_pct"].map(pct)
            st.dataframe(display_alerts, use_container_width=True, hide_index=True)

    with tab_filings:
        if filings_df.empty:
            st.info("No parsed filing rows match the selected filters.")
        else:
            display_filings = filings_df.copy()
            display_filings["total_value_usd"] = display_filings["total_value_usd"].map(money)
            display_filings["price_per_share"] = display_filings["price_per_share"].map(money)
            st.dataframe(display_filings, use_container_width=True, hide_index=True)

    with tab_context:
        cols = st.columns(3)
        for idx, row in context_df.iterrows():
            cols[idx % 3].metric(str(row["metric"]).title(), f"{int(row['records']):,}")
        st.dataframe(context_df, use_container_width=True, hide_index=True)

except (GoogleAPIError, RuntimeError) as exc:
    st.error(f"Dashboard query failed: {exc}")
    st.stop()
