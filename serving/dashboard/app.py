from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from watchlist_snapshot import WATCHLIST_COMPANY_NAMES


PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
DATASET = os.getenv("BQ_DATASET", "sec_insider")
DEFAULT_LOOKBACK_DAYS = int(os.getenv("DASHBOARD_DEFAULT_LOOKBACK_DAYS", "90"))
LOOKBACK_OPTIONS = [7, 30, 90, 365]
NO_TICKER_MATCH = "__NO_TICKER_MATCH__"


st.set_page_config(
    page_title="SEC Insider Trading Monitor",
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


def as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    return list(value)


def ticker_label(ticker: str) -> str:
    company_name = WATCHLIST_COMPANY_NAMES.get(ticker)
    return f"{ticker} - {company_name}" if company_name else ticker


def resolve_search_tickers(search_text: str) -> list[str]:
    query = search_text.strip().lower()
    if not query:
        return []

    return sorted(
        ticker
        for ticker, company_name in WATCHLIST_COMPANY_NAMES.items()
        if query in ticker.lower() or query in company_name.lower()
    )


def merge_ticker_filters(
    search_text: str,
    search_matches: list[str],
    selected_tickers: list[str],
) -> list[str]:
    if not search_text.strip():
        return selected_tickers
    if not search_matches:
        return [NO_TICKER_MATCH]
    if selected_tickers:
        overlap = sorted(set(search_matches) & set(selected_tickers))
        return overlap or [NO_TICKER_MATCH]
    return search_matches


def common_params(
    lookback_days: int,
    tickers: list[str],
    transaction_codes: list[str],
    insider_roles: list[str],
    min_value: float,
) -> list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter]:
    return [
        bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days),
        bigquery.ScalarQueryParameter("ticker_count", "INT64", len(tickers)),
        bigquery.ArrayQueryParameter("tickers", "STRING", tickers),
        bigquery.ScalarQueryParameter("code_count", "INT64", len(transaction_codes)),
        bigquery.ArrayQueryParameter("transaction_codes", "STRING", transaction_codes),
        bigquery.ScalarQueryParameter("role_count", "INT64", len(insider_roles)),
        bigquery.ArrayQueryParameter("insider_roles", "STRING", insider_roles),
        bigquery.ScalarQueryParameter("min_value", "FLOAT64", min_value),
    ]


def filing_filter(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return f"""
      AND (@ticker_count = 0 OR {prefix}ticker IN UNNEST(@tickers))
      AND (@code_count = 0 OR {prefix}transaction_code IN UNNEST(@transaction_codes))
      AND (@role_count = 0 OR {prefix}insider_role IN UNNEST(@insider_roles))
      AND COALESCE(CAST({prefix}total_value_usd AS FLOAT64), 0) >= @min_value
    """


def load_filter_options(lookback_days: int) -> dict[str, list[str]]:
    df = query_dataframe(
        f"""
        SELECT
          ARRAY_AGG(DISTINCT ticker IGNORE NULLS ORDER BY ticker) AS tickers,
          ARRAY_AGG(DISTINCT transaction_code IGNORE NULLS ORDER BY transaction_code) AS transaction_codes,
          ARRAY_AGG(DISTINCT insider_role IGNORE NULLS ORDER BY insider_role) AS insider_roles
        FROM `{table_id("filings_raw")}`
        WHERE filing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
        """,
        [bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days)],
    )
    if df.empty:
        return {"tickers": [], "transaction_codes": [], "insider_roles": []}
    row = df.iloc[0]
    return {
        "tickers": as_list(row.get("tickers")),
        "transaction_codes": as_list(row.get("transaction_codes")),
        "insider_roles": as_list(row.get("insider_roles")),
    }


def load_kpis(
    lookback_days: int,
    tickers: list[str],
    transaction_codes: list[str],
    insider_roles: list[str],
    min_value: float,
) -> pd.DataFrame:
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
        {filing_filter()}
        """,
        common_params(lookback_days, tickers, transaction_codes, insider_roles, min_value),
    )


def load_freshness() -> pd.DataFrame:
    return query_dataframe(
        f"""
        SELECT 'filings' AS source, MAX(filing_date) AS latest_date FROM `{table_id("filings_raw")}`
        UNION ALL
        SELECT 'prices', MAX(price_date) FROM `{table_id("prices_enriched")}`
        UNION ALL
        SELECT 'macro', MAX(macro_date) FROM `{table_id("macro_context")}`
        """
    )


def load_recent_filings(
    lookback_days: int,
    tickers: list[str],
    transaction_codes: list[str],
    insider_roles: list[str],
    min_value: float,
    limit: int,
) -> pd.DataFrame:
    return normalize_dataframe(
        query_dataframe(
            f"""
            SELECT
              f.filing_date,
              f.transaction_date,
              f.ticker,
              f.insider_name,
              f.insider_role,
              f.officer_title,
              f.transaction_code,
              f.transaction_acquired_disposed,
              f.shares,
              f.share_delta,
              f.price_per_share,
              f.total_value_usd,
              f.accession_number,
              d.sec_archive_url
            FROM `{table_id("filings_raw")}` f
            LEFT JOIN `{table_id("filing_documents_raw")}` d
              USING (accession_number, ticker, cik, filing_date)
            WHERE f.filing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
            {filing_filter("f")}
            ORDER BY f.filing_date DESC, f.total_value_usd DESC
            LIMIT @limit
            """,
            [
                *common_params(lookback_days, tickers, transaction_codes, insider_roles, min_value),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ],
        )
    )


def load_activity(
    lookback_days: int,
    tickers: list[str],
    transaction_codes: list[str],
    insider_roles: list[str],
    min_value: float,
) -> pd.DataFrame:
    return query_dataframe(
        f"""
        SELECT
          filing_date,
          ticker,
          COUNT(*) AS transactions,
          SUM(CAST(total_value_usd AS FLOAT64)) AS total_value_usd
        FROM `{table_id("filings_raw")}`
        WHERE filing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
        {filing_filter()}
        GROUP BY filing_date, ticker
        ORDER BY filing_date
        """,
        common_params(lookback_days, tickers, transaction_codes, insider_roles, min_value),
    )


def load_alerts(
    lookback_days: int,
    tickers: list[str],
    transaction_codes: list[str],
    insider_roles: list[str],
    min_value: float,
    min_score: float,
) -> pd.DataFrame:
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
            {filing_filter()}
            ORDER BY suspicion_score DESC, alerted_at DESC
            LIMIT 100
            """,
            [
                *common_params(lookback_days, tickers, transaction_codes, insider_roles, min_value),
                bigquery.ScalarQueryParameter("min_score", "FLOAT64", min_score),
            ],
        )
    )


def load_data_health(lookback_days: int) -> pd.DataFrame:
    return query_dataframe(
        f"""
        SELECT 'filing_documents_raw' AS table_name, COUNT(*) AS row_count
        FROM `{table_id("filing_documents_raw")}`
        WHERE filing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
        UNION ALL
        SELECT 'filings_raw', COUNT(*)
        FROM `{table_id("filings_raw")}`
        WHERE filing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
        UNION ALL
        SELECT 'prices_enriched', COUNT(*)
        FROM `{table_id("prices_enriched")}`
        WHERE price_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
        UNION ALL
        SELECT 'company_news_raw', COUNT(*)
        FROM `{table_id("company_news_raw")}`
        WHERE news_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
        UNION ALL
        SELECT 'macro_context', COUNT(*)
        FROM `{table_id("macro_context")}`
        WHERE macro_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
        UNION ALL
        SELECT 'insider_alerts', COUNT(*)
        FROM `{table_id("insider_alerts")}`
        WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
        UNION ALL
        SELECT 'weekly_summary', COUNT(*)
        FROM `{table_id("weekly_summary")}`
        WHERE week_start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
        """,
        [bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days)],
    )


st.title("SEC Insider Trading Monitor")
st.caption("Airflow-orchestrated SEC Form 4 pipeline backed by GCS and BigQuery")

with st.sidebar:
    st.header("Filters")
    default_lookback_index = LOOKBACK_OPTIONS.index(DEFAULT_LOOKBACK_DAYS) if DEFAULT_LOOKBACK_DAYS in LOOKBACK_OPTIONS else 2
    lookback_days = st.radio(
        "Lookback",
        options=LOOKBACK_OPTIONS,
        index=default_lookback_index,
        horizontal=True,
        format_func=lambda days: f"{days}d",
    )
    row_limit = st.slider("Rows", min_value=25, max_value=250, value=100, step=25)
    min_value = st.number_input("Minimum transaction value", min_value=0, value=0, step=10_000)
    min_score = st.slider("Minimum alert score", min_value=0.0, max_value=1.0, value=0.4, step=0.05)
    search_text = st.text_input("Search company or ticker", placeholder="Apple, AAPL, Tesla")

try:
    options = load_filter_options(lookback_days)
    ticker_options = sorted(set(options["tickers"]) | set(WATCHLIST_COMPANY_NAMES))
    search_matches = resolve_search_tickers(search_text)

    with st.sidebar:
        if search_text.strip():
            if search_matches:
                preview = ", ".join(search_matches[:8])
                suffix = "..." if len(search_matches) > 8 else ""
                st.caption(f"Search matched: {preview}{suffix}")
            else:
                st.warning("No watchlist company matches this search.")

        selected_tickers = st.multiselect(
            "Tickers",
            options=ticker_options,
            format_func=ticker_label,
        )
        selected_codes = st.multiselect("Transaction codes", options=options["transaction_codes"])
        selected_roles = st.multiselect("Insider roles", options=options["insider_roles"])

    tickers = merge_ticker_filters(search_text, search_matches, selected_tickers)
    transaction_codes = selected_codes
    insider_roles = selected_roles

    kpis = load_kpis(lookback_days, tickers, transaction_codes, insider_roles, float(min_value))
    kpi_row = kpis.iloc[0] if not kpis.empty else pd.Series(dtype=object)
    freshness = load_freshness()
    freshness_map = dict(zip(freshness["source"], freshness["latest_date"], strict=False))

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Filings", f"{int(kpi_row.get('filing_count') or 0):,}")
    col2.metric("Transactions", f"{int(kpi_row.get('transaction_count') or 0):,}")
    col3.metric("Insiders", f"{int(kpi_row.get('insider_count') or 0):,}")
    col4.metric("Reported Value", money(kpi_row.get("total_value_usd")))

    fresh1, fresh2, fresh3 = st.columns(3)
    fresh1.metric("Latest Filing", str(freshness_map.get("filings") or "-"))
    fresh2.metric("Latest Price", str(freshness_map.get("prices") or "-"))
    fresh3.metric("Latest Macro", str(freshness_map.get("macro") or "-"))

    activity_df = load_activity(lookback_days, tickers, transaction_codes, insider_roles, float(min_value))
    alerts_df = load_alerts(
        lookback_days,
        tickers,
        transaction_codes,
        insider_roles,
        float(min_value),
        min_score,
    )
    filings_df = load_recent_filings(
        lookback_days,
        tickers,
        transaction_codes,
        insider_roles,
        float(min_value),
        row_limit,
    )
    health_df = load_data_health(lookback_days)

    tab_overview, tab_alerts, tab_filings, tab_health = st.tabs(
        ["Overview", "Alerts", "Filings", "Data Health"]
    )

    with tab_overview:
        if search_text.strip() and int(kpi_row.get("transaction_count") or 0) == 0:
            st.info("No insider trades found for this search and selected filter window.")

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

    with tab_health:
        health_cols = st.columns(4)
        for idx, row in health_df.iterrows():
            health_cols[idx % 4].metric(str(row["table_name"]), f"{int(row['row_count']):,}")
        st.dataframe(health_df, use_container_width=True, hide_index=True)

except (GoogleAPIError, RuntimeError) as exc:
    st.error(f"Dashboard query failed: {exc}")
    st.stop()
