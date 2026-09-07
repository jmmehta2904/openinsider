# OpenInsider

OpenInsider is an end-to-end SEC insider trading intelligence pipeline built on Google Cloud. It turns raw SEC EDGAR Form 4 filings into structured, enriched, and searchable analytics backed by BigQuery and orchestrated with Apache Airflow.

The system is designed around real data engineering concerns: source ingestion, raw data retention, warehouse modeling, idempotent loads, orchestration, cloud IAM, cost control, and analytical serving.

## Why This Exists

SEC insider trading disclosures are public, but they are not immediately useful for analysis. Form 4 filings arrive through EDGAR as filing metadata and XML documents. Market prices, company news, and macro indicators live in separate systems. To answer useful questions, those sources need to be collected, normalized, joined, and served from a queryable warehouse.

OpenInsider builds that workflow as a cloud-native data product.

## What It Does

- Tracks a curated 150-company universe verified against SEC ticker metadata.
- Ingests SEC EDGAR Form 4 filings and filing documents.
- Stores raw source responses in Google Cloud Storage.
- Parses Form 4 XML into transaction-level BigQuery rows.
- Pulls market price and company news context from Finnhub.
- Pulls macro context from FRED.
- Enriches transactions with price/news/macro signals.
- Scores notable insider activity into an alerts table.
- Produces weekly summary outputs.
- Serves a Streamlit dashboard from Cloud Run with company search and analytical filters.

## Architecture

```text
SEC EDGAR        Finnhub        FRED
   |               |             |
   v               v             v
Cloud Functions: fetch_filings, fetch_prices, fetch_macro
   |
   v
Google Cloud Storage raw zone
   |
   v
Apache Airflow orchestration
   |
   v
BigQuery warehouse
   |
   v
Streamlit dashboard on Cloud Run
```

Cloud Functions isolate external API access. GCS keeps the raw landing zone auditable. Airflow coordinates ingestion, parsing, enrichment, scoring, and reporting. BigQuery stores the analytical model. Cloud Run serves the dashboard without requiring the Airflow VM to stay online.

## Orchestration

The scheduled entry point is the master DAG:

```text
openinsider_pipeline
```

Execution flow:

```text
sec_ingest
  -> macro_context
  -> price_enrichment
  -> flag_suspicious
  -> weekly_report
```

The master DAG uses `TriggerDagRunOperator` with `wait_for_completion=True`, so each stage must complete successfully before the next stage starts. Child DAGs are manual-only and can be triggered independently for debugging, backfills, and stage-level validation.

Default schedule:

```text
30 23 * * 1-5 UTC
```

That schedule runs once per weekday after the US market day has enough data available, while keeping cloud usage controlled.

## Warehouse Model

BigQuery is organized around raw, enriched, and analytical outputs:

- `filing_documents_raw`: filing document metadata and SEC archive links.
- `filings_raw`: parsed Form 4 transaction rows.
- `prices_enriched`: price data used for post-trade context.
- `company_news_raw`: company-level news context.
- `macro_context`: market-wide macro indicators.
- `insider_alerts`: scored insider activity.
- `weekly_summary`: reporting output for weekly review.
- `pipeline_runs`: operational run tracking.
- `company_dim`: company/ticker reference data.

Tables are partitioned by date fields and clustered by high-use analytical columns such as ticker, CIK, transaction code, insider role, and accession number where appropriate.

## Dashboard

The dashboard is a BigQuery-backed Streamlit application deployed on Cloud Run.

It supports:

- Company or ticker search.
- Lookback presets: 7, 30, 90, and 365 days.
- Ticker filtering.
- Transaction code filtering.
- Insider role filtering.
- Minimum transaction value filtering.
- Minimum alert score filtering.
- Filing, alert, overview, and data health views.
- Freshness metrics for filings, prices, and macro data.

The dashboard reads from BigQuery directly. It does not depend on Airflow being online.

## Engineering Decisions

- Raw source responses are retained in GCS before transformation.
- Cloud Functions handle external API calls instead of embedding source calls directly inside Airflow tasks.
- Airflow owns orchestration, retries, and stage ordering.
- BigQuery is the analytical source of truth.
- Loads are designed to avoid duplicate rows where practical.
- The dashboard is separated from orchestration and served independently on Cloud Run.
- The SEC ticker reference is used to verify the committed watchlist, not as a runtime dependency for every DAG run.

## Security

Secrets are not committed to the repository.

Runtime configuration is supplied through environment variables, Secret Manager, and GCP service accounts. Cloud Functions are intended to run privately and be invoked only by authorized service accounts. The dashboard service account only needs BigQuery read/query permissions.

Minimum dashboard permissions:

```text
roles/bigquery.jobUser
roles/bigquery.dataViewer
```

## Cost Control

The system is scoped for controlled demo-scale operation:

- 150-company curated watchlist.
- Conservative weekday schedule.
- Configurable lookback windows.
- Configurable max filings per ticker.
- Airflow can run on a temporary VM and be stopped when not actively testing.
- Cloud Run, Cloud Functions, GCS, and BigQuery remain lightweight for this workload size.

Stop the Airflow VM when not in use:

```bash
gcloud compute instances stop openinsider-airflow-vm --zone=$ZONE
```

## Required Configuration

The runtime environment needs:

```text
GCP_PROJECT_ID
REGION
GCS_BUCKET
BQ_DATASET
FETCH_FILINGS_URL
FETCH_PRICES_URL
FETCH_MACRO_URL
SEC_USER_AGENT
FINNHUB_KEY
FRED_KEY
```

Optional controls:

```text
SEC_INGEST_TICKERS
SEC_INGEST_DAYS_BACK
SEC_MAX_FILINGS_PER_TICKER
PRICE_ENRICHMENT_TICKERS
PRICE_ENRICHMENT_LOOKBACK_DAYS
FINNHUB_NEWS_DAYS_BACK
DASHBOARD_DEFAULT_LOOKBACK_DAYS
```

## Local Airflow

```bash
docker compose build
docker compose up -d
docker compose ps
```

Airflow UI:

```text
http://localhost:8080
```

Validate DAG imports:

```bash
docker compose exec airflow-webserver airflow dags list
docker compose exec postgres psql -U airflow -d airflow -c "SELECT filename, timestamp FROM import_error;"
```

Trigger the full orchestrated workflow:

```bash
docker compose exec airflow-webserver airflow dags trigger openinsider_pipeline
```

## Dashboard Deployment

Deploy from the dashboard directory:

```bash
cd serving/dashboard

gcloud run deploy openinsider-dashboard \
  --source . \
  --region $REGION \
  --allow-unauthenticated \
  --memory 512Mi \
  --set-env-vars GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET=$BQ_DATASET,DASHBOARD_DEFAULT_LOOKBACK_DAYS=90
```

## Validation

Check pipeline runs:

```bash
docker compose exec postgres psql -U airflow -d airflow -c "SELECT dag_id, run_id, state, start_date, end_date FROM dag_run WHERE dag_id IN ('openinsider_pipeline','sec_ingest','macro_context','price_enrichment','flag_suspicious','weekly_report') ORDER BY execution_date DESC LIMIT 20;"
```

Check warehouse row counts:

```bash
bq query --use_legacy_sql=false "
SELECT 'filings_raw' AS table_name, COUNT(*) AS rows FROM \`$PROJECT_ID.$BQ_DATASET.filings_raw\`
UNION ALL
SELECT 'filing_documents_raw', COUNT(*) FROM \`$PROJECT_ID.$BQ_DATASET.filing_documents_raw\`
UNION ALL
SELECT 'macro_context', COUNT(*) FROM \`$PROJECT_ID.$BQ_DATASET.macro_context\`
UNION ALL
SELECT 'prices_enriched', COUNT(*) FROM \`$PROJECT_ID.$BQ_DATASET.prices_enriched\`
UNION ALL
SELECT 'company_news_raw', COUNT(*) FROM \`$PROJECT_ID.$BQ_DATASET.company_news_raw\`
UNION ALL
SELECT 'insider_alerts', COUNT(*) FROM \`$PROJECT_ID.$BQ_DATASET.insider_alerts\`
UNION ALL
SELECT 'weekly_summary', COUNT(*) FROM \`$PROJECT_ID.$BQ_DATASET.weekly_summary\`;
"
```

## Repository Boundary

This repository contains the implementation, DAGs, schemas, and dashboard code. It does not include credentials, API keys, private GCP resources, or billing configuration.

## Disclaimer

This is an analytical engineering system built on public filings and external market data APIs. It is not financial advice.

