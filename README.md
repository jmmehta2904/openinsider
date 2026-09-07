# OpenInsider Pipeline

OpenInsider Pipeline is a public data engineering portfolio project that ingests SEC EDGAR Form 4 insider trading filings, enriches them with market/news/macro context, stores curated layers in BigQuery, and serves an analytical dashboard from Cloud Run.

The project demonstrates GCP-native ingestion, raw-to-gold warehouse modeling, Airflow orchestration, idempotent loads, and secure service-account based execution without committing credentials to the repository.

## Architecture

```text
SEC EDGAR        Finnhub           FRED
   |               |                |
   v               v                v
Cloud Functions: fetch_filings, fetch_prices, fetch_macro
   |
   v
Cloud Storage raw zone
   |
   v
Airflow DAGs on Compute Engine
   |
   v
BigQuery bronze/silver/gold tables
   |
   v
Streamlit dashboard on Cloud Run
```

## Implemented Components

- `config/watchlist.py`: 50-company ticker-to-CIK watchlist.
- `ingestion/fetch_filings`: SEC EDGAR Form 4 ingestion into GCS.
- `ingestion/fetch_prices`: Finnhub quote and company-news ingestion into GCS.
- `ingestion/fetch_macro`: FRED macro indicator ingestion into GCS.
- `transforms/parse_form4.py`: Form 4 XML parser preserving transaction-level detail.
- `infra/bigquery_schema.sql`: partitioned and clustered BigQuery warehouse schema.
- `dags/`: Airflow DAGs for ingestion, enrichment, scoring, and weekly reporting.
- `serving/dashboard`: Streamlit dashboard querying BigQuery directly.

## Required Environment

The real `.env` file is intentionally not committed. Required values:

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

Optional demo controls:

```text
AIRFLOW_TASK_RETRIES=0
AIRFLOW_TASK_RETRY_DELAY_MINUTES=1
DASHBOARD_DEFAULT_LOOKBACK_DAYS=90
```

## BigQuery Setup

```bash
bq query --use_legacy_sql=false --location=us-central1 < infra/bigquery_schema.sql
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

Default local login:

```text
airflow / airflow
```

## DAG Smoke Tests

```bash
docker compose exec airflow-webserver airflow dags list
docker compose exec postgres psql -U airflow -d airflow -c "SELECT filename, timestamp FROM import_error;"
docker compose exec airflow-webserver airflow dags test sec_ingest 2026-09-07
```

## Dashboard Deployment

Run from the repository root:

```bash
cd serving/dashboard

gcloud run deploy openinsider-dashboard \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 512Mi \
  --set-env-vars GCP_PROJECT_ID=insider-trade-507718,BQ_DATASET=sec_insider,DASHBOARD_DEFAULT_LOOKBACK_DAYS=90
```

The Cloud Run service account needs:

```text
roles/bigquery.jobUser
roles/bigquery.dataViewer
```

## Cost Control

Keep Airflow DAGs paused while testing manually. Stop the Compute Engine VM when not actively developing:

```bash
gcloud compute instances stop openinsider-airflow-vm --zone=us-central1-a
```

Cloud Functions, GCS, BigQuery, and Cloud Run should remain low-cost for this narrow watchlist workload, especially when used as a short-lived portfolio demo.
