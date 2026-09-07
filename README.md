# OpenInsider Pipeline

A GCP and Airflow data engineering project that tracks SEC Form 4 insider trading filings, enriches them with market and macro context, and serves a BigQuery-backed dashboard.

## Problem

SEC insider trading filings are public, but they are hard to use directly. Form 4 documents arrive as XML filings in EDGAR, market context lives in separate APIs, and useful analysis requires joining filings, prices, news, and macro indicators into a queryable warehouse.

## Solution

OpenInsider Pipeline turns that raw public data into an analytical workflow:

```text
SEC EDGAR + Finnhub + FRED
        |
        v
Cloud Functions
        |
        v
Cloud Storage raw zone
        |
        v
Airflow orchestration
        |
        v
BigQuery warehouse
        |
        v
Streamlit dashboard on Cloud Run
```

The pipeline keeps raw source files in GCS, parses Form 4 XML into transaction-level BigQuery rows, enriches trades with price/news/macro context, scores notable activity, and exposes the results through a dashboard.
The dashboard supports ticker/company search plus filters for transaction code, insider role, transaction value, and alert score.

## Pipeline Flow

The scheduled master DAG is `openinsider_pipeline`:

```text
sec_ingest
  -> macro_context
  -> price_enrichment
  -> flag_suspicious
  -> weekly_report
```

Child DAGs are manual-only and reusable for debugging or learning individual Airflow stages.

## Tech Stack

- Python 3.12
- Apache Airflow 2.9
- Google Cloud Functions
- Google Cloud Storage
- BigQuery
- Cloud Run
- Streamlit
- Docker
- PostgreSQL for Airflow metadata

## Data Sources

- SEC EDGAR Form 4 filings and official company ticker metadata
- Finnhub quotes and company news
- FRED macro indicators: S&P 500, VIX, Fed Funds Rate, 10-year Treasury yield

## Security

Secrets and API keys are not committed. Runtime credentials are supplied through environment variables, Secret Manager, or GCP service accounts. Cloud Functions are private and invoked by authorized service accounts.

## Cost Control

The project is intentionally scoped to a 150-company watchlist verified against SEC ticker metadata. Airflow can run on a temporary Compute Engine VM, while Cloud Functions, GCS, BigQuery, and Cloud Run remain low-cost for demo-scale usage. Stop the VM when not testing:

```bash
gcloud compute instances stop openinsider-airflow-vm --zone=us-central1-a
```

## Run Locally

```bash
docker compose build
docker compose up -d
docker compose ps
```

Airflow UI:

```text
http://localhost:8080
```

## Validate Airflow

```bash
docker compose exec airflow-webserver airflow dags list
docker compose exec postgres psql -U airflow -d airflow -c "SELECT filename, timestamp FROM import_error;"
```

Run the full orchestrated pipeline manually:

```bash
docker compose exec airflow-webserver airflow dags test openinsider_pipeline 2026-09-07
```

## Deploy Dashboard

```bash
cd serving/dashboard

gcloud run deploy openinsider-dashboard \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 512Mi \
  --set-env-vars GCP_PROJECT_ID=insider-trade-507718,BQ_DATASET=sec_insider,DASHBOARD_DEFAULT_LOOKBACK_DAYS=90
```

The dashboard service account needs BigQuery read/query access:

```text
roles/bigquery.jobUser
roles/bigquery.dataViewer
```
