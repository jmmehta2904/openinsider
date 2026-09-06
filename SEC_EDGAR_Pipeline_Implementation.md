# SEC EDGAR Insider Trading Intelligence Pipeline
### Complete Implementation Guide — GCP + Airflow + Python

---

## What This Project Is

A production-grade data engineering pipeline that ingests corporate insider trades from the SEC in near real-time, cross-references live stock prices and macro context, scores suspicious activity, and exposes a live public dashboard and REST API.

**What it demonstrates to a recruiter or engineering interviewer:**
- Multi-source data ingestion from real public APIs (no fake datasets)
- Airflow DAG orchestration with dependencies, retries, and scheduling
- GCP cloud-native stack: Cloud Functions, Cloud Storage, BigQuery, Cloud Run
- Data modelling: raw → enriched → aggregated layers
- A live URL they can click during your interview

**Stack:** Python 3.12 · Apache Airflow 2.9 · GCP (Cloud Functions, Cloud Storage, BigQuery, Cloud Run, Cloud Scheduler, Compute Engine e2-micro) · FastAPI · Streamlit · PostgreSQL (Airflow metadata only) · Docker

---

## Project Scope — Deliberately Narrow

You are NOT ingesting all of EDGAR. You are tracking a **watchlist of 50 high-profile companies** (S&P 100 subset). This keeps BigQuery under 10 GB free forever, keeps ingestion simple, and makes the dashboard fast and readable.

**Watchlist rationale:** Pick companies where insider trades are newsworthy — AAPL, TSLA, NVDA, MSFT, META, AMZN, GOOGL, JPM, GS, NFLX, and ~40 others. The smaller the watchlist, the cleaner the signal and the easier to explain.

---

## Repository Structure

```
sec-insider-pipeline/
│
├── ingestion/                    # Cloud Functions (deployed to GCP)
│   ├── fetch_filings/
│   │   ├── main.py               # Cloud Function: SEC → GCS
│   │   └── requirements.txt
│   ├── fetch_prices/
│   │   ├── main.py               # Cloud Function: Finnhub → GCS
│   │   └── requirements.txt
│   └── fetch_macro/
│       ├── main.py               # Cloud Function: FRED → GCS
│       └── requirements.txt
│
├── dags/                         # Airflow DAGs (on e2-micro VM)
│   ├── dag_sec_ingest.py         # DAG 1: Trigger filing fetch + load to BQ
│   ├── dag_price_enrichment.py   # DAG 2: Enrich filings with price data
│   ├── dag_flag_suspicious.py    # DAG 3: Score and flag trades
│   └── dag_weekly_report.py      # DAG 4: Weekly Parquet export
│
├── transforms/                   # Python transformation logic
│   ├── parse_form4.py            # XML parser for Form 4 filings
│   ├── enrich.py                 # Join filings + prices
│   └── score.py                  # Suspicious trade scoring logic
│
├── serving/
│   ├── api/                      # FastAPI app → Cloud Run
│   │   ├── main.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── dashboard/                # Streamlit app → Cloud Run
│       ├── app.py
│       ├── Dockerfile
│       └── requirements.txt
│
├── infra/
│   ├── setup_gcp.sh              # One-shot GCP provisioning script
│   └── bigquery_schema.sql       # Table definitions
│
├── config/
│   ├── watchlist.py              # 50 tickers + CIKs
│   └── settings.py               # Env vars, constants
│
├── docker-compose.yml            # Local Airflow dev environment
├── .env.example
└── README.md
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA SOURCES (Free APIs)                    │
│   SEC EDGAR (no key)    Finnhub (free key)    FRED (free key)   │
└───────────┬─────────────────────┬───────────────────┬───────────┘
            │                     │                   │
            ▼                     ▼                   ▼
┌─────────────────────────────────────────────────────────────────┐
│              INGESTION LAYER (GCP Cloud Functions)              │
│  fetch_filings()          fetch_prices()       fetch_macro()    │
│  HTTP-triggered           HTTP-triggered       HTTP-triggered   │
│  Python 3.12              Python 3.12          Python 3.12      │
└──────────────────────────────┬──────────────────────────────────┘
                               │  Raw JSON → GCS
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                   RAW LAYER (Cloud Storage)                     │
│  gs://sec-pipeline-raw/filings/YYYY-MM-DD/                      │
│  gs://sec-pipeline-raw/prices/YYYY-MM-DD/                       │
│  gs://sec-pipeline-raw/macro/YYYY-MM-DD/                        │
└──────────────────────────────┬──────────────────────────────────┘
                               │  Load + Transform
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│               ORCHESTRATION (Airflow on e2-micro VM)            │
│                                                                  │
│  DAG 1: sec_ingest (every 15 min)                               │
│    trigger Cloud Function → wait → parse XML → BQ load          │
│                                                                  │
│  DAG 2: price_enrichment (hourly, depends on DAG 1)             │
│    fetch prices for tickers in today's filings → BQ upsert      │
│                                                                  │
│  DAG 3: flag_suspicious (daily 11 PM)                           │
│    score trades → write alerts table → log                       │
│                                                                  │
│  DAG 4: weekly_report (Monday 6 AM)                             │
│    join all tables → export Parquet to GCS                       │
└──────────────────────────────┬──────────────────────────────────┘
                               │  Structured data
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│               WAREHOUSE LAYER (BigQuery)                        │
│  filings_raw          prices_enriched        macro_context      │
│  insider_alerts        company_dim            weekly_summary     │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                ┌──────────────┴──────────────┐
                ▼                             ▼
┌──────────────────────┐         ┌────────────────────────┐
│  FastAPI (Cloud Run) │         │  Streamlit (Cloud Run) │
│  Public REST API     │◄────────│  Live dashboard        │
│  /alerts /scores     │         │  yourdomain.run.app    │
└──────────────────────┘         └────────────────────────┘
```

---

## Phase 0 — Setup (Day 1, Morning, ~2 hours)

### 0.1 Local dev environment

```bash
# Clone your repo
git clone https://github.com/yourusername/sec-insider-pipeline
cd sec-insider-pipeline

# Python environment
python3.12 -m venv .venv
source .venv/bin/activate
pip install requests pandas python-dotenv google-cloud-bigquery google-cloud-storage apache-airflow[gcp]==2.9.0

# Copy env template
cp .env.example .env
# Fill in: FINNHUB_KEY, FRED_KEY, GCP_PROJECT_ID
```

### 0.2 Local Airflow via Docker Compose

```yaml
# docker-compose.yml
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres_data:/var/lib/postgresql/data

  airflow-init:
    image: apache/airflow:2.9.0
    depends_on: [postgres]
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    command: db init

  airflow-webserver:
    image: apache/airflow:2.9.0
    depends_on: [airflow-init]
    ports:
      - "8080:8080"
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    volumes:
      - ./dags:/opt/airflow/dags
    command: webserver

  airflow-scheduler:
    image: apache/airflow:2.9.0
    depends_on: [airflow-init]
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    volumes:
      - ./dags:/opt/airflow/dags
    command: scheduler

volumes:
  postgres_data:
```

```bash
docker-compose up -d
# Airflow UI at http://localhost:8080
# Default login: airflow / airflow
```

### 0.3 GCP project setup

```bash
# Install gcloud CLI if not already
# https://cloud.google.com/sdk/docs/install

gcloud auth login
gcloud projects create sec-insider-pipeline-001
gcloud config set project sec-insider-pipeline-001

# Enable all required APIs in one shot
gcloud services enable \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  cloudscheduler.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  run.googleapis.com \
  compute.googleapis.com \
  artifactregistry.googleapis.com \
  logging.googleapis.com

# Create GCS bucket (raw data lake)
gsutil mb -l us-central1 gs://sec-pipeline-raw-$(gcloud config get-value project)

# Create BigQuery dataset
bq mk --dataset --location=US sec_insider
```

---

## Phase 1 — Watchlist & Config (30 minutes)

```python
# config/watchlist.py
# 50 companies: ticker → CIK (pre-resolved so you never need to hit the tickers API at runtime)

WATCHLIST = {
    "AAPL": "0000320193",
    "MSFT": "0000789019",
    "NVDA": "0001045810",
    "TSLA": "0001318605",
    "GOOGL": "0001652044",
    "AMZN": "0001018724",
    "META": "0001326801",
    "JPM":  "0000019617",
    "GS":   "0000886982",
    "NFLX": "0001065280",
    "BRK":  "0001067983",
    "V":    "0001403161",
    "MA":   "0001141391",
    "UNH":  "0000731766",
    "HD":   "0000354950",
    "PG":   "0000080424",
    "JNJ":  "0000200406",
    "ABBV": "0001551152",
    "MRK":  "0000310158",
    "PFE":  "0000078003",
    "KO":   "0000021344",
    "PEP":  "0000077476",
    "WMT":  "0000104169",
    "COST": "0000909832",
    "TGT":  "0000027419",
    "BAC":  "0000070858",
    "WFC":  "0000072971",
    "C":    "0000831001",
    "MS":   "0000895421",
    "BLK":  "0001364742",
    "ORCL": "0001341439",
    "CRM":  "0001108524",
    "ADBE": "0000796343",
    "AMD":  "0000002488",
    "INTC": "0000050863",
    "QCOM": "0000804328",
    "TXN":  "0000097476",
    "NOW":  "0001373715",
    "SNOW": "0001640147",
    "UBER": "0001543151",
    "LYFT": "0001759509",
    "SHOP": "0001594805",
    "SQ":   "0001512673",
    "PYPL": "0001633917",
    "COIN": "0001679788",
    "SPOT": "0001639920",
    "NFLX": "0001065280",
    "DIS":  "0001001039",
    "CMCSA":"0001166691",
    "T":    "0000732717",
}

# config/settings.py
import os
from dotenv import load_dotenv
load_dotenv()

FINNHUB_KEY      = os.getenv("FINNHUB_KEY")
FRED_KEY         = os.getenv("FRED_KEY")
GCP_PROJECT      = os.getenv("GCP_PROJECT_ID", "sec-insider-pipeline-001")
GCS_BUCKET       = os.getenv("GCS_BUCKET", f"sec-pipeline-raw-{GCP_PROJECT}")
BQ_DATASET       = "sec_insider"
SEC_HEADERS      = {"User-Agent": "YourName yourname@gmail.com"}
SEC_RATE_LIMIT   = 0.12  # sleep between requests = 10 req/sec max
```

---

## Phase 2 — BigQuery Schema (30 minutes)

```sql
-- infra/bigquery_schema.sql
-- Run: bq query --use_legacy_sql=false < infra/bigquery_schema.sql

-- RAW filings table (one row per Form 4 transaction line)
CREATE TABLE IF NOT EXISTS `sec_insider.filings_raw` (
  accession_number    STRING NOT NULL,
  ticker              STRING,
  cik                 STRING,
  company_name        STRING,
  insider_name        STRING,
  insider_role        STRING,       -- 'officer', 'director', '10pct_owner'
  transaction_date    DATE,
  transaction_code    STRING,       -- 'P'=buy, 'S'=sell, 'A'=award, 'D'=disposition
  shares              FLOAT64,
  price_per_share     FLOAT64,
  total_value_usd     FLOAT64,
  post_trade_shares   FLOAT64,
  filing_date         DATE,
  raw_gcs_path        STRING,       -- gs:// path to original XML
  ingested_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY filing_date
OPTIONS (require_partition_filter = false);

-- ENRICHED prices table (Finnhub data ±10 days around each trade)
CREATE TABLE IF NOT EXISTS `sec_insider.prices_enriched` (
  ticker              STRING NOT NULL,
  price_date          DATE NOT NULL,
  open                FLOAT64,
  high                FLOAT64,
  low                 FLOAT64,
  close               FLOAT64,
  prev_close          FLOAT64,
  pct_change          FLOAT64,      -- (close - prev_close) / prev_close * 100
  fetched_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY price_date;

-- MACRO context table (FRED data)
CREATE TABLE IF NOT EXISTS `sec_insider.macro_context` (
  macro_date          DATE NOT NULL,
  sp500               FLOAT64,
  vix                 FLOAT64,
  fed_funds_rate      FLOAT64,
  treasury_10y        FLOAT64,
  fetched_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY macro_date;

-- ALERTS table (output of scoring DAG)
CREATE TABLE IF NOT EXISTS `sec_insider.insider_alerts` (
  alert_id            STRING NOT NULL,   -- MD5 of accession_number + ticker
  accession_number    STRING,
  ticker              STRING,
  insider_name        STRING,
  insider_role        STRING,
  transaction_date    DATE,
  transaction_code    STRING,
  total_value_usd     FLOAT64,
  price_7d_after      FLOAT64,
  price_change_7d_pct FLOAT64,
  vix_on_trade_date   FLOAT64,
  suspicion_score     FLOAT64,          -- 0.0 to 1.0
  flag_reason         STRING,           -- human-readable reason
  alerted_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY transaction_date;

-- COMPANY dimension table (static, loaded once)
CREATE TABLE IF NOT EXISTS `sec_insider.company_dim` (
  ticker              STRING NOT NULL,
  cik                 STRING NOT NULL,
  company_name        STRING,
  sector              STRING,
  market_cap_bn       FLOAT64
);
```

---

## Phase 3 — Ingestion Layer: Cloud Functions (Day 1, Afternoon, ~3 hours)

### 3.1 Cloud Function 1: fetch_filings

```python
# ingestion/fetch_filings/main.py
import functions_framework
import requests
import json
import time
from datetime import date, timedelta
from google.cloud import storage
from config.watchlist import WATCHLIST
from config.settings import SEC_HEADERS, GCS_BUCKET, SEC_RATE_LIMIT

@functions_framework.http
def fetch_filings(request):
    """
    Cloud Function: polls SEC EDGAR for Form 4 filings
    for each ticker in WATCHLIST filed in the last 2 days.
    Writes raw XML + parsed JSON to GCS.
    Returns count of filings fetched.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)

    today = date.today().isoformat()
    two_days_ago = (date.today() - timedelta(days=2)).isoformat()
    total_fetched = 0

    for ticker, cik in WATCHLIST.items():
        try:
            # Get submission history for this CIK
            url = f"https://data.sec.gov/submissions/CIK{cik}.json"
            resp = requests.get(url, headers=SEC_HEADERS, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            recent = data.get("filings", {}).get("recent", {})
            forms      = recent.get("form", [])
            dates      = recent.get("filingDate", [])
            accessions = recent.get("accessionNumber", [])
            documents  = recent.get("primaryDocument", [])

            for i, form in enumerate(forms):
                if form != "4":
                    continue
                if dates[i] < two_days_ago:
                    continue

                accession = accessions[i]
                doc       = documents[i]
                gcs_path  = f"filings/{today}/{ticker}/{accession.replace('-','')}/{doc}"

                # Skip if already fetched (idempotent)
                blob = bucket.blob(gcs_path)
                if blob.exists():
                    continue

                # Fetch the actual Form 4 XML
                cik_int = str(int(cik))
                filing_url = (
                    f"https://www.sec.gov/Archives/edgar/data/"
                    f"{cik_int}/{accession.replace('-','')}/{doc}"
                )
                xml_resp = requests.get(filing_url, headers=SEC_HEADERS, timeout=15)
                xml_resp.raise_for_status()

                # Write raw XML to GCS
                blob.upload_from_string(
                    xml_resp.text,
                    content_type="application/xml"
                )

                # Write metadata sidecar JSON to GCS
                meta = {
                    "ticker": ticker,
                    "cik": cik,
                    "accession_number": accession,
                    "filing_date": dates[i],
                    "primary_document": doc,
                    "gcs_xml_path": f"gs://{GCS_BUCKET}/{gcs_path}",
                    "fetched_at": today
                }
                meta_blob = bucket.blob(gcs_path.replace(".xml", "_meta.json"))
                meta_blob.upload_from_string(
                    json.dumps(meta, indent=2),
                    content_type="application/json"
                )

                total_fetched += 1
                time.sleep(SEC_RATE_LIMIT)

        except Exception as e:
            print(f"[ERROR] {ticker} ({cik}): {e}")
            continue

    return {"status": "ok", "filings_fetched": total_fetched, "date": today}, 200
```

### 3.2 Cloud Function 2: fetch_prices

```python
# ingestion/fetch_prices/main.py
import functions_framework
import requests
import json
from datetime import date, timedelta
from google.cloud import storage
from config.settings import FINNHUB_KEY, GCS_BUCKET

FINNHUB_BASE = "https://finnhub.io/api/v1"

@functions_framework.http
def fetch_prices(request):
    """
    Accepts JSON body: {"tickers": ["AAPL", "TSLA", ...]}
    Fetches current quote from Finnhub for each ticker.
    Writes to GCS: prices/YYYY-MM-DD/{ticker}.json
    Called by Airflow DAG 2 with only the tickers that had filings today.
    """
    body = request.get_json(silent=True) or {}
    tickers = body.get("tickers", [])
    if not tickers:
        return {"error": "no tickers provided"}, 400

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    today = date.today().isoformat()
    fetched = []

    for ticker in tickers:
        try:
            # Current quote
            quote_url = f"{FINNHUB_BASE}/quote"
            q = requests.get(
                quote_url,
                params={"symbol": ticker, "token": FINNHUB_KEY},
                timeout=10
            ).json()

            # Company news (last 7 days — for sentiment context)
            news_url = f"{FINNHUB_BASE}/company-news"
            week_ago = (date.today() - timedelta(days=7)).isoformat()
            news = requests.get(
                news_url,
                params={
                    "symbol": ticker,
                    "from": week_ago,
                    "to": today,
                    "token": FINNHUB_KEY
                },
                timeout=10
            ).json()

            payload = {
                "ticker": ticker,
                "date": today,
                "current_price": q.get("c"),
                "high": q.get("h"),
                "low": q.get("l"),
                "open": q.get("o"),
                "prev_close": q.get("pc"),
                "pct_change": round(
                    ((q.get("c", 0) - q.get("pc", 1)) / q.get("pc", 1)) * 100, 4
                ) if q.get("pc") else None,
                "news_count_7d": len(news) if isinstance(news, list) else 0,
                "fetched_at": today
            }

            gcs_path = f"prices/{today}/{ticker}.json"
            bucket.blob(gcs_path).upload_from_string(
                json.dumps(payload, indent=2),
                content_type="application/json"
            )
            fetched.append(ticker)

        except Exception as e:
            print(f"[ERROR] price fetch {ticker}: {e}")

    return {"status": "ok", "fetched": fetched}, 200
```

### 3.3 Cloud Function 3: fetch_macro

```python
# ingestion/fetch_macro/main.py
import functions_framework
import requests
import json
from datetime import date, timedelta
from google.cloud import storage
from config.settings import FRED_KEY, GCS_BUCKET

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

SERIES = {
    "sp500":          "SP500",
    "vix":            "VIXCLS",
    "fed_funds_rate": "FEDFUNDS",
    "treasury_10y":   "DGS10",
}

@functions_framework.http
def fetch_macro(request):
    """
    Fetches last 30 days of FRED macro indicators.
    Writes to GCS: macro/YYYY-MM-DD/macro.json
    Runs daily via Cloud Scheduler.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    today = date.today().isoformat()
    thirty_days_ago = (date.today() - timedelta(days=30)).isoformat()

    macro_data = {"date": today, "series": {}}

    for field, series_id in SERIES.items():
        try:
            resp = requests.get(
                FRED_BASE,
                params={
                    "series_id":   series_id,
                    "api_key":     FRED_KEY,
                    "file_type":   "json",
                    "sort_order":  "desc",
                    "limit":       30,
                    "observation_start": thirty_days_ago,
                },
                timeout=15
            )
            resp.raise_for_status()
            observations = resp.json().get("observations", [])

            # Keep only valid non-"." values
            clean = [
                {"date": o["date"], "value": float(o["value"])}
                for o in observations
                if o["value"] not in (".", "")
            ]
            macro_data["series"][field] = clean

        except Exception as e:
            print(f"[ERROR] FRED {series_id}: {e}")

    gcs_path = f"macro/{today}/macro.json"
    bucket.blob(gcs_path).upload_from_string(
        json.dumps(macro_data, indent=2),
        content_type="application/json"
    )
    return {"status": "ok", "date": today}, 200
```

### 3.4 Deploy all three Cloud Functions

```bash
# Deploy fetch_filings
gcloud functions deploy fetch_filings \
  --gen2 \
  --runtime python312 \
  --region us-central1 \
  --entry-point fetch_filings \
  --trigger-http \
  --allow-unauthenticated \
  --memory 512MB \
  --timeout 540s \
  --set-env-vars GCS_BUCKET=sec-pipeline-raw-sec-insider-pipeline-001 \
  --source ingestion/fetch_filings/

# Deploy fetch_prices
gcloud functions deploy fetch_prices \
  --gen2 \
  --runtime python312 \
  --region us-central1 \
  --entry-point fetch_prices \
  --trigger-http \
  --allow-unauthenticated \
  --memory 256MB \
  --timeout 120s \
  --set-env-vars GCS_BUCKET=...,FINNHUB_KEY=... \
  --source ingestion/fetch_prices/

# Deploy fetch_macro
gcloud functions deploy fetch_macro \
  --gen2 \
  --runtime python312 \
  --region us-central1 \
  --entry-point fetch_macro \
  --trigger-http \
  --allow-unauthenticated \
  --memory 256MB \
  --timeout 120s \
  --set-env-vars GCS_BUCKET=...,FRED_KEY=... \
  --source ingestion/fetch_macro/
```

---

## Phase 4 — Form 4 XML Parser (1 hour)

Form 4 is XML. You need to parse it into structured rows before loading to BigQuery.

```python
# transforms/parse_form4.py
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional

def parse_form4_xml(xml_text: str, ticker: str, cik: str,
                    accession_number: str, filing_date: str,
                    gcs_path: str) -> List[Dict]:
    """
    Parse Form 4 XML into a list of transaction dicts.
    One Form 4 can have multiple transaction rows.
    Returns: list of row dicts matching BigQuery schema.
    """
    rows = []
    try:
        root = ET.fromstring(xml_text)
        ns = {"sec": "http://www.sec.gov/edgar/ownership"}

        # Insider identity
        owner = root.find(".//reportingOwner")
        insider_name = _text(owner, "reportingOwnerRelationship/../reportingOwnerId/rptOwnerName") \
                    or _text(root, ".//rptOwnerName") or "Unknown"
        
        role_node = root.find(".//reportingOwnerRelationship")
        insider_role = _derive_role(role_node)

        issuer = root.find(".//issuer")
        company_name = _text(issuer, "issuerName") or ""

        # Non-derivative transactions (actual stock buys/sells)
        for txn in root.findall(".//nonDerivativeTransaction"):
            row = _build_row(
                txn, ticker, cik, company_name, insider_name,
                insider_role, accession_number, filing_date, gcs_path
            )
            if row:
                rows.append(row)

        # Derivative transactions (options, RSUs)
        for txn in root.findall(".//derivativeTransaction"):
            row = _build_row(
                txn, ticker, cik, company_name, insider_name,
                insider_role, accession_number, filing_date, gcs_path,
                is_derivative=True
            )
            if row:
                rows.append(row)

    except ET.ParseError as e:
        print(f"[PARSE ERROR] {accession_number}: {e}")
    return rows


def _build_row(txn, ticker, cik, company_name, insider_name,
               insider_role, accession_number, filing_date, gcs_path,
               is_derivative=False) -> Optional[Dict]:
    try:
        txn_date   = _text(txn, "transactionDate/value") or filing_date
        txn_code   = _text(txn, "transactionCoding/transactionCode") or ""
        shares_str = _text(txn, "transactionAmounts/transactionShares/value") or "0"
        price_str  = _text(txn, "transactionAmounts/transactionPricePerShare/value") or "0"
        post_str   = _text(txn, "postTransactionAmounts/sharesOwnedFollowingTransaction/value") or "0"

        shares = float(shares_str or 0)
        price  = float(price_str or 0)
        post   = float(post_str or 0)

        # Sells are negative in our model
        if txn_code in ("S", "D"):
            shares = -abs(shares)

        return {
            "accession_number": accession_number,
            "ticker":           ticker,
            "cik":              cik,
            "company_name":     company_name,
            "insider_name":     insider_name,
            "insider_role":     insider_role,
            "transaction_date": txn_date,
            "transaction_code": txn_code,
            "shares":           shares,
            "price_per_share":  price,
            "total_value_usd":  abs(shares) * price,
            "post_trade_shares": post,
            "filing_date":      filing_date,
            "raw_gcs_path":     gcs_path,
        }
    except Exception as e:
        print(f"[ROW BUILD ERROR]: {e}")
        return None


def _text(node, path) -> Optional[str]:
    if node is None:
        return None
    el = node.find(path)
    return el.text.strip() if el is not None and el.text else None


def _derive_role(role_node) -> str:
    if role_node is None:
        return "unknown"
    flags = {
        "isDirector":   "director",
        "isOfficer":    "officer",
        "isTenPercentOwner": "10pct_owner",
    }
    for tag, label in flags.items():
        if _text(role_node, tag) == "1":
            return label
    return "other"
```

---

## Phase 5 — Airflow DAGs (Day 1 Evening, ~3 hours)

### DAG 1: sec_ingest — runs every 15 minutes

```python
# dags/dag_sec_ingest.py
from datetime import datetime, timedelta
import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from google.cloud import storage, bigquery
from transforms.parse_form4 import parse_form4_xml
from config.settings import GCS_BUCKET, GCP_PROJECT, BQ_DATASET

CLOUD_FUNCTION_URL = "https://us-central1-{PROJECT}.cloudfunctions.net/fetch_filings"

default_args = {
    "owner": "yourname",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

def trigger_fetch(**context):
    """Task 1: Call Cloud Function to fetch new filings to GCS."""
    resp = requests.post(
        CLOUD_FUNCTION_URL.format(PROJECT=GCP_PROJECT),
        timeout=600
    )
    resp.raise_for_status()
    result = resp.json()
    print(f"Fetched {result['filings_fetched']} new filings")
    context["ti"].xcom_push(key="fetch_result", value=result)


def parse_and_load(**context):
    """Task 2: Read raw XMLs from GCS, parse, load to BigQuery."""
    from datetime import date
    storage_client = storage.Client()
    bq_client      = bigquery.Client(project=GCP_PROJECT)
    bucket         = storage_client.bucket(GCS_BUCKET)
    today          = date.today().isoformat()

    # List all meta JSONs from today's run
    blobs = list(bucket.list_blobs(prefix=f"filings/{today}/"))
    meta_blobs = [b for b in blobs if b.name.endswith("_meta.json")]

    rows_to_insert = []
    tickers_seen   = set()

    for meta_blob in meta_blobs:
        meta = json.loads(meta_blob.download_as_text())
        ticker     = meta["ticker"]
        accession  = meta["accession_number"]
        filing_date= meta["filing_date"]
        gcs_xml    = meta["gcs_xml_path"]
        cik        = meta["cik"]

        # Fetch the XML blob
        xml_path = gcs_xml.replace(f"gs://{GCS_BUCKET}/", "")
        xml_blob = bucket.blob(xml_path)
        if not xml_blob.exists():
            continue

        xml_text = xml_blob.download_as_text()
        rows = parse_form4_xml(
            xml_text, ticker, cik,
            accession, filing_date, gcs_xml
        )
        rows_to_insert.extend(rows)
        tickers_seen.add(ticker)

    if rows_to_insert:
        table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.filings_raw"
        errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
        print(f"Loaded {len(rows_to_insert)} rows to {table_ref}")

    # Push tickers for downstream DAG
    context["ti"].xcom_push(key="tickers_with_filings", value=list(tickers_seen))


with DAG(
    dag_id="sec_ingest",
    default_args=default_args,
    description="Fetch SEC Form 4 filings → parse → load to BigQuery",
    schedule_interval="*/15 * * * *",      # every 15 minutes
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["sec", "ingestion"],
) as dag:

    start = EmptyOperator(task_id="start")

    fetch  = PythonOperator(task_id="trigger_fetch",    python_callable=trigger_fetch)
    parse  = PythonOperator(task_id="parse_and_load",   python_callable=parse_and_load)

    start >> fetch >> parse
```

### DAG 2: price_enrichment — runs hourly

```python
# dags/dag_price_enrichment.py
from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from google.cloud import bigquery
from config.settings import GCP_PROJECT, BQ_DATASET, GCS_BUCKET

PRICE_FUNCTION_URL = "https://us-central1-{PROJECT}.cloudfunctions.net/fetch_prices"

default_args = {
    "owner": "yourname",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def get_todays_tickers(**context):
    """Query BigQuery for unique tickers that filed today."""
    from datetime import date
    bq = bigquery.Client(project=GCP_PROJECT)
    today = date.today().isoformat()
    query = f"""
        SELECT DISTINCT ticker
        FROM `{GCP_PROJECT}.{BQ_DATASET}.filings_raw`
        WHERE filing_date = '{today}'
        AND ticker IS NOT NULL
    """
    result = list(bq.query(query).result())
    tickers = [r["ticker"] for r in result]
    print(f"Tickers with filings today: {tickers}")
    context["ti"].xcom_push(key="tickers", value=tickers)


def fetch_and_load_prices(**context):
    """Call Cloud Function with today's tickers, then load GCS JSON to BQ."""
    from datetime import date
    from google.cloud import storage
    import json

    tickers = context["ti"].xcom_pull(task_ids="get_todays_tickers", key="tickers")
    if not tickers:
        print("No tickers today. Skipping.")
        return

    # Call Cloud Function
    resp = requests.post(
        PRICE_FUNCTION_URL.format(PROJECT=GCP_PROJECT),
        json={"tickers": tickers},
        timeout=120
    )
    resp.raise_for_status()

    # Load price JSONs from GCS to BigQuery
    today = date.today().isoformat()
    storage_client = storage.Client()
    bq_client      = bigquery.Client(project=GCP_PROJECT)
    bucket         = storage_client.bucket(GCS_BUCKET)
    table_ref      = f"{GCP_PROJECT}.{BQ_DATASET}.prices_enriched"

    rows = []
    for ticker in tickers:
        blob = bucket.blob(f"prices/{today}/{ticker}.json")
        if not blob.exists():
            continue
        data = json.loads(blob.download_as_text())
        rows.append({
            "ticker":      data["ticker"],
            "price_date":  data["date"],
            "open":        data.get("open"),
            "high":        data.get("high"),
            "low":         data.get("low"),
            "close":       data.get("current_price"),
            "prev_close":  data.get("prev_close"),
            "pct_change":  data.get("pct_change"),
        })

    if rows:
        errors = bq_client.insert_rows_json(table_ref, rows)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
        print(f"Loaded {len(rows)} price rows")


with DAG(
    dag_id="price_enrichment",
    default_args=default_args,
    description="Fetch Finnhub prices for tickers with today's filings",
    schedule_interval="@hourly",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["prices", "enrichment"],
) as dag:

    get_tickers   = PythonOperator(task_id="get_todays_tickers",     python_callable=get_todays_tickers)
    fetch_prices  = PythonOperator(task_id="fetch_and_load_prices",  python_callable=fetch_and_load_prices)

    get_tickers >> fetch_prices
```

### DAG 3: flag_suspicious — runs daily at 11 PM

```python
# dags/dag_flag_suspicious.py
from datetime import datetime, timedelta
import hashlib
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from config.settings import GCP_PROJECT, BQ_DATASET

default_args = {
    "owner": "yourname",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# Suspicion scoring thresholds
LARGE_SELL_THRESHOLD   = 500_000   # USD
LARGE_BUY_THRESHOLD    = 1_000_000 # USD
PRICE_DROP_THRESHOLD   = -5.0      # % within 7 days after sell
PRICE_SURGE_THRESHOLD  = 10.0      # % within 7 days after buy
HIGH_VIX_THRESHOLD     = 25.0      # market stress level


def compute_and_store_alerts(**context):
    """
    Joins filings_raw + prices_enriched + macro_context.
    Scores each trade and writes flagged rows to insider_alerts.
    Scoring logic:
      +0.3  large sale (>$500k)
      +0.3  stock drops >5% within 7 days of sell
      +0.2  filed by officer (not just director)
      +0.1  trade during high VIX (>25) — unusual timing
      +0.1  stock surges >10% within 7 days of buy
    Max score: 1.0
    Threshold for alert: score >= 0.4
    """
    from datetime import date
    bq = bigquery.Client(project=GCP_PROJECT)
    today = date.today().isoformat()

    query = f"""
    WITH filings AS (
        SELECT
            f.accession_number,
            f.ticker,
            f.insider_name,
            f.insider_role,
            f.transaction_date,
            f.transaction_code,
            f.total_value_usd,
            f.shares
        FROM `{GCP_PROJECT}.{BQ_DATASET}.filings_raw` f
        WHERE f.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          AND f.transaction_code IN ('S', 'P')  -- sells and buys only
          AND f.total_value_usd > 10000          -- ignore tiny trades
    ),
    prices_at_trade AS (
        SELECT ticker, price_date, close, pct_change
        FROM `{GCP_PROJECT}.{BQ_DATASET}.prices_enriched`
    ),
    prices_7d_later AS (
        SELECT
            p1.ticker,
            p1.price_date AS trade_date,
            p2.close AS price_7d_after,
            SAFE_DIVIDE(p2.close - p1.close, p1.close) * 100 AS price_change_7d_pct
        FROM prices_at_trade p1
        JOIN prices_at_trade p2
          ON p1.ticker = p2.ticker
          AND p2.price_date = DATE_ADD(p1.price_date, INTERVAL 7 DAY)
    ),
    macro AS (
        SELECT macro_date, vix, sp500
        FROM `{GCP_PROJECT}.{BQ_DATASET}.macro_context`
    )

    SELECT
        f.accession_number,
        f.ticker,
        f.insider_name,
        f.insider_role,
        f.transaction_date,
        f.transaction_code,
        f.total_value_usd,
        p7.price_7d_after,
        p7.price_change_7d_pct,
        m.vix AS vix_on_trade_date,

        -- Suspicion scoring
        ROUND(
          CASE WHEN f.transaction_code = 'S'
                AND f.total_value_usd > {LARGE_SELL_THRESHOLD}
               THEN 0.3 ELSE 0.0 END
        + CASE WHEN f.transaction_code = 'S'
                AND p7.price_change_7d_pct < {PRICE_DROP_THRESHOLD}
               THEN 0.3 ELSE 0.0 END
        + CASE WHEN f.insider_role = 'officer'    THEN 0.2 ELSE 0.0 END
        + CASE WHEN m.vix > {HIGH_VIX_THRESHOLD}  THEN 0.1 ELSE 0.0 END
        + CASE WHEN f.transaction_code = 'P'
                AND p7.price_change_7d_pct > {PRICE_SURGE_THRESHOLD}
               THEN 0.1 ELSE 0.0 END,
        2) AS suspicion_score,

        CONCAT(
          CASE WHEN f.total_value_usd > {LARGE_SELL_THRESHOLD}
               THEN 'Large sale >$500k. ' ELSE '' END,
          CASE WHEN p7.price_change_7d_pct < {PRICE_DROP_THRESHOLD}
               THEN 'Stock dropped >5% in 7d after sell. ' ELSE '' END,
          CASE WHEN m.vix > {HIGH_VIX_THRESHOLD}
               THEN 'High VIX (market stress). ' ELSE '' END
        ) AS flag_reason

    FROM filings f
    LEFT JOIN prices_7d_later p7
           ON f.ticker = p7.ticker
           AND f.transaction_date = p7.trade_date
    LEFT JOIN macro m
           ON f.transaction_date = m.macro_date
    WHERE
        -- Only surface meaningful scores
        (
          CASE WHEN f.transaction_code = 'S' AND f.total_value_usd > {LARGE_SELL_THRESHOLD} THEN 0.3 ELSE 0.0 END
        + CASE WHEN f.transaction_code = 'S' AND p7.price_change_7d_pct < {PRICE_DROP_THRESHOLD} THEN 0.3 ELSE 0.0 END
        + CASE WHEN f.insider_role = 'officer' THEN 0.2 ELSE 0.0 END
        + CASE WHEN m.vix > {HIGH_VIX_THRESHOLD} THEN 0.1 ELSE 0.0 END
        + CASE WHEN f.transaction_code = 'P' AND p7.price_change_7d_pct > {PRICE_SURGE_THRESHOLD} THEN 0.1 ELSE 0.0 END
        ) >= 0.4
    """

    results = list(bq.query(query).result())
    alert_rows = []
    for row in results:
        alert_id = hashlib.md5(
            f"{row.accession_number}{row.ticker}".encode()
        ).hexdigest()
        alert_rows.append({
            "alert_id":            alert_id,
            "accession_number":    row.accession_number,
            "ticker":              row.ticker,
            "insider_name":        row.insider_name,
            "insider_role":        row.insider_role,
            "transaction_date":    str(row.transaction_date),
            "transaction_code":    row.transaction_code,
            "total_value_usd":     row.total_value_usd,
            "price_7d_after":      row.price_7d_after,
            "price_change_7d_pct": row.price_change_7d_pct,
            "vix_on_trade_date":   row.vix_on_trade_date,
            "suspicion_score":     float(row.suspicion_score),
            "flag_reason":         row.flag_reason,
        })

    if alert_rows:
        table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.insider_alerts"
        errors = bq.insert_rows_json(table_ref, alert_rows)
        if errors:
            raise RuntimeError(f"Alert insert errors: {errors}")
        print(f"Stored {len(alert_rows)} alerts")
    else:
        print("No new alerts today.")


with DAG(
    dag_id="flag_suspicious",
    default_args=default_args,
    description="Score trades and write alerts to BigQuery",
    schedule_interval="0 23 * * *",  # daily at 11 PM
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["scoring", "alerts"],
) as dag:

    score = PythonOperator(
        task_id="compute_and_store_alerts",
        python_callable=compute_and_store_alerts
    )
```

### DAG 4: weekly_report — runs Monday 6 AM

```python
# dags/dag_weekly_report.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery, storage
from config.settings import GCP_PROJECT, BQ_DATASET, GCS_BUCKET
import pandas as pd

default_args = {"owner": "yourname", "retries": 1}

def export_weekly_parquet(**context):
    """Export last 7 days of alerts as Parquet to GCS for archival."""
    from datetime import date
    bq  = bigquery.Client(project=GCP_PROJECT)
    today = date.today().isoformat()

    query = f"""
        SELECT a.*, f.company_name, f.shares
        FROM `{GCP_PROJECT}.{BQ_DATASET}.insider_alerts` a
        LEFT JOIN `{GCP_PROJECT}.{BQ_DATASET}.filings_raw` f
          ON a.accession_number = f.accession_number
        WHERE a.transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        ORDER BY a.suspicion_score DESC
    """
    df = bq.query(query).to_dataframe()

    if df.empty:
        print("No alerts this week.")
        return

    # Write to GCS as Parquet
    gcs_path = f"gs://{GCS_BUCKET}/reports/weekly/{today}/insider_alerts.parquet"
    df.to_parquet(gcs_path, index=False, engine="pyarrow")
    print(f"Exported {len(df)} rows to {gcs_path}")

    # Also write as CSV for easy viewing
    csv_path = f"gs://{GCS_BUCKET}/reports/weekly/{today}/insider_alerts.csv"
    df.to_csv(csv_path, index=False)
    print(f"Also exported CSV to {csv_path}")


with DAG(
    dag_id="weekly_report",
    default_args=default_args,
    description="Export weekly alert summary to GCS as Parquet",
    schedule_interval="0 6 * * 1",  # every Monday at 6 AM
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["reporting", "export"],
) as dag:

    export = PythonOperator(
        task_id="export_weekly_parquet",
        python_callable=export_weekly_parquet
    )
```

---

## Phase 6 — Deploy Airflow on GCP e2-micro VM (Day 2, Morning, ~1.5 hours)

```bash
# Create free e2-micro VM (Always Free tier in us-central1)
gcloud compute instances create airflow-vm \
  --machine-type e2-micro \
  --zone us-central1-a \
  --image-family ubuntu-2404-lts \
  --image-project ubuntu-os-cloud \
  --boot-disk-size 30GB \
  --tags http-server,https-server

# SSH into VM
gcloud compute ssh airflow-vm --zone us-central1-a

# --- On the VM ---
# Install Docker
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 git
sudo usermod -aG docker $USER
newgrp docker

# Clone your repo
git clone https://github.com/yourusername/sec-insider-pipeline.git
cd sec-insider-pipeline

# Copy .env with your real keys
nano .env  # add FINNHUB_KEY, FRED_KEY, GCP_PROJECT_ID, etc.

# Start Airflow
docker compose up -d

# Give Airflow a minute, then check
docker compose ps  # all should be "running"

# Set up GCP auth inside the VM
# Option A: Service account key (simpler for portfolio)
# Create SA in GCP Console → IAM → Service Accounts
# Grant roles: BigQuery Data Editor, Storage Object Admin, Cloud Functions Invoker
# Download JSON key → upload to VM via scp
gcloud compute scp service-account-key.json airflow-vm:~/sec-insider-pipeline/
# Then set in docker-compose: GOOGLE_APPLICATION_CREDENTIALS=/app/service-account-key.json

# Confirm Airflow UI is accessible
# In GCP Console → VM → External IP:8080
# You may need to add firewall rule:
gcloud compute firewall-rules create allow-airflow \
  --allow tcp:8080 \
  --source-ranges 0.0.0.0/0 \
  --description "Airflow UI for portfolio project"
```

---

## Phase 7 — Cloud Scheduler (15 minutes)

Cloud Scheduler will act as the master cron that triggers your Cloud Functions. Airflow then handles the downstream DAG logic.

```bash
# Trigger fetch_filings every 15 minutes
gcloud scheduler jobs create http trigger-sec-filings \
  --schedule "*/15 * * * *" \
  --uri "https://us-central1-YOUR_PROJECT.cloudfunctions.net/fetch_filings" \
  --http-method POST \
  --location us-central1 \
  --description "Trigger SEC filings ingestion every 15 min"

# Trigger fetch_macro daily at midnight
gcloud scheduler jobs create http trigger-macro \
  --schedule "0 0 * * *" \
  --uri "https://us-central1-YOUR_PROJECT.cloudfunctions.net/fetch_macro" \
  --http-method POST \
  --location us-central1 \
  --description "Trigger FRED macro data daily"
```

> **Note:** You get 3 free Cloud Scheduler jobs. Use 2 for the above; keep the 3rd as spare. Finnhub prices are triggered on-demand by Airflow DAG 2 (not by Scheduler), so you don't need a 3rd job for it.

---

## Phase 8 — Serving Layer: FastAPI + Streamlit (Day 2, Afternoon, ~2 hours)

### 8.1 FastAPI — Public REST API

```python
# serving/api/main.py
from fastapi import FastAPI, Query
from google.cloud import bigquery
from typing import Optional
import os

app = FastAPI(
    title="SEC Insider Trading Intelligence API",
    description="Real-time insider trade alerts scored for suspicious activity",
    version="1.0.0"
)

PROJECT  = os.getenv("GCP_PROJECT_ID", "sec-insider-pipeline-001")
DATASET  = "sec_insider"
BQ = bigquery.Client(project=PROJECT)


@app.get("/")
def root():
    return {
        "project": "SEC Insider Trading Intelligence Pipeline",
        "endpoints": ["/alerts", "/scores/top", "/filings/recent", "/health"]
    }


@app.get("/alerts")
def get_alerts(
    min_score: float = Query(0.4, description="Minimum suspicion score (0.0–1.0)"),
    ticker: Optional[str] = Query(None, description="Filter by ticker"),
    limit: int = Query(50, le=200)
):
    """Get suspicious insider trades above a given score threshold."""
    ticker_filter = f"AND ticker = '{ticker.upper()}'" if ticker else ""
    query = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.insider_alerts`
        WHERE suspicion_score >= {min_score}
        {ticker_filter}
        ORDER BY suspicion_score DESC, alerted_at DESC
        LIMIT {limit}
    """
    rows = list(BQ.query(query).result())
    return {"count": len(rows), "alerts": [dict(r) for r in rows]}


@app.get("/scores/top")
def top_scores(days: int = Query(7, le=30)):
    """Top 10 highest scored alerts in the last N days."""
    query = f"""
        SELECT ticker, insider_name, insider_role,
               transaction_date, transaction_code,
               total_value_usd, suspicion_score, flag_reason
        FROM `{PROJECT}.{DATASET}.insider_alerts`
        WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        ORDER BY suspicion_score DESC
        LIMIT 10
    """
    rows = list(BQ.query(query).result())
    return {"top_alerts": [dict(r) for r in rows]}


@app.get("/filings/recent")
def recent_filings(ticker: Optional[str] = None, limit: int = 50):
    """Raw filings from the last 7 days."""
    ticker_filter = f"AND ticker = '{ticker.upper()}'" if ticker else ""
    query = f"""
        SELECT ticker, insider_name, insider_role,
               transaction_date, transaction_code,
               shares, price_per_share, total_value_usd, filing_date
        FROM `{PROJECT}.{DATASET}.filings_raw`
        WHERE filing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        {ticker_filter}
        ORDER BY filing_date DESC
        LIMIT {limit}
    """
    rows = list(BQ.query(query).result())
    return {"filings": [dict(r) for r in rows]}


@app.get("/health")
def health():
    return {"status": "ok"}
```

```dockerfile
# serving/api/Dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
```

```
# serving/api/requirements.txt
fastapi==0.111.0
uvicorn==0.30.0
google-cloud-bigquery==3.25.0
pandas==2.2.2
pyarrow==16.1.0
```

### 8.2 Streamlit Dashboard

```python
# serving/dashboard/app.py
import streamlit as st
import requests
import pandas as pd
import plotly.express as px

API_BASE = "https://api-HASH-uc.a.run.app"  # replace with your Cloud Run URL

st.set_page_config(
    page_title="SEC Insider Intelligence",
    page_icon="📊",
    layout="wide"
)

st.title("📊 SEC Insider Trading Intelligence")
st.caption("Real-time pipeline: SEC EDGAR → BigQuery → Scored alerts")

# Sidebar filters
st.sidebar.header("Filters")
min_score = st.sidebar.slider("Min suspicion score", 0.0, 1.0, 0.4, 0.1)
ticker_filter = st.sidebar.text_input("Filter by ticker (e.g. NVDA)").upper() or None
days = st.sidebar.slider("Lookback (days)", 1, 30, 7)

# Top alerts
st.subheader("🚨 Top Suspicious Trades")
try:
    params = {"min_score": min_score, "limit": 100}
    if ticker_filter:
        params["ticker"] = ticker_filter
    resp = requests.get(f"{API_BASE}/alerts", params=params, timeout=10)
    alerts = resp.json().get("alerts", [])
    df_alerts = pd.DataFrame(alerts)

    if df_alerts.empty:
        st.info("No alerts match the current filters.")
    else:
        df_alerts["transaction_date"] = pd.to_datetime(df_alerts["transaction_date"])
        df_alerts["total_value_usd"]  = df_alerts["total_value_usd"].apply(
            lambda x: f"${x:,.0f}"
        )

        st.dataframe(
            df_alerts[[
                "ticker", "insider_name", "insider_role",
                "transaction_date", "transaction_code",
                "total_value_usd", "suspicion_score", "flag_reason"
            ]].sort_values("suspicion_score", ascending=False),
            use_container_width=True,
            hide_index=True
        )

        # Score distribution
        st.subheader("Score Distribution")
        fig = px.histogram(
            pd.DataFrame(alerts),
            x="suspicion_score",
            nbins=10,
            color_discrete_sequence=["#e74c3c"],
            title="Suspicion Score Distribution"
        )
        st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Could not load alerts: {e}")

# Recent filings
st.subheader("📋 Recent Filings (Last 7 Days)")
try:
    params = {"limit": 50}
    if ticker_filter:
        params["ticker"] = ticker_filter
    resp = requests.get(f"{API_BASE}/filings/recent", params=params, timeout=10)
    filings = resp.json().get("filings", [])
    df_filings = pd.DataFrame(filings)

    if not df_filings.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Filings", len(df_filings))
        col2.metric("Unique Tickers", df_filings["ticker"].nunique())
        sells = df_filings[df_filings["transaction_code"] == "S"]
        col3.metric("Sell Transactions", len(sells))
        st.dataframe(df_filings, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Could not load filings: {e}")

st.markdown("---")
st.caption("Data: SEC EDGAR · Prices: Finnhub · Macro: FRED (St. Louis Fed)")
```

### 8.3 Deploy both to Cloud Run

```bash
# Build and deploy FastAPI
cd serving/api
gcloud run deploy sec-insider-api \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 512Mi \
  --set-env-vars GCP_PROJECT_ID=sec-insider-pipeline-001

# Build and deploy Streamlit
cd ../dashboard
gcloud run deploy sec-insider-dashboard \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 512Mi \
  --set-env-vars API_BASE=https://sec-insider-api-HASH-uc.a.run.app

# After deploy, GCP prints the public URLs:
# https://sec-insider-api-xyz.run.app     ← put this in your resume
# https://sec-insider-dashboard-xyz.run.app  ← link in your README
```

---

## Phase 9 — End-to-End Testing (1 hour)

```bash
# 1. Test Cloud Functions directly
curl -X POST https://us-central1-YOUR_PROJECT.cloudfunctions.net/fetch_filings
curl -X POST https://us-central1-YOUR_PROJECT.cloudfunctions.net/fetch_macro

# 2. Verify data landed in GCS
gsutil ls gs://sec-pipeline-raw-sec-insider-pipeline-001/filings/
gsutil ls gs://sec-pipeline-raw-sec-insider-pipeline-001/macro/

# 3. Check BigQuery tables
bq query --use_legacy_sql=false \
  "SELECT COUNT(*), ticker FROM sec_insider.filings_raw GROUP BY ticker ORDER BY 1 DESC LIMIT 10"

# 4. Trigger DAG 3 manually in Airflow UI (to test scoring without waiting for 11 PM)
# Airflow UI → flag_suspicious → Trigger DAG

# 5. Test REST API
curl https://sec-insider-api-xyz.run.app/alerts?min_score=0.3
curl https://sec-insider-api-xyz.run.app/scores/top?days=7

# 6. Open dashboard in browser
# https://sec-insider-dashboard-xyz.run.app
```

---

## Project Cost Breakdown

| Service | Free Tier | Your Usage | Monthly Cost |
|---|---|---|---|
| Cloud Functions | 2M invocations/mo | ~3,000/mo (50 tickers × 15-min polls) | **$0** |
| Cloud Scheduler | 3 jobs | 2 jobs | **$0** |
| Cloud Storage | 5 GB | ~200 MB/mo | **$0** |
| BigQuery | 1 TiB queries + 10 GB storage | <1 GB data, <10 GB queries | **$0** |
| Cloud Run | 2M requests/mo | ~10K requests/mo | **$0** |
| Compute Engine e2-micro | 1 VM/mo | 1 VM (Airflow) | **$0** |
| **Total** | | | **$0/month** |

---

## What To Put On Your Resume

```
SEC Insider Trading Intelligence Pipeline                              github.com/you/sec-insider-pipeline
  Live: https://sec-insider-dashboard-xyz.run.app

  Built a real-time data engineering pipeline ingesting SEC EDGAR Form 4 filings,
  Finnhub stock prices, and FRED macro indicators across a 50-company watchlist.

  • Multi-source ingestion: 3 Cloud Functions (Python 3.12) polling SEC EDGAR,
    Finnhub, and FRED APIs; raw data landed as JSON/Parquet in Cloud Storage (GCS)
  • Orchestration: 4 Airflow DAGs (ingest → enrich → score → report) on GCP
    e2-micro VM; dependency management, retries, XCom data passing between tasks
  • Warehouse: BigQuery partitioned tables (filings_raw, prices_enriched,
    insider_alerts); SQL scoring logic joining 3 sources
  • Serving: FastAPI REST API + Streamlit dashboard deployed on Cloud Run;
    public live URL with real-time BigQuery queries
  • Stack: Python · Apache Airflow 2.9 · GCP (Cloud Functions, Cloud Storage,
    BigQuery, Cloud Run, Cloud Scheduler, Compute Engine) · FastAPI · Streamlit · Docker
```

---

## Key Engineering Decisions to Explain in Interviews

**"Why Cloud Functions for ingestion instead of running it inside Airflow?"**
Cloud Functions are stateless, independently deployable, and retried by Cloud Scheduler separately from Airflow. If the SEC API is slow, it doesn't block Airflow's scheduler thread. The Airflow DAG just reads from GCS — decoupled from the fetch latency.

**"Why GCS as an intermediate layer instead of loading directly to BigQuery?"**
GCS acts as a durable raw layer. If the BigQuery schema changes, you can re-parse from GCS without re-fetching from the SEC. This is the medallion architecture pattern: raw → enriched → aggregated.

**"How do you handle duplicate filings?"**
The GCS write checks `blob.exists()` before writing — idempotent by design. BigQuery inserts are append-only; deduplication is handled in the scoring query with `DISTINCT accession_number`.

**"What breaks at 10x scale?"**
The e2-micro VM would be the bottleneck. The fix: move to Cloud Composer (managed Airflow) or Airflow on a larger VM. The Cloud Functions and BigQuery layers scale horizontally without any changes.

---

## Day-by-Day Timeline

### Day 1
- **Morning (2h):** Phase 0 + Phase 1 — local setup, GCP project, config, watchlist
- **Late morning (1h):** Phase 2 — BigQuery schema, deploy tables
- **Afternoon (3h):** Phase 3 — write + test all 3 Cloud Functions locally, then deploy to GCP
- **Evening (3h):** Phase 4 + Phase 5 — XML parser + write all 4 Airflow DAGs, test locally in Docker

### Day 2
- **Morning (1.5h):** Phase 6 — provision e2-micro VM, install Airflow, copy DAGs, verify UI
- **Late morning (30m):** Phase 7 — Cloud Scheduler jobs
- **Afternoon (2h):** Phase 8 — FastAPI + Streamlit, deploy to Cloud Run, get public URLs
- **Evening (1h):** Phase 9 — end-to-end testing, fix bugs, write README
- **Last 30 min:** Polish README with architecture diagram, live URL, resume bullet

---

*Built for recruitment showcase. All data from public APIs. Runs entirely within GCP's Always Free tier.*
