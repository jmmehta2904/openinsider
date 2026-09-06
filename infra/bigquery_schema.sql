-- BigQuery schema for the SEC EDGAR insider trading pipeline.
--
-- Run from Cloud Shell:
--   bq query --use_legacy_sql=false --location=us-central1 < infra/bigquery_schema.sql
--
-- The dataset is referenced without a project id so the script can be reused
-- across dev/prod GCP projects by changing the active gcloud project.

CREATE SCHEMA IF NOT EXISTS `sec_insider`
OPTIONS (
  location = "us-central1",
  description = "SEC EDGAR insider trading warehouse"
);

CREATE TABLE IF NOT EXISTS `sec_insider.company_dim` (
  ticker STRING NOT NULL OPTIONS(description = "Public market ticker used by external market-data APIs"),
  cik STRING NOT NULL OPTIONS(description = "SEC Central Index Key, stored as a zero-padded 10 digit string"),
  company_name STRING OPTIONS(description = "Issuer legal name"),
  exchange STRING OPTIONS(description = "Primary listing exchange when known"),
  sector STRING OPTIONS(description = "Business sector for dashboard grouping"),
  industry STRING OPTIONS(description = "Business industry for dashboard grouping"),
  is_active BOOL NOT NULL OPTIONS(description = "Whether the company is currently included in the watchlist"),
  valid_from DATE NOT NULL OPTIONS(description = "Date this dimension record became valid"),
  valid_to DATE OPTIONS(description = "Date this dimension record stopped being valid; NULL means current"),
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL
)
CLUSTER BY ticker, cik
OPTIONS (
  description = "Slow-changing company/watchlist dimension"
);

CREATE TABLE IF NOT EXISTS `sec_insider.filing_documents_raw` (
  accession_number STRING NOT NULL OPTIONS(description = "SEC filing accession number"),
  ticker STRING NOT NULL OPTIONS(description = "Ticker mapped from the project watchlist"),
  cik STRING NOT NULL OPTIONS(description = "Issuer CIK"),
  form_type STRING NOT NULL OPTIONS(description = "SEC form type, expected to be 4 for this project"),
  filing_date DATE NOT NULL OPTIONS(description = "Date the filing was accepted by SEC EDGAR"),
  report_date DATE OPTIONS(description = "Issuer report period when provided by SEC EDGAR"),
  feed_primary_document STRING OPTIONS(description = "Primary document value from the submissions feed; may include SEC viewer prefixes such as xslF345X06/"),
  raw_primary_document STRING OPTIONS(description = "Raw document filename used inside the accession archive directory"),
  sec_viewer_url STRING OPTIONS(description = "Rendered SEC viewer URL when the feed provides an xslF345X06 path"),
  sec_archive_url STRING OPTIONS(description = "Raw SEC archive URL for XML ingestion"),
  raw_gcs_path STRING NOT NULL OPTIONS(description = "GCS URI for the raw XML document"),
  metadata_gcs_path STRING OPTIONS(description = "GCS URI for the metadata sidecar JSON"),
  content_sha256 STRING OPTIONS(description = "Hash of raw XML content for change detection"),
  ingestion_run_id STRING OPTIONS(description = "Pipeline run id that fetched this document"),
  fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL
)
PARTITION BY filing_date
CLUSTER BY ticker, cik, accession_number
OPTIONS (
  description = "Raw SEC filing document inventory and lineage metadata",
  require_partition_filter = false
);

CREATE TABLE IF NOT EXISTS `sec_insider.filings_raw` (
  transaction_id STRING NOT NULL OPTIONS(description = "Stable transaction-level id derived from accession number and transaction attributes"),
  accession_number STRING NOT NULL OPTIONS(description = "SEC filing accession number"),
  ticker STRING NOT NULL OPTIONS(description = "Issuer ticker"),
  cik STRING NOT NULL OPTIONS(description = "Issuer CIK"),
  company_name STRING OPTIONS(description = "Issuer legal name from Form 4"),
  insider_name STRING OPTIONS(description = "Reporting owner name"),
  insider_cik STRING OPTIONS(description = "Reporting owner CIK when available"),
  insider_role STRING OPTIONS(description = "Normalized role: officer, director, 10pct_owner, other, or unknown"),
  officer_title STRING OPTIONS(description = "Officer title from Form 4 when provided"),
  is_director BOOL OPTIONS(description = "Whether reporting owner is marked as director"),
  is_officer BOOL OPTIONS(description = "Whether reporting owner is marked as officer"),
  is_ten_percent_owner BOOL OPTIONS(description = "Whether reporting owner is marked as ten percent owner"),
  transaction_date DATE OPTIONS(description = "Transaction date from Form 4"),
  filing_date DATE NOT NULL OPTIONS(description = "Date the Form 4 was filed"),
  security_title STRING OPTIONS(description = "Security title reported for the transaction"),
  transaction_code STRING OPTIONS(description = "SEC transaction code, such as P, S, A, or D"),
  transaction_acquired_disposed STRING OPTIONS(description = "A for acquired, D for disposed, when provided"),
  shares FLOAT64 OPTIONS(description = "Transaction shares exactly as reported on Form 4"),
  share_delta FLOAT64 OPTIONS(description = "Signed transaction share movement; acquisitions positive and disposals negative"),
  price_per_share NUMERIC OPTIONS(description = "Reported price per share"),
  total_value_usd NUMERIC OPTIONS(description = "Absolute transaction value in USD"),
  post_trade_shares FLOAT64 OPTIONS(description = "Shares owned following the reported transaction"),
  ownership_nature STRING OPTIONS(description = "Direct or indirect ownership description"),
  is_derivative BOOL NOT NULL OPTIONS(description = "Whether the row came from derivativeTransaction"),
  raw_gcs_path STRING NOT NULL OPTIONS(description = "GCS URI for the source XML document"),
  source_system STRING DEFAULT "sec_edgar" NOT NULL,
  parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
  ingestion_run_id STRING OPTIONS(description = "Pipeline run id that parsed this row")
)
PARTITION BY filing_date
CLUSTER BY ticker, transaction_code, insider_role, accession_number
OPTIONS (
  description = "Parsed Form 4 transaction rows from SEC EDGAR",
  require_partition_filter = false
);

CREATE TABLE IF NOT EXISTS `sec_insider.prices_enriched` (
  ticker STRING NOT NULL OPTIONS(description = "Ticker symbol"),
  price_date DATE NOT NULL OPTIONS(description = "Market date for the price row"),
  open NUMERIC OPTIONS(description = "Open price"),
  high NUMERIC OPTIONS(description = "High price"),
  low NUMERIC OPTIONS(description = "Low price"),
  close NUMERIC OPTIONS(description = "Close or latest price"),
  prev_close NUMERIC OPTIONS(description = "Previous close price"),
  volume INT64 OPTIONS(description = "Trading volume when available"),
  pct_change FLOAT64 OPTIONS(description = "Percent change versus previous close"),
  news_count_7d INT64 OPTIONS(description = "Count of vendor news articles found for the ticker in the trailing seven days"),
  data_vendor STRING DEFAULT "finnhub" NOT NULL,
  fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
  ingestion_run_id STRING OPTIONS(description = "Pipeline run id that loaded this row")
)
PARTITION BY price_date
CLUSTER BY ticker
OPTIONS (
  description = "Price observations used to enrich insider transactions",
  require_partition_filter = false
);

CREATE TABLE IF NOT EXISTS `sec_insider.company_news_raw` (
  news_id STRING NOT NULL OPTIONS(description = "Stable id derived from ticker, vendor article id, URL, or headline hash"),
  ticker STRING NOT NULL OPTIONS(description = "Ticker symbol"),
  published_at TIMESTAMP OPTIONS(description = "Article publication timestamp"),
  news_date DATE OPTIONS(description = "Article publication date used for partitioning"),
  source STRING OPTIONS(description = "News source returned by the vendor"),
  category STRING OPTIONS(description = "Vendor article category"),
  headline STRING OPTIONS(description = "Article headline"),
  summary STRING OPTIONS(description = "Article summary or snippet"),
  url STRING OPTIONS(description = "Canonical article URL"),
  image_url STRING OPTIONS(description = "Article image URL when returned by the vendor"),
  related STRING OPTIONS(description = "Vendor-provided related tickers or symbols"),
  data_vendor STRING DEFAULT "finnhub" NOT NULL,
  fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
  ingestion_run_id STRING OPTIONS(description = "Pipeline run id that loaded this row")
)
PARTITION BY news_date
CLUSTER BY ticker, source
OPTIONS (
  description = "Raw company news from Finnhub used as context around insider trades",
  require_partition_filter = false
);

CREATE TABLE IF NOT EXISTS `sec_insider.macro_context` (
  macro_date DATE NOT NULL OPTIONS(description = "Observation date"),
  sp500 NUMERIC OPTIONS(description = "S&P 500 index level"),
  vix NUMERIC OPTIONS(description = "CBOE volatility index level"),
  fed_funds_rate NUMERIC OPTIONS(description = "Federal funds rate"),
  treasury_10y NUMERIC OPTIONS(description = "10-year Treasury yield"),
  data_vendor STRING DEFAULT "fred" NOT NULL,
  fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
  ingestion_run_id STRING OPTIONS(description = "Pipeline run id that loaded this row")
)
PARTITION BY macro_date
OPTIONS (
  description = "Daily macro indicators used as scoring context",
  require_partition_filter = false
);

CREATE TABLE IF NOT EXISTS `sec_insider.insider_alerts` (
  alert_id STRING NOT NULL OPTIONS(description = "Stable alert id derived from transaction id and scoring version"),
  transaction_id STRING OPTIONS(description = "Source transaction id from filings_raw"),
  accession_number STRING OPTIONS(description = "SEC filing accession number"),
  ticker STRING NOT NULL OPTIONS(description = "Issuer ticker"),
  insider_name STRING OPTIONS(description = "Reporting owner name"),
  insider_role STRING OPTIONS(description = "Normalized insider role"),
  transaction_date DATE OPTIONS(description = "Transaction date"),
  filing_date DATE OPTIONS(description = "Filing date"),
  transaction_code STRING OPTIONS(description = "SEC transaction code"),
  total_value_usd NUMERIC OPTIONS(description = "Absolute transaction value in USD"),
  trade_close NUMERIC OPTIONS(description = "Price on or near transaction date"),
  price_7d_after NUMERIC OPTIONS(description = "Price seven calendar days after transaction when available"),
  price_change_7d_pct FLOAT64 OPTIONS(description = "Seven-day price move after the trade"),
  vix_on_trade_date NUMERIC OPTIONS(description = "VIX level on or near transaction date"),
  news_count_7d INT64 OPTIONS(description = "Count of relevant company news articles near the trade"),
  top_news_headline STRING OPTIONS(description = "Most relevant or most recent headline near the trade"),
  top_news_url STRING OPTIONS(description = "URL for the headline stored in top_news_headline"),
  suspicion_score FLOAT64 NOT NULL OPTIONS(description = "Suspicion score from 0.0 to 1.0"),
  scoring_version STRING NOT NULL OPTIONS(description = "Version label for the scoring logic"),
  flag_reason STRING OPTIONS(description = "Human-readable explanation of the score"),
  alerted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
  ingestion_run_id STRING OPTIONS(description = "Pipeline run id that generated this alert")
)
PARTITION BY transaction_date
CLUSTER BY ticker, suspicion_score, transaction_code
OPTIONS (
  description = "Scored insider-trading alerts generated from enriched filings",
  require_partition_filter = false
);

CREATE TABLE IF NOT EXISTS `sec_insider.weekly_summary` (
  week_start_date DATE NOT NULL OPTIONS(description = "Monday week start date"),
  ticker STRING NOT NULL OPTIONS(description = "Issuer ticker"),
  filing_count INT64 NOT NULL OPTIONS(description = "Number of Form 4 filings in the week"),
  transaction_count INT64 NOT NULL OPTIONS(description = "Number of transaction rows in the week"),
  buy_count INT64 NOT NULL OPTIONS(description = "Count of purchase transactions"),
  sell_count INT64 NOT NULL OPTIONS(description = "Count of sale transactions"),
  total_buy_value_usd NUMERIC OPTIONS(description = "Total reported purchase value"),
  total_sell_value_usd NUMERIC OPTIONS(description = "Total reported sale value"),
  alert_count INT64 NOT NULL OPTIONS(description = "Number of alerts generated for the week"),
  max_suspicion_score FLOAT64 OPTIONS(description = "Highest alert score in the week"),
  generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL
)
PARTITION BY week_start_date
CLUSTER BY ticker
OPTIONS (
  description = "Gold-layer weekly aggregate for reports and dashboard trends",
  require_partition_filter = false
);

CREATE TABLE IF NOT EXISTS `sec_insider.pipeline_runs` (
  run_id STRING NOT NULL OPTIONS(description = "Unique id for one pipeline run"),
  pipeline_name STRING NOT NULL OPTIONS(description = "Pipeline or DAG name"),
  task_name STRING OPTIONS(description = "Task name when applicable"),
  status STRING NOT NULL OPTIONS(description = "started, succeeded, failed, or skipped"),
  started_at TIMESTAMP NOT NULL,
  ended_at TIMESTAMP,
  records_read INT64,
  records_written INT64,
  error_message STRING,
  metadata JSON
)
PARTITION BY DATE(started_at)
CLUSTER BY pipeline_name, status
OPTIONS (
  description = "Operational audit log for ingestion, transforms, and exports",
  require_partition_filter = false
);
