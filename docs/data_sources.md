# Data Sources

## SEC EDGAR

The pipeline uses the SEC submissions endpoint to discover recent Form 4 filings:

```text
https://data.sec.gov/submissions/CIK{ten_digit_cik}.json
```

The `primaryDocument` field can point to a rendered SEC viewer path such as
`xslF345X06/form4.xml`. For ingestion, store both the feed path and the raw
archive document path. The raw XML URL should use the accession directory and
the document basename:

```text
https://www.sec.gov/Archives/edgar/data/{cik_without_leading_zeroes}/{accession_without_dashes}/{document_basename}
```

## Finnhub

Finnhub is used for market context around companies with insider filings:

- Quote endpoint for current price fields.
- Company-news endpoint for article details around the trade window.

News articles are kept in `company_news_raw` so the dashboard and alert scoring
can show headlines instead of only a news count.

## FRED

FRED is used for macro context:

- `SP500`
- `VIXCLS`
- `FEDFUNDS`
- `DGS10`

These observations land in `macro_context` and are joined to transactions by
date during scoring.
