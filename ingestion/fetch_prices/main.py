"""Cloud Function: fetch Finnhub quote and company-news context into GCS."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone, timedelta
from typing import Any

import functions_framework
import requests
from google.cloud import storage

from config.settings import FINNHUB_BASE_URL, FINNHUB_KEY, GCS_BUCKET, require_setting
from config.watchlist import WATCHLIST


def _json_response(payload: dict[str, Any], status: int = 200) -> tuple[str, int, dict[str, str]]:
    return json.dumps(payload, default=str), status, {"Content-Type": "application/json"}


def _get(url: str, params: dict[str, Any]) -> Any:
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def _article_id(ticker: str, article: dict[str, Any]) -> str:
    stable_value = article.get("id") or article.get("url") or article.get("headline") or ""
    return hashlib.sha256(f"{ticker}:{stable_value}".encode("utf-8")).hexdigest()


def _unix_to_iso_timestamp(value: int | None) -> str | None:
    if not value:
        return None
    return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()


@functions_framework.http
def fetch_prices(request):
    """Fetch quotes and recent company news for requested watchlist tickers."""
    try:
        bucket_name = require_setting("GCS_BUCKET", GCS_BUCKET)
        finnhub_key = require_setting("FINNHUB_KEY", FINNHUB_KEY)
        body = request.get_json(silent=True) or {}

        tickers = body.get("tickers") or []
        if not tickers:
            return _json_response({"status": "error", "error": "No tickers provided"}, 400)

        tickers = _validate_tickers(tickers)
        as_of_date = body.get("as_of_date") or date.today().isoformat()
        news_days_back = int(body.get("news_days_back", 7))
        news_from = body.get("news_from") or (
            date.fromisoformat(as_of_date) - timedelta(days=news_days_back)
        ).isoformat()
        news_to = body.get("news_to") or as_of_date

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        quote_results: list[dict[str, Any]] = []
        news_results: list[dict[str, Any]] = []
        failures: list[dict[str, str]] = []

        for ticker in tickers:
            try:
                quote = _get(
                    f"{FINNHUB_BASE_URL}/quote",
                    {"symbol": ticker, "token": finnhub_key},
                )
                news = _get(
                    f"{FINNHUB_BASE_URL}/company-news",
                    {
                        "symbol": ticker,
                        "from": news_from,
                        "to": news_to,
                        "token": finnhub_key,
                    },
                )
                if not isinstance(news, list):
                    news = []

                quote_payload = {
                    "ticker": ticker,
                    "price_date": as_of_date,
                    "open": quote.get("o"),
                    "high": quote.get("h"),
                    "low": quote.get("l"),
                    "close": quote.get("c"),
                    "prev_close": quote.get("pc"),
                    "volume": None,
                    "pct_change": _pct_change(quote.get("c"), quote.get("pc")),
                    "news_count_7d": len(news),
                    "data_vendor": "finnhub",
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                }
                quote_path = f"prices/{as_of_date}/{ticker}.json"
                bucket.blob(quote_path).upload_from_string(
                    json.dumps(quote_payload, indent=2, sort_keys=True),
                    content_type="application/json",
                )
                quote_results.append({"ticker": ticker, "gcs_path": f"gs://{bucket_name}/{quote_path}"})

                normalized_articles = [_normalize_article(ticker, article) for article in news]
                news_payload = {
                    "ticker": ticker,
                    "from": news_from,
                    "to": news_to,
                    "article_count": len(normalized_articles),
                    "articles": normalized_articles,
                    "data_vendor": "finnhub",
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                }
                news_path = f"news/{as_of_date}/{ticker}.json"
                bucket.blob(news_path).upload_from_string(
                    json.dumps(news_payload, indent=2, sort_keys=True),
                    content_type="application/json",
                )
                news_results.append(
                    {
                        "ticker": ticker,
                        "article_count": len(normalized_articles),
                        "gcs_path": f"gs://{bucket_name}/{news_path}",
                    }
                )
            except Exception as exc:
                failures.append({"ticker": ticker, "error": str(exc)})

        return _json_response(
            {
                "status": "ok",
                "price_files": quote_results,
                "news_files": news_results,
                "failures": failures,
            }
        )
    except Exception as exc:
        return _json_response({"status": "error", "error": str(exc)}, 500)


def _validate_tickers(tickers: list[str] | str) -> list[str]:
    if isinstance(tickers, str):
        tickers = [ticker.strip() for ticker in tickers.split(",")]

    normalized = [ticker.upper() for ticker in tickers]
    unknown = sorted(set(normalized) - set(WATCHLIST))
    if unknown:
        raise ValueError(f"Unknown watchlist ticker(s): {', '.join(unknown)}")
    return sorted(set(normalized))


def _pct_change(current: float | int | None, previous_close: float | int | None) -> float | None:
    if current is None or previous_close in (None, 0):
        return None
    return round(((float(current) - float(previous_close)) / float(previous_close)) * 100, 4)


def _normalize_article(ticker: str, article: dict[str, Any]) -> dict[str, Any]:
    return {
        "news_id": _article_id(ticker, article),
        "ticker": ticker,
        "published_at": _unix_to_iso_timestamp(article.get("datetime")),
        "source": article.get("source"),
        "category": article.get("category"),
        "headline": article.get("headline"),
        "summary": article.get("summary"),
        "url": article.get("url"),
        "image_url": article.get("image"),
        "related": article.get("related"),
    }
