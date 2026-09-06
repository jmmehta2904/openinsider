"""SEC EDGAR company watchlist.

The pipeline tracks a deliberately small, high-signal universe of companies.
CIKs are stored as zero-padded strings because SEC EDGAR submission endpoints
expect `CIK{cik}.json`, where `cik` is ten digits.
"""

from __future__ import annotations


WATCHLIST: dict[str, str] = {
    "AAPL": "0000320193",
    "MSFT": "0000789019",
    "NVDA": "0001045810",
    "TSLA": "0001318605",
    "GOOGL": "0001652044",
    "AMZN": "0001018724",
    "META": "0001326801",
    "JPM": "0000019617",
    "GS": "0000886982",
    "NFLX": "0001065280",
    "BRK.B": "0001067983",
    "V": "0001403161",
    "MA": "0001141391",
    "UNH": "0000731766",
    "HD": "0000354950",
    "PG": "0000080424",
    "JNJ": "0000200406",
    "ABBV": "0001551152",
    "MRK": "0000310158",
    "PFE": "0000078003",
    "KO": "0000021344",
    "PEP": "0000077476",
    "WMT": "0000104169",
    "COST": "0000909832",
    "TGT": "0000027419",
    "BAC": "0000070858",
    "WFC": "0000072971",
    "C": "0000831001",
    "MS": "0000895421",
    "BLK": "0001364742",
    "ORCL": "0001341439",
    "CRM": "0001108524",
    "ADBE": "0000796343",
    "AMD": "0000002488",
    "INTC": "0000050863",
    "QCOM": "0000804328",
    "TXN": "0000097476",
    "NOW": "0001373715",
    "SNOW": "0001640147",
    "UBER": "0001543151",
    "LYFT": "0001759509",
    "SHOP": "0001594805",
    "XYZ": "0001512673",
    "PYPL": "0001633917",
    "COIN": "0001679788",
    "SPOT": "0001639920",
    "DIS": "0001744489",
    "CMCSA": "0001166691",
    "T": "0000732717",
    "NKE": "0000320187",
}


WATCHLIST_TICKERS: tuple[str, ...] = tuple(WATCHLIST.keys())
WATCHLIST_CIKS: tuple[str, ...] = tuple(WATCHLIST.values())
