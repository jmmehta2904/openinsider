"""Parse SEC Form 4 XML into transaction rows matching the BigQuery schema."""

from __future__ import annotations

import hashlib
import xml.etree.ElementTree as ET
from typing import Any, Iterable, Optional


TRANSACTION_CONTAINER_TAGS = {
    "nonDerivativeTransaction",
    "derivativeTransaction",
}


def parse_form4_xml(
    xml_text: str,
    ticker: str,
    cik: str,
    accession_number: str,
    filing_date: str,
    raw_gcs_path: str,
    ingestion_run_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Parse a Form 4 XML document into one row per transaction.

    The parser preserves every transaction code in the raw layer. Scoring can
    later decide which codes matter instead of throwing away information early.
    """
    root = ET.fromstring(xml_text)
    issuer = _first_descendant(root, "issuer")
    owner = _first_descendant(root, "reportingOwner")
    owner_id = _first_child(owner, "reportingOwnerId")
    relationship = _first_child(owner, "reportingOwnerRelationship")

    company_name = _text_at(issuer, ["issuerName"])
    insider_name = _text_at(owner_id, ["rptOwnerName"])
    insider_cik = _text_at(owner_id, ["rptOwnerCik"])
    role_flags = _role_flags(relationship)
    insider_role = _derive_role(role_flags)
    officer_title = _text_at(relationship, ["officerTitle"])

    rows: list[dict[str, Any]] = []
    for table_name, is_derivative in (
        ("nonDerivativeTransaction", False),
        ("derivativeTransaction", True),
    ):
        transactions = _descendants(root, table_name)
        for index, transaction in enumerate(transactions, start=1):
            row = _build_transaction_row(
                transaction=transaction,
                transaction_index=index,
                transaction_table=table_name,
                is_derivative=is_derivative,
                ticker=ticker,
                cik=cik,
                company_name=company_name,
                insider_name=insider_name,
                insider_cik=insider_cik,
                insider_role=insider_role,
                officer_title=officer_title,
                role_flags=role_flags,
                accession_number=accession_number,
                filing_date=filing_date,
                raw_gcs_path=raw_gcs_path,
                ingestion_run_id=ingestion_run_id,
            )
            if row is not None:
                rows.append(row)

    return rows


def _build_transaction_row(
    transaction: ET.Element,
    transaction_index: int,
    transaction_table: str,
    is_derivative: bool,
    ticker: str,
    cik: str,
    company_name: Optional[str],
    insider_name: Optional[str],
    insider_cik: Optional[str],
    insider_role: str,
    officer_title: Optional[str],
    role_flags: dict[str, bool],
    accession_number: str,
    filing_date: str,
    raw_gcs_path: str,
    ingestion_run_id: Optional[str],
) -> Optional[dict[str, Any]]:
    transaction_date = _text_at(transaction, ["transactionDate", "value"]) or filing_date
    transaction_code = _text_at(transaction, ["transactionCoding", "transactionCode"])
    acquired_disposed = _text_at(
        transaction,
        ["transactionAmounts", "transactionAcquiredDisposedCode", "value"],
    )
    shares = _to_float(_text_at(transaction, ["transactionAmounts", "transactionShares", "value"]))
    price_per_share = _to_float(
        _text_at(transaction, ["transactionAmounts", "transactionPricePerShare", "value"])
    )
    post_trade_shares = _to_float(
        _text_at(
            transaction,
            ["postTransactionAmounts", "sharesOwnedFollowingTransaction", "value"],
        )
    )
    security_title = _text_at(transaction, ["securityTitle", "value"])
    ownership_nature = (
        _text_at(transaction, ["ownershipNature", "directOrIndirectOwnership", "value"])
        or _text_at(transaction, ["ownershipNature", "natureOfOwnership", "value"])
    )

    if not transaction_code and shares is None and price_per_share is None:
        return None

    share_delta = _signed_share_delta(shares, acquired_disposed, transaction_code)
    total_value_usd = None
    if shares is not None and price_per_share is not None:
        total_value_usd = abs(shares) * price_per_share

    transaction_id = _transaction_id(
        accession_number=accession_number,
        transaction_table=transaction_table,
        transaction_index=transaction_index,
        transaction_date=transaction_date,
        transaction_code=transaction_code,
        security_title=security_title,
        acquired_disposed=acquired_disposed,
        shares=shares,
        price_per_share=price_per_share,
    )

    return {
        "transaction_id": transaction_id,
        "accession_number": accession_number,
        "ticker": ticker,
        "cik": cik,
        "company_name": company_name,
        "insider_name": insider_name,
        "insider_cik": insider_cik,
        "insider_role": insider_role,
        "officer_title": officer_title,
        "is_director": role_flags["is_director"],
        "is_officer": role_flags["is_officer"],
        "is_ten_percent_owner": role_flags["is_ten_percent_owner"],
        "transaction_date": transaction_date,
        "filing_date": filing_date,
        "security_title": security_title,
        "transaction_code": transaction_code,
        "transaction_acquired_disposed": acquired_disposed,
        "shares": shares,
        "share_delta": share_delta,
        "price_per_share": price_per_share,
        "total_value_usd": total_value_usd,
        "post_trade_shares": post_trade_shares,
        "ownership_nature": ownership_nature,
        "is_derivative": is_derivative,
        "raw_gcs_path": raw_gcs_path,
        "source_system": "sec_edgar",
        "ingestion_run_id": ingestion_run_id,
    }


def _transaction_id(
    accession_number: str,
    transaction_table: str,
    transaction_index: int,
    transaction_date: str,
    transaction_code: Optional[str],
    security_title: Optional[str],
    acquired_disposed: Optional[str],
    shares: Optional[float],
    price_per_share: Optional[float],
) -> str:
    value = "|".join(
        [
            accession_number,
            transaction_table,
            str(transaction_index),
            transaction_date or "",
            transaction_code or "",
            security_title or "",
            acquired_disposed or "",
            "" if shares is None else str(shares),
            "" if price_per_share is None else str(price_per_share),
        ]
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _role_flags(relationship: Optional[ET.Element]) -> dict[str, bool]:
    return {
        "is_director": _text_at(relationship, ["isDirector"]) == "1",
        "is_officer": _text_at(relationship, ["isOfficer"]) == "1",
        "is_ten_percent_owner": _text_at(relationship, ["isTenPercentOwner"]) == "1",
    }


def _derive_role(flags: dict[str, bool]) -> str:
    roles: list[str] = []
    if flags["is_officer"]:
        roles.append("officer")
    if flags["is_director"]:
        roles.append("director")
    if flags["is_ten_percent_owner"]:
        roles.append("10pct_owner")
    return "+".join(roles) if roles else "other"


def _signed_share_delta(
    shares: Optional[float],
    acquired_disposed: Optional[str],
    transaction_code: Optional[str],
) -> Optional[float]:
    if shares is None:
        return None
    if acquired_disposed == "A":
        return abs(shares)
    if acquired_disposed == "D":
        return -abs(shares)
    if transaction_code in {"S", "D", "G"}:
        return -abs(shares)
    if transaction_code in {"P", "A", "M"}:
        return abs(shares)
    return shares


def _to_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    normalized = value.replace(",", "").strip()
    if normalized in {"", "N/A", "n/a", "-"}:
        return None
    return float(normalized)


def _text_at(node: Optional[ET.Element], path: list[str]) -> Optional[str]:
    current = node
    for tag in path:
        current = _first_child(current, tag)
        if current is None:
            return None
    if current.text is None:
        return None
    text = current.text.strip()
    return text if text else None


def _first_child(node: Optional[ET.Element], tag: str) -> Optional[ET.Element]:
    if node is None:
        return None
    for child in list(node):
        if _local_name(child.tag) == tag:
            return child
    return None


def _first_descendant(node: ET.Element, tag: str) -> Optional[ET.Element]:
    for descendant in _descendants(node, tag):
        return descendant
    return None


def _descendants(node: ET.Element, tag: str) -> Iterable[ET.Element]:
    for descendant in node.iter():
        if _local_name(descendant.tag) == tag:
            yield descendant


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]
