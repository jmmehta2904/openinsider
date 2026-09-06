from transforms.parse_form4 import parse_form4_xml


SAMPLE_FORM4_XML = """<?xml version="1.0"?>
<ownershipDocument>
  <schemaVersion>X0609</schemaVersion>
  <documentType>4</documentType>
  <periodOfReport>2026-09-01</periodOfReport>
  <issuer>
    <issuerCik>0000789019</issuerCik>
    <issuerName>MICROSOFT CORP</issuerName>
    <issuerTradingSymbol>MSFT</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001214156</rptOwnerCik>
      <rptOwnerName>Nadella Satya</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>1</isDirector>
      <isOfficer>1</isOfficer>
      <isTenPercentOwner>0</isTenPercentOwner>
      <officerTitle>Chairman and CEO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-09-01</value></transactionDate>
      <transactionCoding>
        <transactionCode>S</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1040</value></transactionShares>
        <transactionPricePerShare><value>498.2396</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>572247.534</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-09-01</value></transactionDate>
      <transactionCoding>
        <transactionCode>A</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>100</value></transactionShares>
        <transactionPricePerShare><value>0</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>572347.534</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
  <derivativeTable>
    <derivativeTransaction>
      <securityTitle><value>Stock Option</value></securityTitle>
      <transactionDate><value>2026-09-01</value></transactionDate>
      <transactionCoding>
        <transactionCode>M</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>50</value></transactionShares>
        <transactionPricePerShare><value>10</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>50</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature>
        <natureOfOwnership><value>Indirect By Trust</value></natureOfOwnership>
      </ownershipNature>
    </derivativeTransaction>
  </derivativeTable>
</ownershipDocument>
"""


def test_parse_form4_returns_one_row_per_transaction():
    rows = parse_form4_xml(
        SAMPLE_FORM4_XML,
        ticker="MSFT",
        cik="0000789019",
        accession_number="0000789019-26-000161",
        filing_date="2026-09-02",
        raw_gcs_path="gs://bucket/filings/2026-09-02/MSFT/form4.xml",
        ingestion_run_id="test-run",
    )

    assert len(rows) == 3
    assert len({row["transaction_id"] for row in rows}) == 3


def test_parse_form4_maps_identity_role_and_sale_amounts():
    row = parse_form4_xml(
        SAMPLE_FORM4_XML,
        ticker="MSFT",
        cik="0000789019",
        accession_number="0000789019-26-000161",
        filing_date="2026-09-02",
        raw_gcs_path="gs://bucket/filings/2026-09-02/MSFT/form4.xml",
    )[0]

    assert row["company_name"] == "MICROSOFT CORP"
    assert row["insider_name"] == "Nadella Satya"
    assert row["insider_cik"] == "0001214156"
    assert row["insider_role"] == "officer+director"
    assert row["officer_title"] == "Chairman and CEO"
    assert row["is_officer"] is True
    assert row["is_director"] is True
    assert row["is_ten_percent_owner"] is False
    assert row["transaction_code"] == "S"
    assert row["shares"] == 1040.0
    assert row["share_delta"] == -1040.0
    assert row["price_per_share"] == 498.2396
    assert row["total_value_usd"] == 518169.184
    assert row["post_trade_shares"] == 572247.534
    assert row["is_derivative"] is False


def test_parse_form4_preserves_derivative_rows():
    rows = parse_form4_xml(
        SAMPLE_FORM4_XML,
        ticker="MSFT",
        cik="0000789019",
        accession_number="0000789019-26-000161",
        filing_date="2026-09-02",
        raw_gcs_path="gs://bucket/filings/2026-09-02/MSFT/form4.xml",
    )

    derivative = rows[-1]
    assert derivative["is_derivative"] is True
    assert derivative["security_title"] == "Stock Option"
    assert derivative["transaction_code"] == "M"
    assert derivative["share_delta"] == 50.0
    assert derivative["ownership_nature"] == "Indirect By Trust"
