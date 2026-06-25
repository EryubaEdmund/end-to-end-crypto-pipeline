import pytest

from src.ingestion.kraken_client import KrakenAPIError, parse_ticker_payload

SAMPLE_KRAKEN_PAYLOAD = {
    "error": [],
    "result": {
        "XXBTZUSD": {"c": ["65000.10", "0.01"], "v": ["500.0", "1200.0"]},
        "XETHZUSD": {"c": ["3400.50", "0.5"], "v": ["1000.0", "3000.0"]},
    },
}


def test_parse_ticker_payload_happy_path():
    rows = parse_ticker_payload(SAMPLE_KRAKEN_PAYLOAD)
    assert len(rows) == 2
    by_symbol = {r["symbol"]: r for r in rows}
    assert by_symbol["XXBTZUSD"]["price"] == pytest.approx(65000.10)
    assert by_symbol["XXBTZUSD"]["volume"] == pytest.approx(1200.0)
    assert by_symbol["XXBTZUSD"]["exchange"] == "kraken"


def test_parse_ticker_payload_raises_on_error():
    with pytest.raises(KrakenAPIError):
        parse_ticker_payload({"error": ["EQuery:Unknown asset pair"], "result": {}})


def test_parse_ticker_payload_handles_missing_volume_gracefully():
    payload = {"error": [], "result": {"XXBTZUSD": {"c": ["65000.10", "0.01"]}}}
    rows = parse_ticker_payload(payload)
    assert rows[0]["volume"] == 0.0
