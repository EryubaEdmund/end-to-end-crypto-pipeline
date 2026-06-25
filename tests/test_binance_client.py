from datetime import datetime, timezone

import pytest

from src.ingestion.binance_client import (
    BinanceAPIError,
    fetch_klines,
    fetch_ticker_prices,
    parse_klines_payload,
    parse_ticker_payload,
    validate_klines_payload,
    validate_ticker_payload,
)

SAMPLE_TICKER_PAYLOAD = [
    {"symbol": "BTCUSDT", "lastPrice": "65000.50", "volume": "1234.5"},
    {"symbol": "ETHUSDT", "lastPrice": "3400.10", "volume": "9999.0"},
]

SAMPLE_KLINES_PAYLOAD = [
    [1718841600000, "65000.0", "65500.0", "64800.0", "65200.0", "12.5",
     1718841659999, "813000.0", 100, "6.0", "390600.0", "0"],
]


def test_parse_ticker_payload_happy_path():
    rows = parse_ticker_payload(SAMPLE_TICKER_PAYLOAD)
    assert len(rows) == 2
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["price"] == pytest.approx(65000.50)
    assert rows[0]["volume"] == pytest.approx(1234.5)
    assert rows[0]["exchange"] == "binance"
    assert isinstance(rows[0]["event_time"], datetime)


def test_validate_ticker_payload_rejects_error_response():
    error_payload = {"code": -1121, "msg": "Invalid symbol."}
    with pytest.raises(BinanceAPIError, match="Invalid symbol"):
        validate_ticker_payload(error_payload)


def test_validate_ticker_payload_rejects_malformed_entry():
    with pytest.raises(BinanceAPIError):
        validate_ticker_payload([{"symbol": "BTCUSDT"}])  # missing lastPrice


def test_parse_klines_payload_happy_path():
    rows = parse_klines_payload(SAMPLE_KLINES_PAYLOAD, symbol="BTCUSDT", interval="1m")
    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "BTCUSDT"
    assert row["open"] == pytest.approx(65000.0)
    assert row["high"] == pytest.approx(65500.0)
    assert row["volume"] == pytest.approx(12.5)
    assert row["open_time"] == datetime.fromtimestamp(1718841600000 / 1000, tz=timezone.utc)


def test_validate_klines_payload_rejects_short_rows():
    with pytest.raises(BinanceAPIError):
        validate_klines_payload([[1, 2, 3]])  # too few fields


def test_fetch_ticker_prices_filters_to_requested_symbols(requests_mock):
    requests_mock.get(
        "https://api.binance.com/api/v3/ticker/24hr",
        json=SAMPLE_TICKER_PAYLOAD,
    )
    rows = fetch_ticker_prices(["BTCUSDT"])
    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTCUSDT"


def test_fetch_klines_calls_expected_endpoint(requests_mock):
    requests_mock.get(
        "https://api.binance.com/api/v3/klines",
        json=SAMPLE_KLINES_PAYLOAD,
    )
    rows = fetch_klines("BTCUSDT", interval="1m", limit=1)
    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTCUSDT"


def test_fetch_ticker_prices_raises_on_api_error(requests_mock):
    requests_mock.get(
        "https://api.binance.com/api/v3/ticker/24hr",
        json={"code": -1121, "msg": "Invalid symbol."},
    )
    with pytest.raises(BinanceAPIError):
        fetch_ticker_prices(["BTCUSDT"])
