from datetime import datetime, timezone

from streaming.producer import build_message, build_messages

NOW = datetime(2026, 6, 20, 12, 0, tzinfo=timezone.utc)


def test_build_message_serializes_datetime_to_isoformat():
    row = {"symbol": "BTCUSDT", "price": 65000.5, "volume": 1.0, "exchange": "binance", "event_time": NOW}
    msg = build_message(row)
    assert msg["event_time"] == NOW.isoformat()
    assert msg["symbol"] == "BTCUSDT"
    assert isinstance(msg["price"], float)


def test_build_message_defaults_exchange_and_volume():
    row = {"symbol": "BTCUSDT", "price": 1.0, "event_time": NOW}
    msg = build_message(row)
    assert msg["exchange"] == "binance"
    assert msg["volume"] == 0.0


def test_build_messages_handles_a_batch():
    rows = [
        {"symbol": "BTCUSDT", "price": 1.0, "event_time": NOW},
        {"symbol": "ETHUSDT", "price": 2.0, "event_time": NOW},
    ]
    messages = build_messages(rows)
    assert [m["symbol"] for m in messages] == ["BTCUSDT", "ETHUSDT"]
