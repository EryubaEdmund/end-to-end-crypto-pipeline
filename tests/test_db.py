from datetime import datetime, timezone

from src.ingestion import db

NOW = datetime(2026, 6, 20, 12, 0, tzinfo=timezone.utc)


def test_build_insert_prices_shapes_values_correctly():
    rows = [{"symbol": "BTCUSDT", "price": 65000.5, "volume": 12.3, "exchange": "binance", "event_time": NOW}]
    sql, values = db.build_insert_prices(rows)
    assert "INSERT INTO prices" in sql
    assert values == [("BTCUSDT", 65000.5, 12.3, "binance", NOW)]


def test_build_insert_prices_defaults_volume_when_absent():
    rows = [{"symbol": "BTCUSDT", "price": 1.0, "exchange": "binance", "event_time": NOW}]
    _, values = db.build_insert_prices(rows)
    assert values[0][2] == 0


def test_build_insert_trades_shapes_values_correctly():
    rows = [
        {
            "symbol": "BTCUSDT", "open_time": NOW, "close_time": NOW,
            "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10,
            "interval": "1m", "exchange": "binance",
        }
    ]
    sql, values = db.build_insert_trades(rows)
    assert "ON CONFLICT" in sql
    assert values[0][0] == "BTCUSDT"
    assert values[0][7] == 10  # volume position


def test_build_insert_log_includes_error_message_when_provided():
    sql, values = db.build_insert_log("binance", "ingest_ticker", "failure", 0, NOW, "boom")
    assert values == ("binance", "ingest_ticker", "failure", 0, "boom", NOW)


def test_build_upsert_symbol_defaults_exchange_to_binance():
    sql, values = db.build_upsert_symbol("BTCUSDT", "BTC", "USDT")
    assert "ON CONFLICT (symbol) DO NOTHING" in sql
    assert values == ("BTCUSDT", "BTC", "USDT", "binance")


def test_get_connection_params_reads_env(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "myhost")
    monkeypatch.setenv("POSTGRES_DB", "mydb")
    params = db.get_connection_params()
    assert params["host"] == "myhost"
    assert params["dbname"] == "mydb"
