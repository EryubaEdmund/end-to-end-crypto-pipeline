import json
from datetime import datetime, timezone

import pytest
from pyspark.sql import Row, SparkSession

from spark_jobs.stream_processing import compute_window_metrics, parse_kafka_value


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("stream-processing-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_parse_kafka_value_extracts_typed_columns(spark):
    message = {
        "symbol": "BTCUSDT",
        "price": 65000.5,
        "volume": 12.3,
        "exchange": "binance",
        "event_time": "2026-06-20T12:00:00",
    }
    raw_df = spark.createDataFrame([Row(value=json.dumps(message).encode("utf-8"))])
    parsed = parse_kafka_value(raw_df)
    row = parsed.collect()[0]
    assert row["symbol"] == "BTCUSDT"
    assert row["price"] == pytest.approx(65000.5)
    assert set(parsed.columns) == {"symbol", "price", "volume", "exchange", "event_time"}


def test_compute_window_metrics_aggregates_per_symbol_per_window(spark):
    t0 = datetime(2026, 6, 20, 12, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 6, 20, 12, 0, 30, tzinfo=timezone.utc)  # same 1-min window as t0
    t2 = datetime(2026, 6, 20, 12, 1, 5, tzinfo=timezone.utc)  # next 1-min window

    rows = [
        Row(symbol="BTCUSDT", price=100.0, volume=10.0, exchange="binance", event_time=t0),
        Row(symbol="BTCUSDT", price=102.0, volume=20.0, exchange="binance", event_time=t1),
        Row(symbol="BTCUSDT", price=110.0, volume=30.0, exchange="binance", event_time=t2),
        Row(symbol="ETHUSDT", price=10.0, volume=1.0, exchange="binance", event_time=t0),
    ]
    df = spark.createDataFrame(rows)

    metrics = compute_window_metrics(df, "1 minute").orderBy("symbol", "window_start").collect()

    btc_first_window = [r for r in metrics if r.symbol == "BTCUSDT" and r.sample_count == 2][0]
    assert btc_first_window.avg_price == pytest.approx(101.0)
    assert btc_first_window.min_price == pytest.approx(100.0)
    assert btc_first_window.max_price == pytest.approx(102.0)
    assert btc_first_window.total_volume == pytest.approx(15.0)

    btc_second_window = [r for r in metrics if r.symbol == "BTCUSDT" and r.sample_count == 1][0]
    assert btc_second_window.avg_price == pytest.approx(110.0)

    eth_window = [r for r in metrics if r.symbol == "ETHUSDT"][0]
    assert eth_window.sample_count == 1
    assert eth_window.avg_price == pytest.approx(10.0)


def test_compute_window_metrics_hourly_rollup_groups_more_coarsely(spark):
    t0 = datetime(2026, 6, 20, 12, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 6, 20, 12, 45, 0, tzinfo=timezone.utc)  # same hour as t0

    rows = [
        Row(symbol="BTCUSDT", price=100.0, volume=10.0, exchange="binance", event_time=t0),
        Row(symbol="BTCUSDT", price=200.0, volume=10.0, exchange="binance", event_time=t1),
    ]
    df = spark.createDataFrame(rows)
    metrics = compute_window_metrics(df, "1 hour").collect()
    assert len(metrics) == 1
    assert metrics[0].sample_count == 2
    assert metrics[0].avg_price == pytest.approx(150.0)
