"""Airflow DAG: ingest market data, stage it in Postgres, publish to Kafka.

Two independent schedules live in one DAG file for compactness:
  - `ingest_ticker_task`  : every 5 minutes, current price + 24h volume -> Postgres -> Kafka
  - `ingest_klines_task`  : hourly, historical OHLCV candles -> Postgres only

Kraken ingestion is wired in as an optional, best-effort task: if the
Kraken adapter raises, the task logs and succeeds anyway so an optional
exchange never blocks the primary Binance pipeline.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

SYMBOLS = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="crypto_market_ingestion",
    description="Ingest crypto market data from exchange APIs, stage it, and publish to Kafka",
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["crypto", "ingestion"],
)
def crypto_market_ingestion():
    @task
    def ingest_ticker() -> int:
        from src.ingestion import db
        from src.ingestion.binance_client import fetch_ticker_prices

        started_at = datetime.utcnow()
        try:
            rows = fetch_ticker_prices(SYMBOLS)
            written = db.write_prices(rows)
            db.write_log("binance", "ingest_ticker", "success", written, started_at)
            return written
        except Exception as exc:
            db.write_log("binance", "ingest_ticker", "failure", 0, started_at, str(exc))
            raise

    @task
    def ingest_ticker_kraken() -> int:
        """Optional extension. Failures here are logged, not raised, by design."""
        from src.ingestion import db
        from src.ingestion.kraken_client import fetch_ticker_prices as fetch_kraken_prices

        started_at = datetime.utcnow()
        try:
            pairs = [s.replace("USDT", "USD") for s in SYMBOLS]  # rough Binance->Kraken pair mapping
            rows = fetch_kraken_prices(pairs)
            written = db.write_prices(rows)
            db.write_log("kraken", "ingest_ticker_kraken", "success", written, started_at)
            return written
        except Exception as exc:  # noqa: BLE001 - intentionally non-fatal, optional source
            db.write_log("kraken", "ingest_ticker_kraken", "failure", 0, started_at, str(exc))
            return 0

    @task
    def publish_to_kafka(written: int) -> int:
        from streaming.producer import publish_rows
        from src.ingestion.binance_client import fetch_ticker_prices

        if written == 0:
            return 0
        rows = fetch_ticker_prices(SYMBOLS)
        return publish_rows(rows)

    ticker_rows = ingest_ticker()
    ingest_ticker_kraken()
    publish_to_kafka(ticker_rows)


@dag(
    dag_id="crypto_market_klines_backfill",
    description="Hourly ingestion of historical OHLCV candles into the staging layer",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["crypto", "ingestion", "backfill"],
)
def crypto_market_klines_backfill():
    @task
    def ingest_klines(symbol: str) -> int:
        from src.ingestion import db
        from src.ingestion.binance_client import fetch_klines

        started_at = datetime.utcnow()
        try:
            rows = fetch_klines(symbol, interval="1m", limit=60)
            written = db.write_trades(rows)
            db.write_log("binance", f"ingest_klines:{symbol}", "success", written, started_at)
            return written
        except Exception as exc:
            db.write_log("binance", f"ingest_klines:{symbol}", "failure", 0, started_at, str(exc))
            raise

    for symbol in SYMBOLS:
        ingest_klines.override(task_id=f"ingest_klines_{symbol}")(symbol)


crypto_market_ingestion()
crypto_market_klines_backfill()
