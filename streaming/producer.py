"""Kafka producer for the streaming layer.

Publishes freshly-ingested ticker rows onto a Kafka topic so the
ingestion service and the Spark processing service stay decoupled —
neither needs to know the other exists, they only share the topic contract.

`build_message` is a pure function (row dict -> JSON-serializable dict),
kept separate from the actual `KafkaProducer` so it's testable without a
broker.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Iterable

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto.prices")


def build_message(row: dict) -> dict:
    """Convert a `prices`-shaped row into a JSON-safe Kafka message."""
    event_time = row["event_time"]
    if isinstance(event_time, datetime):
        event_time = event_time.isoformat()
    return {
        "symbol": row["symbol"],
        "price": float(row["price"]),
        "volume": float(row.get("volume", 0.0)),
        "exchange": row.get("exchange", "binance"),
        "event_time": event_time,
    }


def build_messages(rows: Iterable[dict]) -> list[dict]:
    return [build_message(r) for r in rows]


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def publish_rows(rows: list[dict], producer: KafkaProducer | None = None, topic: str = KAFKA_TOPIC) -> int:
    """Publish each row to Kafka, keyed by symbol so per-symbol order is preserved."""
    if not rows:
        return 0
    owns_producer = producer is None
    producer = producer or get_producer()
    try:
        for row in rows:
            message = build_message(row)
            producer.send(topic, key=message["symbol"], value=message)
        producer.flush()
    finally:
        if owns_producer:
            producer.close()
    return len(rows)


if __name__ == "__main__":
    # Manual smoke-test entry point: fetch live prices and publish them once.
    from src.ingestion.binance_client import fetch_ticker_prices

    symbols = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(",")
    rows = fetch_ticker_prices(symbols)
    count = publish_rows(rows)
    print(f"Published {count} messages to topic '{KAFKA_TOPIC}'")
