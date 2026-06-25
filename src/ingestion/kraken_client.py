"""Optional Kraken adapter.

Kept deliberately thin: it only needs to produce rows shaped like
`binance_client.parse_ticker_payload` does, so the rest of the pipeline
(Postgres writer, Kafka producer, Spark job) is exchange-agnostic.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import requests

DEFAULT_BASE_URL = os.getenv("KRAKEN_API_BASE", "https://api.kraken.com")
REQUEST_TIMEOUT_SECONDS = 10


class KrakenAPIError(RuntimeError):
    pass


def parse_ticker_payload(payload: dict[str, Any]) -> list[dict]:
    """Parse Kraken's `/0/public/Ticker` response into `prices`-shaped rows."""
    if payload.get("error"):
        raise KrakenAPIError(f"Kraken error: {payload['error']}")
    now = datetime.now(timezone.utc)
    rows = []
    for pair, data in payload.get("result", {}).items():
        # Kraken ticker: "c" = [last trade closed price, lot volume], "v" = [today volume, last 24h volume]
        rows.append(
            {
                "symbol": pair,
                "price": float(data["c"][0]),
                "volume": float(data["v"][1]) if "v" in data else 0.0,
                "exchange": "kraken",
                "event_time": now,
            }
        )
    return rows


def fetch_ticker_prices(pairs: list[str], base_url: str = DEFAULT_BASE_URL) -> list[dict]:
    response = requests.get(
        f"{base_url}/0/public/Ticker",
        params={"pair": ",".join(pairs)},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return parse_ticker_payload(response.json())
