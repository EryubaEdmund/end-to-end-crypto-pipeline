"""Binance REST API client for the ingestion layer.

Design note: network I/O (`fetch_*`) is kept separate from pure parsing /
validation functions (`parse_*`, `validate_*`) so the logic can be unit
tested without hitting the network or mocking an HTTP library.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import requests

DEFAULT_BASE_URL = os.getenv("BINANCE_API_BASE", "https://api.binance.com")
REQUEST_TIMEOUT_SECONDS = 10


class BinanceAPIError(RuntimeError):
    """Raised when Binance returns an error payload or an unexpected shape."""


# --------------------------------------------------------------------------- #
# Pure parsing / validation (no network) — these are what the unit tests hit.
# --------------------------------------------------------------------------- #

def validate_ticker_payload(payload: Any) -> None:
    """Raise BinanceAPIError if the ticker payload doesn't look right."""
    if isinstance(payload, dict) and "code" in payload and "msg" in payload:
        raise BinanceAPIError(f"Binance error {payload['code']}: {payload['msg']}")
    if not isinstance(payload, list):
        raise BinanceAPIError(f"Expected a list of tickers, got {type(payload).__name__}")
    for item in payload:
        if "symbol" not in item or "lastPrice" not in item:
            raise BinanceAPIError(f"Malformed ticker entry: {item}")


def parse_ticker_payload(payload: list[dict]) -> list[dict]:
    """Turn raw Binance 24hr-ticker JSON into rows ready for the `prices` table.

    We use `/api/v3/ticker/24hr` rather than the bare price endpoint so each
    poll also carries a volume figure (`volume`, the rolling 24h trade
    volume). It's a snapshot, not a per-interval delta — the Spark job
    documents that distinction where it computes volume metrics.
    """
    validate_ticker_payload(payload)
    now = datetime.now(timezone.utc)
    rows = []
    for item in payload:
        rows.append(
            {
                "symbol": item["symbol"],
                "price": float(item["lastPrice"]),
                "volume": float(item.get("volume", 0.0)),
                "exchange": "binance",
                "event_time": now,
            }
        )
    return rows


def validate_klines_payload(payload: Any) -> None:
    if isinstance(payload, dict) and "code" in payload and "msg" in payload:
        raise BinanceAPIError(f"Binance error {payload['code']}: {payload['msg']}")
    if not isinstance(payload, list):
        raise BinanceAPIError(f"Expected a list of klines, got {type(payload).__name__}")
    for row in payload:
        if not isinstance(row, list) or len(row) < 11:
            raise BinanceAPIError(f"Malformed kline row: {row}")


def parse_klines_payload(payload: list[list], symbol: str, interval: str) -> list[dict]:
    """Turn raw Binance kline JSON into rows ready for the `trades` table.

    Binance kline row layout:
    [open_time, open, high, low, close, volume, close_time, ...]
    """
    validate_klines_payload(payload)
    rows = []
    for k in payload:
        rows.append(
            {
                "symbol": symbol,
                "interval": interval,
                "exchange": "binance",
                "open_time": datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc),
                "close_time": datetime.fromtimestamp(k[6] / 1000, tz=timezone.utc),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
            }
        )
    return rows


# --------------------------------------------------------------------------- #
# Network I/O
# --------------------------------------------------------------------------- #

def fetch_ticker_prices(symbols: list[str], base_url: str = DEFAULT_BASE_URL) -> list[dict]:
    """Fetch current price + 24h volume for a list of symbols, e.g. ["BTCUSDT", "ETHUSDT"]."""
    response = requests.get(f"{base_url}/api/v3/ticker/24hr", timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    rows = parse_ticker_payload(payload)
    wanted = set(symbols)
    return [row for row in rows if row["symbol"] in wanted]


def fetch_klines(
    symbol: str,
    interval: str = "1m",
    limit: int = 60,
    base_url: str = DEFAULT_BASE_URL,
) -> list[dict]:
    """Fetch historical OHLCV candles for a single symbol."""
    response = requests.get(
        f"{base_url}/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    return parse_klines_payload(payload, symbol=symbol, interval=interval)
