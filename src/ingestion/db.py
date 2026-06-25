"""PostgreSQL staging writer.

As with the exchange clients, the SQL-building logic is separated from the
actual DB connection so it can be unit tested with no database running.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime
from typing import Iterable

import psycopg2
import psycopg2.extras


def get_connection_params() -> dict:
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "user": os.getenv("POSTGRES_USER", "admin"),
        "password": os.getenv("POSTGRES_PASSWORD", "password"),
        "dbname": os.getenv("POSTGRES_DB", "crypto_pipeline"),
    }


@contextmanager
def get_connection():
    conn = psycopg2.connect(**get_connection_params())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Pure helpers: turn row dicts into (sql, values) tuples — testable directly.
# --------------------------------------------------------------------------- #

def build_upsert_symbol(symbol: str, base_asset: str, quote_asset: str, exchange: str = "binance"):
    sql = """
        INSERT INTO symbols (symbol, base_asset, quote_asset, exchange)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (symbol) DO NOTHING
    """
    return sql, (symbol, base_asset, quote_asset, exchange)


def build_insert_prices(rows: Iterable[dict]):
    sql = """
        INSERT INTO prices (symbol, price, volume, exchange, event_time)
        VALUES (%s, %s, %s, %s, %s)
    """
    values = [(r["symbol"], r["price"], r.get("volume", 0), r["exchange"], r["event_time"]) for r in rows]
    return sql, values


def build_insert_trades(rows: Iterable[dict]):
    sql = """
        INSERT INTO trades
            (symbol, open_time, close_time, open, high, low, close, volume, interval, exchange)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, open_time, interval, exchange) DO NOTHING
    """
    values = [
        (
            r["symbol"], r["open_time"], r["close_time"], r["open"], r["high"],
            r["low"], r["close"], r["volume"], r["interval"], r["exchange"],
        )
        for r in rows
    ]
    return sql, values


def build_insert_log(
    source: str, task: str, status: str, rows_ingested: int,
    started_at: datetime, error_message: str | None = None,
):
    sql = """
        INSERT INTO ingestion_logs (source, task, status, rows_ingested, error_message, started_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    return sql, (source, task, status, rows_ingested, error_message, started_at)


# --------------------------------------------------------------------------- #
# I/O: actually execute the above against Postgres.
# --------------------------------------------------------------------------- #

def write_prices(rows: list[dict]) -> int:
    if not rows:
        return 0
    sql, values = build_insert_prices(rows)
    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, values)
    return len(values)


def write_trades(rows: list[dict]) -> int:
    if not rows:
        return 0
    sql, values = build_insert_trades(rows)
    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, values)
    return len(values)


def write_log(*args, **kwargs) -> None:
    sql, values = build_insert_log(*args, **kwargs)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, values)
