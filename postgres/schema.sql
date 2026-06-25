-- Staging layer schema for the crypto market data pipeline.
-- Raw, lightly-validated data lands here before being published to Kafka.

CREATE TABLE IF NOT EXISTS symbols (
    symbol          TEXT PRIMARY KEY,
    base_asset      TEXT NOT NULL,
    quote_asset     TEXT NOT NULL,
    exchange        TEXT NOT NULL DEFAULT 'binance',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Live ticker snapshots (one row per poll, per symbol).
CREATE TABLE IF NOT EXISTS prices (
    id              BIGSERIAL PRIMARY KEY,
    symbol          TEXT NOT NULL REFERENCES symbols(symbol),
    price           NUMERIC(20, 8) NOT NULL,
    volume          NUMERIC(28, 8) NOT NULL DEFAULT 0,  -- rolling 24h volume snapshot
    exchange        TEXT NOT NULL DEFAULT 'binance',
    event_time      TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_prices_symbol_time ON prices (symbol, event_time DESC);

-- Historical OHLCV bars ("trades" per the project's table naming convention).
CREATE TABLE IF NOT EXISTS trades (
    id              BIGSERIAL PRIMARY KEY,
    symbol          TEXT NOT NULL REFERENCES symbols(symbol),
    open_time       TIMESTAMPTZ NOT NULL,
    close_time      TIMESTAMPTZ NOT NULL,
    open            NUMERIC(20, 8) NOT NULL,
    high            NUMERIC(20, 8) NOT NULL,
    low             NUMERIC(20, 8) NOT NULL,
    close           NUMERIC(20, 8) NOT NULL,
    volume          NUMERIC(28, 8) NOT NULL,
    interval        TEXT NOT NULL DEFAULT '1m',
    exchange        TEXT NOT NULL DEFAULT 'binance',
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (symbol, open_time, interval, exchange)
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades (symbol, open_time DESC);

-- Operational log of every ingestion run, used for monitoring and backfills.
CREATE TABLE IF NOT EXISTS ingestion_logs (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT NOT NULL,
    task            TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('success', 'failure')),
    rows_ingested   INTEGER NOT NULL DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
