-- Serving layer schema. Cassandra holds the *processed* analytics that
-- Spark produces, modeled for the queries the dashboards/API actually run
-- (query-first modeling, not normalized like the Postgres staging schema).

CREATE KEYSPACE IF NOT EXISTS crypto_metrics
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE crypto_metrics;

-- "Give me the last N minute-bars for symbol X" -> partition by symbol,
-- cluster by window start descending.
CREATE TABLE IF NOT EXISTS minute_metrics (
    symbol          TEXT,
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    avg_price       DOUBLE,
    min_price       DOUBLE,
    max_price       DOUBLE,
    volatility      DOUBLE,   -- stddev of price within the window
    sample_count    INT,
    PRIMARY KEY (symbol, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- Same shape, rolled up to the hour, for trend dashboards.
CREATE TABLE IF NOT EXISTS hourly_metrics (
    symbol          TEXT,
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    avg_price       DOUBLE,
    min_price       DOUBLE,
    max_price       DOUBLE,
    volatility      DOUBLE,
    sample_count    INT,
    PRIMARY KEY (symbol, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- Trading volume per symbol per minute window.
CREATE TABLE IF NOT EXISTS volume_metrics (
    symbol          TEXT,
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    total_volume    DOUBLE,
    trade_count     INT,
    PRIMARY KEY (symbol, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);
