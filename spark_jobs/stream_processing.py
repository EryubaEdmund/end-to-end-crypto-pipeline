"""Spark Structured Streaming job: Kafka -> windowed analytics -> Cassandra.

Run with:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        spark_jobs/stream_processing.py

Design note: the windowed-aggregation logic lives in `compute_window_metrics`,
a plain function of (DataFrame, window_duration) -> DataFrame. That makes it
testable against a small static DataFrame in a local SparkSession, with no
Kafka broker or Cassandra cluster required (see tests/test_stream_processing.py).
The streaming wiring (`build_pipeline`, `main`) is a thin layer on top.

Writing to Cassandra goes through `foreachBatch` + the cassandra-driver
(rather than the spark-cassandra-connector jar) to keep the deployment
footprint smaller — one less native dependency to manage.
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, from_json, max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import stddev, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto.prices")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "crypto_metrics")

MESSAGE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("volume", DoubleType(), nullable=True),
        StructField("exchange", StringType(), nullable=True),
        StructField("event_time", TimestampType(), nullable=False),
    ]
)


# --------------------------------------------------------------------------- #
# Pure transform logic — testable with a local SparkSession, no cluster needed.
# --------------------------------------------------------------------------- #

def parse_kafka_value(raw_df: DataFrame) -> DataFrame:
    """Parse the Kafka `value` column (JSON bytes) into typed columns."""
    return (
        raw_df.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), MESSAGE_SCHEMA).alias("data"))
        .select("data.*")
    )


def compute_window_metrics(df: DataFrame, window_duration: str) -> DataFrame:
    """Aggregate price + volume metrics over tumbling windows of `symbol`.

    `window_duration` is a Spark duration string, e.g. "1 minute" or "1 hour".
    `total_volume` is the average of the rolling 24h-volume snapshots seen in
    the window — a stand-in for true per-interval trade volume, which would
    require a trade-level feed (see README's "Future Enhancements").
    """
    return (
        df.groupBy(window(col("event_time"), window_duration), col("symbol"))
        .agg(
            avg("price").alias("avg_price"),
            spark_min("price").alias("min_price"),
            spark_max("price").alias("max_price"),
            stddev("price").alias("volatility"),
            avg("volume").alias("total_volume"),
            count("price").alias("sample_count"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "avg_price",
            "min_price",
            "max_price",
            "volatility",
            "total_volume",
            "sample_count",
        )
    )


# --------------------------------------------------------------------------- #
# Cassandra sink, used inside foreachBatch.
# --------------------------------------------------------------------------- #

def write_metrics_to_cassandra(batch_df: DataFrame, table: str) -> None:
    """Write one micro-batch of aggregated metrics to a Cassandra table."""
    if batch_df.isEmpty():
        return

    from cassandra.cluster import Cluster

    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    try:
        if table == "volume_metrics":
            insert_cql = f"""
                INSERT INTO {table} (symbol, window_start, window_end, total_volume, trade_count)
                VALUES (?, ?, ?, ?, ?)
            """
            prepared = session.prepare(insert_cql)
            for row in batch_df.collect():
                session.execute(
                    prepared,
                    (row.symbol, row.window_start, row.window_end, row.total_volume, row.sample_count),
                )
        else:
            insert_cql = f"""
                INSERT INTO {table}
                    (symbol, window_start, window_end, avg_price, min_price, max_price, volatility, sample_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            prepared = session.prepare(insert_cql)
            for row in batch_df.collect():
                session.execute(
                    prepared,
                    (
                        row.symbol, row.window_start, row.window_end, row.avg_price,
                        row.min_price, row.max_price, row.volatility, row.sample_count,
                    ),
                )
    finally:
        cluster.shutdown()


# --------------------------------------------------------------------------- #
# Streaming wiring.
# --------------------------------------------------------------------------- #

def build_pipeline(spark: SparkSession):
    """Wire Kafka -> parse -> two windowed aggregations -> Cassandra sinks.

    Returns the two streaming queries (minute, hourly) so callers can manage
    their lifecycle (await termination, stop, etc).
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    parsed = parse_kafka_value(raw).withWatermark("event_time", "2 minutes")

    minute_metrics = compute_window_metrics(parsed, "1 minute")
    hourly_metrics = compute_window_metrics(parsed, "1 hour")

    minute_query = (
        minute_metrics.writeStream.outputMode("update")
        .foreachBatch(lambda df, _id: write_metrics_to_cassandra(df, "minute_metrics"))
        .option("checkpointLocation", "/tmp/checkpoints/minute_metrics")
        .start()
    )
    volume_query = (
        minute_metrics.writeStream.outputMode("update")
        .foreachBatch(lambda df, _id: write_metrics_to_cassandra(df, "volume_metrics"))
        .option("checkpointLocation", "/tmp/checkpoints/volume_metrics")
        .start()
    )
    hourly_query = (
        hourly_metrics.writeStream.outputMode("update")
        .foreachBatch(lambda df, _id: write_metrics_to_cassandra(df, "hourly_metrics"))
        .option("checkpointLocation", "/tmp/checkpoints/hourly_metrics")
        .start()
    )
    return [minute_query, volume_query, hourly_query]


def main():
    spark = SparkSession.builder.appName("crypto-market-stream-processing").getOrCreate()
    queries = build_pipeline(spark)
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
