# Crypto Market Data Engineering Capstone Project

## Overview

This project builds a production-style distributed data pipeline for ingesting, processing, and storing cryptocurrency market data using modern data engineering technologies.

The goal is to design an end-to-end system that demonstrates both batch and streaming data processing patterns while following industry best practices for orchestration, scalability, security, and maintainability.

This repository serves as a portfolio-focused data engineering project showcasing real-time data pipelines, distributed systems, and modern data architecture principles.

---

## Project Objectives

The pipeline is designed to:

- Ingest cryptocurrency market data from public exchange APIs
- Store raw market data for staging and historical analysis
- Stream data through a distributed messaging platform
- Process market events in near real time
- Persist transformed analytics data for fast querying
- Demonstrate scalable and modular data engineering architecture

---

## Data Sources

### Binance API (Primary Source)

The project uses the Binance API as the primary source of cryptocurrency market information, including:

- Live market prices
- Trading pairs
- Order book snapshots
- Historical OHLCV data

Documentation:

https://binance-docs.github.io/apidocs/

---

### Kraken API (Optional Extension)

As an enhancement, the pipeline may also integrate the Kraken API to enable multi-exchange analysis.

Potential datasets include:

- Trade data
- OHLC candles
- Asset pairs
- Market depth information

Documentation:

https://docs.kraken.com/

---

# Architecture

The project follows a modern data engineering architecture consisting of the following layers.

---

## 1. Data Ingestion Layer

### Technologies

- Python
- Apache Airflow
- Exchange APIs

### Responsibilities

- Retrieve market data from exchange APIs
- Perform initial validation checks
- Schedule ingestion workflows using Airflow
- Store raw datasets in PostgreSQL

---

## 2. Staging Layer

### Technology

- PostgreSQL

### Responsibilities

- Store raw API responses
- Maintain historical records
- Serve as an intermediate layer before streaming

Example tables include:

- prices
- trades
- symbols
- ingestion_logs

---

## 3. Streaming Layer

### Technology

- Apache Kafka

### Optional Components

- Kafka UI
- Schema Registry

### Responsibilities

- Publish market events into Kafka topics
- Enable real-time data movement
- Decouple ingestion and processing services

---

## 4. Processing Layer

### Technology

- Apache Spark Structured Streaming

### Responsibilities

- Consume Kafka topics
- Perform distributed transformations
- Generate analytical datasets

Potential analytics include:

- Minute-level price averages
- Hourly price trends
- Trading volume metrics
- Volatility calculations
- Market movement indicators

Future enhancements may include anomaly detection and predictive analytics.

---

## 5. Serving Layer

### Technology

- Apache Cassandra

### Responsibilities

- Store processed market metrics
- Support analytical queries
- Serve as the final destination for transformed data

---

# Workflow

1. Airflow triggers data ingestion jobs.
2. Market data is fetched from Binance and/or Kraken.
3. Raw records are stored in PostgreSQL.
4. Data is published into Kafka topics.
5. Spark consumes Kafka streams and performs transformations.
6. Processed analytics are stored in Cassandra.
7. Dashboards or APIs can consume the processed data.

---

# Security and Configuration

All secrets and configuration values are stored using environment variables — see `.env.example` for the full list (exchange keys, symbols to track, Postgres/Kafka/Cassandra connection settings). Copy it to `.env` and fill in real values; `.env` itself is gitignored.

---

# Repository Structure

```text
crypto-market-pipeline/
│
├── dags/
│   └── airflow_workflows.py        # Two DAGs: 5-min ticker ingestion, hourly kline backfill
│
├── spark_jobs/
│   └── stream_processing.py        # Kafka -> windowed aggregations -> Cassandra
│
├── streaming/
│   └── producer.py                 # Publishes ingested rows onto the Kafka topic
│
├── cassandra/
│   └── schema.cql                  # Serving layer: minute/hourly/volume metrics tables
│
├── postgres/
│   └── schema.sql                  # Staging layer: symbols, prices, trades, ingestion_logs
│
├── src/ingestion/
│   ├── binance_client.py           # Primary source: 24hr ticker + klines, parse/validate
│   ├── kraken_client.py            # Optional source, thin adapter, same row shape
│   └── db.py                       # Postgres writer (SQL-building kept separate from I/O)
│
├── docker/
│   ├── docker-compose.yml
│   └── init/01_create_airflow_db.sql
│
├── tests/                          # See "Testing" below
│
├── .env.example
├── .gitignore
├── Makefile
├── pytest.ini
├── requirements.txt
└── README.md
```

---

# Technology Stack

| Component            | Technology            |
| -------------------- | --------------------- |
| Orchestration        | Apache Airflow        |
| Data Source          | Binance / Kraken APIs |
| Staging Database     | PostgreSQL            |
| Streaming Platform   | Apache Kafka          |
| Processing Engine    | Apache Spark          |
| Serving Database     | Cassandra             |
| Programming Language | Python                |
| Containerization     | Docker                |

---

## Compaction decisions

A few deliberate simplifications keep the stack lean:

- **No Zookeeper.** Kafka runs single-node in KRaft mode (`apache/kafka` image),
  cutting one full service out of the stack.
- **No Kafka UI / Schema Registry.** Listed as optional in the original plan;
  omitted to keep the footprint small. Add them back via the Kafka docs if
  you want topic inspection or Avro/Protobuf schemas.
- **One Postgres instance, two databases.** Airflow's metadata DB
  (`airflow_db`) and the staging DB (`crypto_pipeline`) share a single
  Postgres container (`docker/init/01_create_airflow_db.sql` creates the
  second database on first boot), instead of running a dedicated Postgres
  just for Airflow.
- **Airflow `standalone`.** Webserver, scheduler, and triggerer run as one
  process in one container — appropriate for a single-node demo/portfolio
  deployment, not for production HA.
- **No separate Kafka consumer service.** Spark Structured Streaming reads
  directly from the topic; writing a second, simpler consumer would just be
  duplicate plumbing.
- **Cassandra writes via `cassandra-driver` in `foreachBatch`**, not the
  `spark-cassandra-connector` jar — one fewer native dependency to manage,
  at the cost of doing the write per-row in Python rather than as a bulk
  Spark-native sink. Fine at this data volume; revisit if throughput grows.
- **Volume is a 24h rolling snapshot, not a true per-interval delta.** The
  ticker endpoint reports rolling 24h volume; true per-minute trade volume
  would need a trade-level feed (`aggTrades`), which is listed under Future
  Enhancements rather than built now.

---

# Quickstart

```bash
cp .env.example .env        # then edit values as needed
cd docker
docker compose up -d
```

Then:

- Airflow UI: http://localhost:8080 (the `standalone` command prints the
  generated admin password to its logs on first boot)
- Kafka: localhost:9092
- Postgres: localhost:5432
- Cassandra: localhost:9042

`cassandra-init` applies `cassandra/schema.cql` once Cassandra reports
healthy, then exits — that's expected, it's a one-shot job, not a bug.

---

# Testing

```bash
pip install -r requirements.txt
pytest -v
```

What's covered, and how:

| Area | How it's tested |
| --- | --- |
| Binance/Kraken parsing & validation | Mocked HTTP responses (`requests_mock`); no network calls |
| Postgres writer (`db.py`) | SQL-building helpers tested directly; verified end-to-end against a real local Postgres during development |
| Kafka producer message shaping | Pure-function tests on `build_message` / `build_messages`; no broker required |
| Spark windowed aggregations | Run against a real local `SparkSession` with static DataFrames — genuine Spark execution, no Kafka/Cassandra needed |
| Airflow DAG structure | Parsed via `ast` and checked for the expected `@dag`/`@task` structure, schedules, and that each DAG factory is actually invoked — full Airflow isn't installed in every dev environment, so this avoids requiring it just to validate DAG shape |
| Docker Compose file | Parsed as YAML and reviewed; not run end-to-end (would require Docker-in-Docker / a real cluster, out of scope for unit tests) |

The Postgres and Spark tests above were validated against the live
schema/engine while building this project — `postgres/schema.sql` was
applied to a real PostgreSQL 16 instance and exercised with constraint
violations on purpose (FK, `UNIQUE`, `CHECK`) to confirm they hold; the
Spark aggregation logic ran against real `SparkSession` instances rather
than mocks. Cassandra and Kafka don't have a lightweight way to spin up
in a sandboxed CI-style environment, so those integration paths are
covered by code review and the Compose healthchecks rather than an
automated end-to-end test.

---

# Learning Outcomes

This project demonstrates practical experience with:

- Data ingestion pipelines
- Event-driven architectures
- Stream processing systems
- Distributed data storage
- Workflow orchestration
- Production-oriented data engineering practices

The final outcome is a reproducible and scalable cryptocurrency market data platform that showcases modern data engineering concepts and can be extended with additional analytical and operational capabilities.
