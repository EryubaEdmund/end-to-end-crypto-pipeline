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

All secrets and configuration values are stored using environment variables.

### Example `.env`

```bash
BINANCE_API_KEY=your_key_here
BINANCE_SECRET_KEY=your_secret_here

KRAKEN_API_KEY=your_key_here
KRAKEN_SECRET_KEY=your_secret_here

POSTGRES_USER=admin
POSTGRES_PASSWORD=password
POSTGRES_DB=crypto_pipeline
```

---

# Repository Structure

```text
crypto-market-pipeline/
│
├── dags/
│   └── airflow_workflows.py
│
├── spark_jobs/
│   └── stream_processing.py
│
├── kafka/
│   └── producer_consumer_configs/
│
├── cassandra/
│   └── schema.cql
│
├── postgres/
│   └── schema.sql
│
├── docker/
│   └── docker-compose.yml
│
├── .env.example
├── .gitignore
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

# Expected Deliverables

The repository contains or is expected to contain:

- Airflow DAGs
- Kafka producer and consumer services
- Spark streaming jobs
- PostgreSQL schema definitions
- Cassandra schema definitions
- Docker deployment configuration
- Environment setup documentation
- Architecture diagrams

---

# Future Enhancements

Potential improvements include:

- Full Docker Compose deployment
- Kafka monitoring dashboards
- Multi-exchange market comparison
- Real-time anomaly detection
- Data quality monitoring
- REST API for querying processed data
- Visualization dashboards using Grafana or Streamlit

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
