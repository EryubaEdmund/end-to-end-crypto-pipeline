-- Runs once, on first container start, before postgres/schema.sql.
-- Gives Airflow its own metadata database inside the same Postgres instance,
-- so the project doesn't need a second database container just for Airflow.
CREATE DATABASE airflow_db;
