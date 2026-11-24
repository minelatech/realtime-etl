"""Realtime ETL — Kafka → PySpark → Airflow.

The transformation logic lives in :mod:`etl.transforms` as pure Python so it can be
unit-tested and run locally (``etl.simulate``) without a Spark cluster or Kafka, and
is reused by the Spark Structured Streaming job (``etl.streaming``).
"""
__version__ = "0.1.0"
