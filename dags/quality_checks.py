"""Airflow DAG — data-quality gates on the warehouse (runs hourly)."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"retries": 2, "retry_delay": timedelta(minutes=5)}


def check_freshness(**_):
    """Fail if no events landed in the last window (pipeline likely stalled)."""
    import glob
    import os

    from etl.config import config

    paths = glob.glob(f"{config.warehouse_uri}/events/**/*.parquet", recursive=True)
    if not paths:
        raise ValueError("no event parquet files found — pipeline may be down")
    newest = max(os.path.getmtime(p) for p in paths)
    age_min = (datetime.now().timestamp() - newest) / 60
    if age_min > 30:
        raise ValueError(f"stale data: newest partition is {age_min:.0f} min old")


def check_nulls(**_):
    """Assert key columns are non-null in the latest partition."""
    import pandas as pd  # Airflow workers have pandas

    from etl.config import config

    df = pd.read_parquet(f"{config.warehouse_uri}/events")
    for col in ("event_id", "ts", "type"):
        nulls = int(df[col].isna().sum())
        if nulls:
            raise ValueError(f"column {col} has {nulls} nulls")


with DAG(
    dag_id="quality_checks",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "quality"],
) as dag:
    freshness = PythonOperator(task_id="freshness", python_callable=check_freshness)
    nulls = PythonOperator(task_id="nulls", python_callable=check_nulls)
    freshness >> nulls
