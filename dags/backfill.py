"""Airflow DAG — daily batch backfill that recomputes aggregates from raw events.

The batch layer guarantees correctness: it reprocesses a full day with the same
transformation rules as the streaming job, overwriting that day's aggregate partition.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"retries": 1, "retry_delay": timedelta(minutes=10)}


def recompute_day(ds=None, **_):
    import json
    import os

    import pandas as pd

    from etl.config import config
    from etl.transforms import aggregate

    events_path = f"{config.warehouse_uri}/events/event_date={ds}"
    if not os.path.exists(events_path):
        raise FileNotFoundError(f"no events for {ds}")

    df = pd.read_parquet(events_path)
    aggs = aggregate(df.to_dict("records"))

    out = f"{config.warehouse_uri}/aggregates/date={ds}"
    os.makedirs(out, exist_ok=True)
    with open(f"{out}/part.json", "w") as fh:
        json.dump(aggs, fh)
    return len(aggs)


with DAG(
    dag_id="daily_backfill",
    schedule="0 2 * * *",         # 02:00 every day
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["etl", "batch"],
) as dag:
    PythonOperator(task_id="recompute_day", python_callable=recompute_day)
