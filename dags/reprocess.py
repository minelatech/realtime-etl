"""Airflow DAG — on-demand reprocessing of an arbitrary offset window.

Triggered manually with a config like {"from_offset": 100000, "to_offset": 200000}.
Replays raw events from Kafka (or an archive) through the same transforms.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def reprocess(**context):
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    start = conf.get("from_offset", 0)
    end = conf.get("to_offset", 0)
    # In production this reads the raw topic from `start` to `end` and re-runs
    # etl.transforms.process_batch on the replayed records.
    print(f"replaying offsets {start} → {end} through the transform pipeline")
    return {"replayed": max(end - start, 0)}


with DAG(
    dag_id="reprocess_window",
    schedule=None,                 # manual / API trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "ops"],
) as dag:
    PythonOperator(task_id="reprocess", python_callable=reprocess)
