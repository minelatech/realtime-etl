"""Pipeline configuration."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Config:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "events")
    dlq_topic: str = os.getenv("DLQ_TOPIC", "events.dlq")
    checkpoint_dir: str = os.getenv("CHECKPOINT_DIR", "./chk")
    warehouse_uri: str = os.getenv("WAREHOUSE_URI", "./warehouse")
    window_minutes: int = int(os.getenv("WINDOW_MINUTES", "5"))
    high_value_threshold: float = float(os.getenv("HIGH_VALUE_THRESHOLD", "100"))


config = Config()
