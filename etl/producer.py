"""Sample Kafka producer — emits synthetic events (and a few malformed ones).

    python -m etl.producer --rate 500 --count 10000

Uses kafka-python if available; otherwise writes JSONL to stdout so you can pipe or
inspect it without a broker.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from .config import config

REGIONS = ["Nord", "Sud", "Est", "Ouest", "Centre"]
TYPES = ["purchase", "click", "signup", "refund", "view"]


def make_event() -> dict:
    t = random.choice(TYPES)
    amount = round(random.uniform(5, 300), 2) if t in {"purchase", "refund"} else 0.0
    return {
        "event_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "type": t,
        "user_id": f"u{random.randint(1, 5000)}",
        "region": random.choice(REGIONS),
        "amount": amount,
    }


def _maybe_corrupt(event: dict) -> str:
    """~3% of records are malformed, to exercise the DLQ path."""
    if random.random() < 0.03:
        return "{not valid json"
    if random.random() < 0.03:
        event = {k: v for k, v in event.items() if k != "region"}  # missing field
    return json.dumps(event)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=200, help="events per second")
    parser.add_argument("--count", type=int, default=1000, help="total events")
    parser.add_argument("--topic", default=config.kafka_topic)
    args = parser.parse_args(argv)

    producer = _kafka_producer()
    delay = 1.0 / max(args.rate, 1)
    for _ in range(args.count):
        payload = _maybe_corrupt(make_event())
        if producer:
            producer.send(args.topic, payload.encode("utf-8"))
        else:
            sys.stdout.write(payload + "\n")
        time.sleep(delay)
    if producer:
        producer.flush()
        print(f"sent {args.count} events to '{args.topic}'", file=sys.stderr)
    return 0


def _kafka_producer():
    try:
        from kafka import KafkaProducer

        return KafkaProducer(bootstrap_servers=config.kafka_bootstrap)
    except Exception:
        return None  # no broker / lib → fall back to stdout JSONL


if __name__ == "__main__":
    raise SystemExit(main())
