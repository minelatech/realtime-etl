"""Pure-Python transformation logic (no Spark, no Kafka).

Every step here is deterministic and unit-tested, and the Spark job reuses the exact
same rules so local behaviour matches production.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable

from schemas.event import EVENT_TYPES, REQUIRED_FIELDS

from .config import config


# --------------------------------------------------------------------------- #
#  parse → validate → enrich
# --------------------------------------------------------------------------- #
def parse(raw) -> dict | None:
    """Parse a raw Kafka value (JSON string / bytes / dict) into a dict, or None."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="ignore")
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except (json.JSONDecodeError, TypeError):
        return None


def parse_ts(ts: str) -> datetime:
    """Parse an ISO-8601 timestamp (accepts a trailing 'Z') as UTC."""
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def validate(event: dict) -> tuple[bool, str]:
    """Return (is_valid, reason). Reason is empty when valid."""
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] in (None, ""):
            return False, f"missing field: {field}"
    if event["type"] not in EVENT_TYPES:
        return False, f"unknown type: {event['type']}"
    try:
        amount = float(event["amount"])
    except (TypeError, ValueError):
        return False, "amount not numeric"
    if amount < 0:
        return False, "negative amount"
    try:
        parse_ts(str(event["ts"]))
    except (ValueError, AttributeError):
        return False, "unparseable ts"
    return True, ""


def enrich(event: dict) -> dict:
    """Add derived fields used by analytics."""
    dt = parse_ts(str(event["ts"]))
    amount = float(event["amount"])
    return {
        **event,
        "amount": amount,
        "event_date": dt.date().isoformat(),
        "event_hour": dt.hour,
        "is_high_value": amount >= config.high_value_threshold,
    }


# --------------------------------------------------------------------------- #
#  dedupe → aggregate
# --------------------------------------------------------------------------- #
def dedupe(events: Iterable[dict]) -> list[dict]:
    """Keep the first event per event_id (idempotent ingestion)."""
    seen: set[str] = set()
    out: list[dict] = []
    for e in events:
        eid = e.get("event_id")
        if eid in seen:
            continue
        seen.add(eid)
        out.append(e)
    return out


def _window_start(ts: str, minutes: int) -> str:
    dt = parse_ts(ts)
    floored = dt.replace(minute=(dt.minute // minutes) * minutes, second=0, microsecond=0)
    return floored.isoformat()


def aggregate(events: Iterable[dict], window_minutes: int | None = None) -> list[dict]:
    """Group by (window_start, type, region) → count + total_amount."""
    window_minutes = window_minutes or config.window_minutes
    buckets: dict[tuple, dict] = defaultdict(lambda: {"count": 0, "total_amount": 0.0})
    for e in events:
        key = (_window_start(str(e["ts"]), window_minutes), e["type"], e["region"])
        buckets[key]["count"] += 1
        buckets[key]["total_amount"] += float(e.get("amount", 0.0))
    return [
        {
            "window_start": w,
            "type": t,
            "region": r,
            "count": v["count"],
            "total_amount": round(v["total_amount"], 2),
        }
        for (w, t, r), v in sorted(buckets.items())
    ]


# --------------------------------------------------------------------------- #
#  batch driver (used by the local simulator and Spark's foreachBatch)
# --------------------------------------------------------------------------- #
def process_batch(raw_events: Iterable) -> dict:
    """Full pipeline over a batch. Returns clean rows, aggregates and a DLQ."""
    clean, dlq = [], []
    for raw in raw_events:
        event = parse(raw)
        if event is None:
            dlq.append({"raw": raw, "reason": "unparseable"})
            continue
        ok, reason = validate(event)
        if not ok:
            dlq.append({"raw": event, "reason": reason})
            continue
        clean.append(enrich(event))

    clean = dedupe(clean)
    return {
        "clean": clean,
        "aggregates": aggregate(clean),
        "dlq": dlq,
    }
