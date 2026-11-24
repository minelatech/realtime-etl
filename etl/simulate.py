"""Local pipeline runner — no Spark, no Kafka.

    python -m etl.simulate data/sample_events.jsonl

Reads a JSONL file (one raw event per line, malformed lines allowed) and runs the
exact transformation logic used in production, printing clean rows, aggregates and
the dead-letter queue.
"""
from __future__ import annotations

import json
import sys

from .transforms import process_batch


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    path = argv[0] if argv else "data/sample_events.jsonl"

    with open(path, encoding="utf-8") as fh:
        raw_lines = [line.strip() for line in fh if line.strip()]

    result = process_batch(raw_lines)

    print(f"📦 input lines      : {len(raw_lines)}")
    print(f"✅ clean events     : {len(result['clean'])}")
    print(f"🧊 dead-letter (DLQ): {len(result['dlq'])}")
    for d in result["dlq"]:
        print(f"    - dropped: {d['reason']}")

    print("\n📊 aggregates (window, type, region → count, total):")
    for a in result["aggregates"]:
        print(
            f"    {a['window_start']}  {a['type']:<8} {a['region']:<6} "
            f"count={a['count']:<3} total={a['total_amount']}"
        )

    print("\nsample clean row:")
    if result["clean"]:
        print("    " + json.dumps(result["clean"][0]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
