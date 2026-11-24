"""Unit tests for the pure-Python transform logic (run offline, no Spark)."""
from etl.transforms import (
    aggregate,
    dedupe,
    enrich,
    parse,
    process_batch,
    validate,
)


def _ev(**over):
    base = {
        "event_id": "x1", "ts": "2024-06-01T10:00:00Z", "type": "purchase",
        "user_id": "u1", "region": "Nord", "amount": 50.0,
    }
    base.update(over)
    return base


def test_parse_handles_json_and_garbage():
    assert parse('{"a": 1}') == {"a": 1}
    assert parse(b'{"a": 1}') == {"a": 1}
    assert parse("not json") is None
    assert parse("[1,2,3]") is None  # not an object


def test_validate_rules():
    assert validate(_ev())[0] is True
    assert validate(_ev(type="teleport"))[0] is False
    assert validate(_ev(amount=-1))[0] is False
    assert validate(_ev(region=""))[0] is False
    assert validate(_ev(ts="nonsense"))[0] is False


def test_enrich_adds_derived_fields():
    e = enrich(_ev(amount=150))
    assert e["event_date"] == "2024-06-01"
    assert e["event_hour"] == 10
    assert e["is_high_value"] is True


def test_dedupe_by_event_id():
    out = dedupe([_ev(event_id="a"), _ev(event_id="a"), _ev(event_id="b")])
    assert {e["event_id"] for e in out} == {"a", "b"}
    assert len(out) == 2


def test_aggregate_windows_and_sums():
    events = [
        _ev(event_id="a", ts="2024-06-01T10:00:00Z", amount=100),
        _ev(event_id="b", ts="2024-06-01T10:02:00Z", amount=50),   # same 5-min window
        _ev(event_id="c", ts="2024-06-01T10:06:00Z", amount=20),   # next window
    ]
    aggs = aggregate(events, window_minutes=5)
    first = [a for a in aggs if a["window_start"].endswith("10:00:00+00:00")][0]
    assert first["count"] == 2
    assert first["total_amount"] == 150.0


def test_process_batch_routes_clean_and_dlq():
    raw = [
        '{"event_id":"a","ts":"2024-06-01T10:00:00Z","type":"purchase","user_id":"u1","region":"Nord","amount":10}',
        "{bad json",
        '{"event_id":"b","ts":"2024-06-01T10:00:00Z","type":"teleport","user_id":"u2","region":"Sud","amount":5}',
    ]
    res = process_batch(raw)
    assert len(res["clean"]) == 1
    assert len(res["dlq"]) == 2
    assert res["aggregates"][0]["count"] == 1
