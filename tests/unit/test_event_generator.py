import pytest
import sys

sys.path.insert(0, "ingestion/producers")
from event_generator import EventGenerator


@pytest.fixture
def gen():
    return EventGenerator(chaos_rate=0.0)  # no chaos for unit tests


def test_order_event_has_required_fields(gen):
    event = gen.generate_order_event()
    required = {
        "event_id",
        "event_type",
        "order_id",
        "customer_id",
        "occurred_at",
        "ingested_at",
        "schema_version",
        "payload",
    }
    assert required.issubset(event.keys())


def test_order_id_format(gen):
    event = gen.generate_order_event()
    assert event["order_id"].startswith("ORD-")


def test_late_event_is_in_the_past(gen):
    import time

    event = gen.generate_late_event()
    now_ms = int(time.time() * 1000)
    assert event["occurred_at"] < now_ms - (2 * 3600 * 1000)  # >2h old


def test_malformed_event_missing_order_id(gen):
    event = gen.generate_malformed_event()
    assert "order_id" not in event


def test_schema_evolved_event_version(gen):
    event = gen.generate_schema_evolved_event()
    assert event["schema_version"] == "2.0"
    assert "loyalty_points" in event["payload"]


def test_chaos_produces_some_duplicates():
    gen = EventGenerator(chaos_rate=1.0)  # max chaos
    # Pre-populate recent events
    for _ in range(10):
        gen.generate_order_event()
    dup = gen.generate_duplicate_event()
    assert dup is not None
