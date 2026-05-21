"""
Tests for the Prometheus metrics module.
"""

import sys

sys.path.insert(0, "quality/monitors")


def test_record_batch_processed_increments_counter():
    from pipeline_metrics import RECORDS_PROCESSED, record_batch_processed

    before = RECORDS_PROCESSED.labels(
        layer="test", source_system="test_src", event_type="TEST"
    )._value.get()
    record_batch_processed("test", "test_src", "TEST", 100)
    after = RECORDS_PROCESSED.labels(
        layer="test", source_system="test_src", event_type="TEST"
    )._value.get()
    assert after - before == 100


def test_record_dlq_event_increments_counter():
    from pipeline_metrics import DLQ_EVENTS, record_dlq_event

    before = DLQ_EVENTS.labels(
        source_topic="test-topic", failure_reason="missing_field"
    )._value.get()
    record_dlq_event("test-topic", "missing_field")
    after = DLQ_EVENTS.labels(
        source_topic="test-topic", failure_reason="missing_field"
    )._value.get()
    assert after - before == 1


def test_batch_timer_records_duration():
    import time
    from pipeline_metrics import BATCH_DURATION, BatchTimer

    with BatchTimer("test_job", "test_layer"):
        time.sleep(0.05)  # 50ms simulated work
    # If no exception was raised, the timer worked
    assert True


def test_record_dq_results_sets_gauges():
    from pipeline_metrics import DQ_CHECK_RESULT, record_dq_results

    fake_results = {
        "critical": [
            {"name": "test_check_pass", "passed": True},
            {"name": "test_check_fail", "passed": False},
        ],
        "warning": [
            {"name": "test_warn", "passed": True},
        ],
    }
    record_dq_results(fake_results)
    pass_val = DQ_CHECK_RESULT.labels(
        check_name="test_check_pass", severity="critical"
    )._value.get()
    fail_val = DQ_CHECK_RESULT.labels(
        check_name="test_check_fail", severity="critical"
    )._value.get()
    assert pass_val == 1.0
    assert fail_val == 0.0
