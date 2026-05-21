"""
pipeline_metrics.py
────────────────────
Prometheus metrics for the ROIP pipeline.

Pattern: jobs import this module and call update functions.
A long-running HTTP server exposes /metrics for Prometheus to scrape.

In production this runs as a sidecar container alongside each Spark job.
For local dev we run it as a background thread.

Metrics exposed:
  roip_records_processed_total    — counter, by layer and source
  roip_dlq_events_total           — counter, poison pill events
  roip_batch_duration_seconds     — histogram, job execution time
  roip_dq_check_result            — gauge, 1=pass 0=fail per check
  roip_pipeline_lag_seconds       — gauge, event_time to processed_time
  roip_duplicate_rate             — gauge, fraction of duplicate events
"""

import time
import threading
import logging
from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    start_http_server,
    CollectorRegistry,
    REGISTRY,
)

logger = logging.getLogger("pipeline-metrics")

# ── Metric definitions ─────────────────────────────────────────────────
# Counter: only goes up, never resets (use rate() in Grafana queries)
RECORDS_PROCESSED = Counter(
    "roip_records_processed_total",
    "Total records processed by the pipeline",
    labelnames=["layer", "source_system", "event_type"],
)

DLQ_EVENTS = Counter(
    "roip_dlq_events_total",
    "Total events routed to the dead letter queue",
    labelnames=["source_topic", "failure_reason"],
)

# Histogram: tracks distribution of values (use for latency/duration)
BATCH_DURATION = Histogram(
    "roip_batch_duration_seconds",
    "Spark job batch processing duration",
    labelnames=["job_name", "layer"],
    buckets=[30, 60, 120, 300, 600, 1800],  # 30s to 30min buckets
)

# Gauge: can go up or down (use for current state)
DQ_CHECK_RESULT = Gauge(
    "roip_dq_check_result",
    "Data quality check result (1=pass, 0=fail)",
    labelnames=["check_name", "severity"],
)

PIPELINE_LAG = Gauge(
    "roip_pipeline_lag_seconds",
    "Lag between event occurrence and pipeline processing",
    labelnames=["source_topic"],
)

DUPLICATE_RATE = Gauge(
    "roip_duplicate_rate",
    "Fraction of duplicate events in the current batch",
    labelnames=["source_topic"],
)

KAFKA_CONSUMER_LAG = Gauge(
    "roip_kafka_consumer_lag_records",
    "Kafka consumer group lag in number of records",
    labelnames=["topic", "partition"],
)

SILVER_RECORD_COUNT = Gauge(
    "roip_silver_record_count",
    "Number of records in Silver table for the processing date",
    labelnames=["processing_date"],
)


# ── Helper functions ───────────────────────────────────────────────────
def record_batch_processed(layer: str, source: str, event_type: str, count: int):
    RECORDS_PROCESSED.labels(
        layer=layer, source_system=source, event_type=event_type
    ).inc(count)


def record_dlq_event(topic: str, reason: str):
    DLQ_EVENTS.labels(source_topic=topic, failure_reason=reason).inc()


def record_dq_results(dq_results: dict):
    """Push DQ check results to Prometheus after each batch run."""
    for check in dq_results.get("critical", []):
        DQ_CHECK_RESULT.labels(check_name=check["name"], severity="critical").set(
            1 if check["passed"] else 0
        )

    for check in dq_results.get("warning", []):
        DQ_CHECK_RESULT.labels(check_name=check["name"], severity="warning").set(
            1 if check["passed"] else 0
        )


def record_pipeline_lag(topic: str, lag_seconds: float):
    PIPELINE_LAG.labels(source_topic=topic).set(lag_seconds)


def record_duplicate_rate(topic: str, rate: float):
    DUPLICATE_RATE.labels(source_topic=topic).set(rate)


class BatchTimer:
    """Context manager for timing Spark jobs."""

    def __init__(self, job_name: str, layer: str):
        self.job_name = job_name
        self.layer = layer
        self._start = None
        self._timer = BATCH_DURATION.labels(job_name=job_name, layer=layer)

    def __enter__(self):
        self._start = time.monotonic()
        return self

    def __exit__(self, *_):
        duration = time.monotonic() - self._start
        self._timer.observe(duration)
        logger.info(
            f"Job [{self.job_name}] layer={self.layer} " f"duration={duration:.1f}s"
        )


def start_metrics_server(port: int = 8000):
    """Start Prometheus HTTP server in a background thread."""

    def _serve():
        start_http_server(port)
        logger.info(f"Prometheus metrics server started on port {port}")
        while True:
            time.sleep(60)

    thread = threading.Thread(target=_serve, daemon=True)
    thread.start()
    logger.info(f"Metrics available at http://localhost:{port}/metrics")
    return thread
