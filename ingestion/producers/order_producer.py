import json
import logging
import signal
import time

from event_generator import EventGenerator
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("order-producer")

BOOTSTRAP_SERVERS = "localhost:9092"
EVENTS_PER_SECOND = 10  # tunable — crank up to stress test
DLQ_TOPIC = "dlq-events"

REQUIRED_FIELDS = {"event_id", "event_type", "order_id", "customer_id", "occurred_at"}


def validate_event(event: dict) -> tuple[bool, str]:
    """Returns (is_valid, reason)."""
    missing = REQUIRED_FIELDS - set(event.keys())
    if missing:
        return False, f"Missing fields: {missing}"
    if not event.get("event_id"):
        return False, "event_id is empty"
    return True, ""


def on_send_success(record_metadata):
    logger.debug(
        f"Delivered → topic={record_metadata.topic} "
        f"partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )


def on_send_error(exc):
    logger.error(f"Delivery failed: {exc}")


class OrderProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            # Reliability settings
            acks="all",  # wait for all in-sync replicas
            retries=3,
            max_in_flight_requests_per_connection=1,  # preserves ordering
            compression_type="snappy",
            linger_ms=5,  # batch events for 5ms before sending
            batch_size=16384,
        )
        self.generator = EventGenerator(chaos_rate=0.05)
        self.running = True
        self.sent_count = 0
        self.dlq_count = 0

        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, *_):
        logger.info(f"Shutting down. Sent={self.sent_count} DLQ={self.dlq_count}")
        self.running = False

    def route_event(self, topic: str, event: dict):
        """Validate → send to topic or DLQ."""
        is_valid, reason = validate_event(event)

        if not is_valid:
            # Wrap in DLQ envelope with failure metadata
            dlq_record = {
                "original_event": event,
                "failure_reason": reason,
                "source_topic": topic,
                "failed_at": int(time.time() * 1000),
            }
            self.producer.send(
                DLQ_TOPIC,
                key=event.get("event_id", "unknown"),
                value=dlq_record,
            ).add_callback(on_send_success).add_errback(on_send_error)
            self.dlq_count += 1
            logger.warning(f"→ DLQ | reason={reason}")
            return

        # Partition key = order_id (ensures order events are co-located)
        partition_key = event.get("order_id", event.get("event_id"))
        self.producer.send(
            topic,
            key=partition_key,
            value=event,
        ).add_callback(on_send_success).add_errback(on_send_error)
        self.sent_count += 1

    def run(self):
        logger.info(f"Producer started — {EVENTS_PER_SECOND} events/sec")
        interval = 1.0 / EVENTS_PER_SECOND

        while self.running:
            start = time.monotonic()
            topic, event = self.generator.next_event()
            self.route_event(topic, event)

            # Log throughput every 100 events
            if self.sent_count % 100 == 0 and self.sent_count > 0:
                logger.info(
                    f"Throughput checkpoint → "
                    f"sent={self.sent_count} dlq={self.dlq_count} "
                    f"dlq_rate={self.dlq_count/self.sent_count:.2%}"
                )

            elapsed = time.monotonic() - start
            sleep = max(0, interval - elapsed)
            time.sleep(sleep)

        self.producer.flush()
        self.producer.close()
        logger.info("Producer cleanly shut down.")


if __name__ == "__main__":
    OrderProducer().run()
