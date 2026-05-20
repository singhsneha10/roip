import json
import logging
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("verify-consumer")

def consume(topic: str = "orders-raw", max_messages: int = 20):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="verify-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=10000,  # stop after 10s of no messages
    )
    logger.info(f"Consuming from '{topic}' (max {max_messages} messages)...")
    count = 0
    for msg in consumer:
        logger.info(
            f"Partition={msg.partition} Offset={msg.offset} "
            f"Key={msg.key} EventType={msg.value.get('event_type')} "
            f"OrderId={msg.value.get('order_id')}"
        )
        count += 1
        if count >= max_messages:
            break
    logger.info(f"Consumed {count} messages.")
    consumer.close()

if __name__ == "__main__":
    consume("orders-raw")