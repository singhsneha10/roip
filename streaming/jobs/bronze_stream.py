"""
bronze_stream.py
────────────────
Spark Structured Streaming job:
  Kafka (orders-raw) → parse + validate + dedup → Bronze Delta table

Key patterns demonstrated:
  - Kafka source with explicit offset management
  - Watermarking for late event handling
  - foreachBatch for exactly-once Delta writes
  - Graceful shutdown handling
"""

import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../utils"))

from bronze_writer import write_to_bronze
from pyspark.sql import functions as F
from spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("bronze-stream")

# ── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "orders-raw"
CHECKPOINT_PATH = "/tmp/roip/checkpoints/bronze_orders"
TRIGGER_SECONDS = 30  # process a micro-batch every 30 seconds
WATERMARK_MINUTES = "10 minutes"  # wait up to 10 min for late events
MAX_OFFSETS = 50000  # max records per micro-batch (backpressure)


def build_kafka_stream(spark):
    """
    Read from Kafka as a streaming DataFrame.

    startingOffsets="latest" means: on first run, start from the
    newest message. On restart, the checkpoint overrides this and
    resumes from exactly where we left off.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)  # backpressure
        .option("failOnDataLoss", "false")  # tolerate Kafka log compaction
        .load()
        # Cast binary Kafka fields to strings
        .withColumn("key", F.col("key").cast("string"))
        .withColumn("value", F.col("value").cast("string"))
        .withColumn("partition", F.col("partition").cast("string"))
        .withColumn("offset", F.col("offset").cast("string"))
    )


def apply_watermark(df):
    """
    Apply watermark for late event handling.

    This tells Spark: "the event time is in 'kafka_timestamp',
    and I'm willing to wait up to 10 minutes for late data before
    closing a time window."

    Without this, Spark holds state for all time windows forever
    → memory leak → job dies after hours/days.

    Interview question: "What happens to events that arrive after
    the watermark?" Answer: they are silently dropped for aggregation
    purposes but we still write them to Bronze (we apply the watermark
    after the Bronze write in our architecture).
    """
    return df.withWatermark("event_time", WATERMARK_MINUTES)


def main():
    logger.info("Starting Bronze streaming job...")
    spark = get_spark_session(app_name="ROIP-Bronze-Stream")

    # Build the streaming source
    raw_stream = build_kafka_stream(spark)

    # Add event_time column for watermarking (Kafka timestamp as proxy)
    # In production you'd parse occurred_at from the JSON first.
    # We use kafka timestamp here as a safe fallback.
    timed_stream = raw_stream.withColumn(
        "event_time", (F.col("timestamp").cast("long") / 1000).cast("timestamp")
    )

    # Apply watermark
    watermarked = apply_watermark(timed_stream)

    # Write using foreachBatch — this is the production pattern
    # for combining streaming with Delta Lake operations that
    # require batch semantics (MERGE, dedup windows, etc.)
    query = (
        watermarked.writeStream.foreachBatch(write_to_bronze)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .queryName("bronze-orders-stream")
        .start()
    )

    logger.info(
        f"Stream running | topic={KAFKA_TOPIC} | "
        f"trigger={TRIGGER_SECONDS}s | watermark={WATERMARK_MINUTES}"
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Interrupt received — stopping stream gracefully...")
        query.stop()
        logger.info("Stream stopped cleanly.")


if __name__ == "__main__":
    main()
