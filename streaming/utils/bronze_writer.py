import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta import DeltaTable


logger = logging.getLogger(__name__)

BRONZE_PATH    = "/tmp/roip/lakehouse/bronze/orders"
DLQ_PATH       = "/tmp/roip/lakehouse/bronze/dlq"
PIPELINE_VER   = "1.0.0"


def write_to_bronze(batch_df: DataFrame, batch_id: int):
    """
    foreachBatch function — called for every micro-batch.

    Pattern:
      1. Parse JSON from Kafka value
      2. Validate fields
      3. Tag duplicates (don't drop — keep for audit)
      4. Route invalids to DLQ path
      5. Write valids to Bronze Delta table (partitioned by date)

    foreachBatch gives exactly-once semantics when combined with
    Delta Lake's idempotent writes — this is a key interview point.
    """
    from schemas import ORDER_EVENT_SCHEMA
    from validator import validate_bronze_events

    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: empty, skipping.")
        return

    # ── 1. Parse the JSON payload from Kafka ─────────────────────────
    parsed_df = (
        batch_df
        .withColumn(
            "parsed",
            F.from_json(F.col("value").cast("string"), ORDER_EVENT_SCHEMA)
        )
        .select(
            "parsed.*",
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
    )

    # ── 2. Add pipeline metadata columns ─────────────────────────────
    enriched_df = (
        parsed_df
        .withColumn("pipeline_version", F.lit(PIPELINE_VER))
        .withColumn("bronze_loaded_at",
                    F.lit(int(__import__("time").time() * 1000)))
        .withColumn(
            "ingestion_date",
            F.date_format(
                (F.col("occurred_at") / 1000).cast("timestamp"),
                "yyyy-MM-dd"
            )
        )
    )

    # ── 3. Validate ───────────────────────────────────────────────────
    valid_df, invalid_df = validate_bronze_events(enriched_df)

    # ── 4. Tag duplicates within this batch ──────────────────────────
    #    We use a window function to find the first occurrence of each
    #    event_id, then tag everything else as a duplicate.
    from pyspark.sql.window import Window
    window = Window.partitionBy("event_id").orderBy("bronze_loaded_at")

    deduped_df = (
        valid_df
        .withColumn("_row_num", F.row_number().over(window))
        .withColumn(
            "is_duplicate",
            F.when(F.col("_row_num") > 1, F.lit("true"))
             .otherwise(F.lit("false"))
        )
        .drop("_row_num")
    )

    # ── 5. Write valid events to Bronze (Delta, partitioned by date) ──
    record_count = deduped_df.count()
    dup_count    = deduped_df.filter(F.col("is_duplicate") == "true").count()

    (
        deduped_df.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .option("mergeSchema", "true")     # handles schema evolution
        .save(BRONZE_PATH)
    )

    # ── 6. Write invalid events to DLQ path ──────────────────────────
    if not invalid_df.isEmpty():
        (
            invalid_df.write
            .format("delta")
            .mode("append")
            .save(DLQ_PATH)
        )

    logger.info(
        f"Batch {batch_id}: written={record_count} "
        f"duplicates={dup_count} "
        f"dlq={invalid_df.count()}"
    )