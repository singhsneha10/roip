"""
bronze_to_silver.py
────────────────────
Batch job: Bronze → Silver

What this job does:
  1. Read Bronze Delta table for a given processing date
  2. Remove confirmed duplicates (keep is_duplicate = 'false')
  3. Cast all types correctly (millis → timestamp, string amounts → double)
  4. Validate business rules
  5. Apply SCD Type 2 merge for customer dimension
  6. Write Silver order facts partitioned by event_date

Run: python bronze_to_silver.py --date 2024-01-15
"""

import os
import sys

sys.path.insert(0, "../quality/monitors")
import argparse
import logging
from datetime import datetime, timedelta

from pipeline_metrics import BatchTimer, record_batch_processed, start_metrics_server

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

sys.path.insert(0, os.path.join(BASE_DIR, "streaming/utils"))
sys.path.insert(0, os.path.join(BASE_DIR, "streaming"))

from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("bronze-to-silver")

# ── Paths ─────────────────────────────────────────────────────────────
BRONZE_PATH = "/tmp/roip/lakehouse/bronze/orders"
SILVER_ORDERS = "/tmp/roip/lakehouse/silver/orders"
SILVER_CUSTOMERS = "/tmp/roip/lakehouse/silver/dim_customers"
SILVER_DQ_RESULTS = "/tmp/roip/lakehouse/silver/dq_results"


# ── Step 1: Read Bronze for the processing date ────────────────────────
def read_bronze(spark, processing_date: str) -> DataFrame:
    """
    Read only the partition we need — this is partition pruning.
    On a 100M record/day platform, reading one date partition
    instead of the full table is the difference between
    a 2-minute job and a 40-minute job.
    """
    logger.info(f"Reading Bronze for date={processing_date}")
    df = (
        spark.read.format("delta")
        .load(BRONZE_PATH)
        .filter(F.col("ingestion_date") == processing_date)
        # Drop confirmed duplicates — keep only first occurrence
        .filter(F.col("is_duplicate") == "false")
    )
    count = df.count()
    logger.info(f"Bronze records loaded: {count}")
    return df


# ── Step 2: Type casting and field extraction ──────────────────────────
def cast_and_extract(df: DataFrame, processing_date: str) -> DataFrame:
    """
    Bronze stores everything loosely typed.
    Silver has strict types — this is the transformation boundary.

    Key casting decisions:
    - occurred_at (epoch millis Long) → event_timestamp (TimestampType)
    - payload map entries (all String) → typed columns
    - amount String → Double (nulls become 0.0, not errors)
    """
    return (
        df
        # Convert epoch millis to proper timestamp
        .withColumn("event_timestamp", (F.col("occurred_at") / 1000).cast("timestamp"))
        .withColumn("event_date", F.to_date(F.col("event_timestamp")))
        .withColumn("event_hour", F.hour(F.col("event_timestamp")))
        # Extract typed fields from the payload map
        .withColumn("city", F.coalesce(F.col("payload")["city"], F.lit("UNKNOWN")))
        .withColumn(
            "category", F.coalesce(F.col("payload")["category"], F.lit("UNKNOWN"))
        )
        .withColumn(
            "amount", F.coalesce(F.col("payload")["amount"].cast("double"), F.lit(0.0))
        )
        .withColumn(
            "item_count",
            F.coalesce(F.col("payload")["item_count"].cast("integer"), F.lit(0)),
        )
        .withColumn(
            "discount_pct",
            F.coalesce(F.col("payload")["discount_pct"].cast("double"), F.lit(0.0)),
        )
        .withColumn("device", F.coalesce(F.col("payload")["device"], F.lit("UNKNOWN")))
        .withColumn("pincode", F.col("payload")["pincode"])
        # Pipeline metadata
        .withColumn("silver_loaded_at", F.current_timestamp())
        .withColumn("processing_date", F.lit(processing_date))
        # Drop raw fields we've now extracted
        .drop(
            "payload",
            "is_duplicate",
            "bronze_loaded_at",
            "kafka_partition",
            "kafka_offset",
        )
    )


# ── Step 3: Silver data quality checks ────────────────────────────────
def run_silver_dq(df: DataFrame, processing_date: str, spark) -> tuple[DataFrame, dict]:
    """
    Run DQ checks and return (clean_df, dq_metrics).
    We don't fail the job on DQ issues — we log and alert.
    This is production thinking: a failed pipeline is worse
    than a pipeline that completes with known quality gaps.
    """
    total = df.count()
    metrics = {"processing_date": processing_date, "total_records": total}

    # Check 1: amount must be positive
    neg_amount = df.filter(F.col("amount") <= 0).count()
    metrics["neg_amount_count"] = neg_amount
    metrics["neg_amount_pct"] = round(neg_amount / max(total, 1) * 100, 2)

    # Check 2: city must not be UNKNOWN
    unknown_city = df.filter(F.col("city") == "UNKNOWN").count()
    metrics["unknown_city_count"] = unknown_city
    metrics["unknown_city_pct"] = round(unknown_city / max(total, 1) * 100, 2)

    # Check 3: event_type distribution shouldn't change >50% vs 7-day avg
    # (simplified version — in production this compares to a rolling average)
    event_dist = {
        row["event_type"]: row["count"]
        for row in df.groupBy("event_type").count().collect()
    }
    metrics["event_type_distribution"] = str(event_dist)

    # Check 4: null customer_id
    null_customers = df.filter(F.col("customer_id").isNull()).count()
    metrics["null_customer_id_count"] = null_customers

    # Overall pass/fail
    metrics["dq_passed"] = (
        metrics["neg_amount_pct"] < 5.0
        and metrics["unknown_city_pct"] < 10.0
        and null_customers == 0
    )

    # Save DQ results to Delta
    dq_df = spark.createDataFrame([metrics])
    (dq_df.write.format("delta").mode("append").save(SILVER_DQ_RESULTS))

    logger.info(f"DQ results: {metrics}")
    if not metrics["dq_passed"]:
        logger.warning("DQ FAILED — pipeline continues but alert triggered")

    return df, metrics


# ── Step 4: Write Silver orders fact table ─────────────────────────────
def write_silver_orders(df: DataFrame, processing_date: str):
    """
    Write Silver orders using Delta MERGE for idempotency.

    Why MERGE instead of overwrite?
    If this job runs twice for the same date (Airflow retry),
    MERGE matches on event_id and only updates if the record
    changed — it never inserts duplicates.
    This is exactly-once semantics at the batch layer.
    """
    try:
        silver_table = DeltaTable.forPath(df.sparkSession, SILVER_ORDERS)
        logger.info("Silver table exists — using MERGE for idempotency")
        (
            silver_table.alias("target")
            .merge(df.alias("source"), "target.event_id = source.event_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    except Exception:
        # Table doesn't exist yet — create it
        logger.info("Silver table not found — creating with initial write")
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy("event_date")
            .option("overwriteSchema", "true")
            .save(SILVER_ORDERS)
        )

    logger.info(f"Silver orders written for {processing_date}")


# ── Step 5: SCD Type 2 customer dimension ─────────────────────────────
def upsert_customer_dimension(df: DataFrame, spark):
    """
    SCD Type 2: track customer attribute history.

    When a customer's city changes:
    - Close the existing record: effective_to = today, is_current = false
    - Insert a new record: effective_from = today, is_current = true

    This preserves history — you can always ask:
    "Where did customer X live when they placed order Y?"

    This is one of the most common data modelling interview topics.
    """
    # Build the latest snapshot of each customer from today's events
    customer_snapshot = (
        df.filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            F.first("city", ignorenulls=True).alias("city"),
            F.first("device", ignorenulls=True).alias("device"),
            F.max("event_timestamp").alias("last_seen_at"),
        )
        .withColumn("effective_from", F.current_date())
        .withColumn("effective_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn("dim_version", F.lit(1))
    )

    try:
        dim_table = DeltaTable.forPath(spark, SILVER_CUSTOMERS)

        # SCD2 MERGE logic:
        # 1. If customer exists with same city → update last_seen_at only
        # 2. If customer exists with DIFFERENT city → expire old, insert new
        # 3. If customer is new → insert
        (
            dim_table.alias("target")
            .merge(
                customer_snapshot.alias("source"),
                """
                target.customer_id = source.customer_id
                AND target.is_current = true
                """,
            )
            # Case 1: same city — just update last_seen_at
            .whenMatchedUpdate(
                condition="target.city = source.city",
                set={"last_seen_at": "source.last_seen_at"},
            )
            # Case 2: city changed — expire the old record
            .whenMatchedUpdate(
                condition="target.city != source.city",
                set={"effective_to": "source.effective_from", "is_current": "false"},
            )
            # Case 3: new customer
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Insert new version for customers whose city changed
        # (the MERGE above expired the old record; now we add the new one)
        changed_customers = (
            customer_snapshot.alias("new")
            .join(
                dim_table.toDF().filter("is_current = false").alias("expired"),
                (F.col("new.customer_id") == F.col("expired.customer_id"))
                & (F.col("expired.effective_to") == F.current_date()),
                "inner",
            )
            .select("new.*")
        )

        if not changed_customers.isEmpty():
            (
                changed_customers.write.format("delta")
                .mode("append")
                .save(SILVER_CUSTOMERS)
            )
            logger.info(
                f"SCD2: {changed_customers.count()} "
                f"customers had city changes — new versions inserted"
            )

    except Exception:
        logger.info("Customer dim not found — creating initial version")
        (
            customer_snapshot.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(SILVER_CUSTOMERS)
        )

    logger.info("Customer dimension SCD2 upsert complete")


# ── Main ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=(datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Processing date (YYYY-MM-DD). Defaults to yesterday.",
    )
    args = parser.parse_args()

    logger.info(f"Bronze→Silver job starting for date={args.date}")

    spark = get_spark_session(app_name=f"ROIP-Silver-{args.date}")

    # Start Prometheus metrics server
    start_metrics_server(port=8001)

    # Track total batch execution time
    with BatchTimer("bronze_to_silver", "silver"):
        bronze_df = read_bronze(spark, args.date)

        silver_df = cast_and_extract(bronze_df, args.date)

        clean_df, dq_metrics = run_silver_dq(silver_df, args.date, spark)

        write_silver_orders(clean_df, args.date)

        upsert_customer_dimension(clean_df, spark)

        # Record processed batch metrics
        record_batch_processed(
            layer="silver",
            source="bronze_orders",
            event_type="ALL",
            count=dq_metrics["total_records"],
        )

    logger.info(
        f"Bronze→Silver complete | "
        f"records={dq_metrics['total_records']} | "
        f"dq_passed={dq_metrics['dq_passed']}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
