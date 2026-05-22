"""
silver_to_gold.py
──────────────────
Batch job: Silver → Gold

Produces three Gold tables:
  1. fact_hourly_orders   — order counts, revenue by hour and city
  2. fact_daily_sla       — delivery SLA pass/fail rates per city
  3. fact_payment_health  — payment success/failure rates by gateway
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../streaming/utils"))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("silver-to-gold")

SILVER_ORDERS = "/tmp/roip/lakehouse/silver/orders"
GOLD_HOURLY = "/tmp/roip/lakehouse/gold/fact_hourly_orders"
GOLD_SLA = "/tmp/roip/lakehouse/gold/fact_daily_sla"
GOLD_PAYMENTS = "/tmp/roip/lakehouse/gold/fact_payment_health"


def read_silver(spark, processing_date: str) -> DataFrame:
    df = (
        spark.read.format("delta")
        .load(SILVER_ORDERS)
        .filter(F.col("event_date") == processing_date)
    )
    logger.info(f"Silver records for {processing_date}: {df.count()}")
    return df


def build_hourly_orders(df: DataFrame, processing_date: str) -> DataFrame:
    """
    Hourly revenue and order volume by city and category.
    Powers the real-time ops dashboard.
    """
    return (
        df.filter(F.col("event_type") == "ORDER_PLACED")
        .groupBy(
            F.col("event_date"),
            F.col("event_hour"),
            F.col("city"),
            F.col("category"),
        )
        .agg(
            F.count("*").alias("order_count"),
            F.sum("amount").alias("gross_revenue"),
            F.avg("amount").alias("avg_order_value"),
            F.sum("item_count").alias("total_items"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("discount_pct").alias("avg_discount_pct"),
        )
        .withColumn("processing_date", F.lit(processing_date))
        .withColumn("gold_loaded_at", F.current_timestamp())
    )


def build_daily_sla(df: DataFrame, processing_date: str) -> DataFrame:
    """
    Delivery SLA analysis per city.

    SLA = order delivered within 48 hours of placement.
    We compute this by joining ORDER_PLACED to ORDER_DELIVERED
    events for the same order_id and measuring the time gap.

    This demonstrates self-join on event streams — a common
    advanced interview question.
    """
    placed = df.filter(F.col("event_type") == "ORDER_PLACED").select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("city"),
        F.col("event_timestamp").alias("placed_at"),
    )

    delivered = df.filter(F.col("event_type") == "ORDER_DELIVERED").select(
        F.col("order_id"),
        F.col("event_timestamp").alias("delivered_at"),
    )

    # Join placed to delivered on order_id
    fulfillment = placed.join(delivered, on="order_id", how="left")

    # Compute delivery hours and SLA flag
    with_sla = (
        fulfillment.withColumn(
            "delivery_hours",
            (F.unix_timestamp("delivered_at") - F.unix_timestamp("placed_at")) / 3600,
        )
        .withColumn(
            "sla_met",
            F.when(
                F.col("delivered_at").isNotNull() & (F.col("delivery_hours") <= 48),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "delivery_status",
            F.when(F.col("delivered_at").isNull(), F.lit("PENDING"))
            .when(F.col("sla_met"), F.lit("ON_TIME"))
            .otherwise(F.lit("LATE")),
        )
    )

    return (
        with_sla.groupBy(
            (
                "event_date"
                if "event_date" in [c for c in with_sla.columns]
                else F.lit(processing_date).alias("event_date")
            ),
            "city",
            "delivery_status",
        )
        .agg(
            F.count("*").alias("order_count"),
            F.avg("delivery_hours").alias("avg_delivery_hours"),
        )
        .withColumn("event_date", F.lit(processing_date))
        .withColumn("processing_date", F.lit(processing_date))
        .withColumn("gold_loaded_at", F.current_timestamp())
    )


def build_payment_health(df: DataFrame, processing_date: str) -> DataFrame:
    """
    Payment success/failure rates by gateway.
    Ops teams watch this in real-time for gateway outages.
    """
    payments = df.filter(
        F.col("event_type").isin(
            ["PAYMENT_INITIATED", "PAYMENT_SUCCESS", "PAYMENT_FAILED"]
        )
    )

    return (
        payments.groupBy("event_date", "event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
        )
        .withColumn("processing_date", F.lit(processing_date))
        .withColumn("gold_loaded_at", F.current_timestamp())
    )


def write_gold(
    df: DataFrame, path: str, processing_date: str, partition_col: str = "event_date"
):
    """
    Write Gold table with date-based overwrite partition.

    Why overwrite partition, not full table?
    If you re-run for 2024-01-15, you want to replace only
    that day's Gold data — not the entire Gold table.
    Delta Lake's replaceWhere does exactly this.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{partition_col} = '{processing_date}'")
        .partitionBy(partition_col)
        .save(path)
    )
    logger.info(f"Gold written → {path} for {processing_date}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=(datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
    )
    args = parser.parse_args()

    logger.info(f"Silver→Gold job starting for date={args.date}")
    spark = get_spark_session(app_name=f"ROIP-Gold-{args.date}")

    silver_df = read_silver(spark, args.date)

    hourly_df = build_hourly_orders(silver_df, args.date)
    sla_df = build_daily_sla(silver_df, args.date)
    payment_df = build_payment_health(silver_df, args.date)

    write_gold(hourly_df, GOLD_HOURLY, args.date)
    write_gold(sla_df, GOLD_SLA, args.date)
    write_gold(payment_df, GOLD_PAYMENTS, args.date)

    logger.info("Silver→Gold complete.")
    spark.stop()


if __name__ == "__main__":
    main()
