"""
Quick inspection of the Bronze Delta table.
Run this in a separate terminal while the stream is running.
"""

import sys, os

sys.path.insert(0, os.path.dirname(__file__))
from spark_session import get_spark_session
from pyspark.sql import functions as F

BRONZE_PATH = "/tmp/roip/lakehouse/bronze/orders"


def inspect_bronze():
    spark = get_spark_session(app_name="ROIP-Inspector")

    print("\n── Bronze table: row counts by date ──────────────────────")
    spark.read.format("delta").load(BRONZE_PATH).groupBy("ingestion_date").agg(
        F.count("*").alias("total_events"),
        F.countDistinct("event_id").alias("unique_events"),
        F.sum(F.when(F.col("is_duplicate") == "true", 1).otherwise(0)).alias(
            "duplicates"
        ),
    ).orderBy("ingestion_date").show(truncate=False)

    print("\n── Bronze table: event type distribution ─────────────────")
    spark.read.format("delta").load(BRONZE_PATH).groupBy("event_type").count().orderBy(
        F.col("count").desc()
    ).show(truncate=False)

    print("\n── Bronze table: schema ──────────────────────────────────")
    spark.read.format("delta").load(BRONZE_PATH).printSchema()

    print("\n── Delta table history (last 5 versions) ─────────────────")
    from delta import DeltaTable

    dt = DeltaTable.forPath(spark, BRONZE_PATH)
    dt.history(5).select("version", "timestamp", "operation", "operationMetrics").show(
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    inspect_bronze()
