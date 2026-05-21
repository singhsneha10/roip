"""
Quick summary of Gold layer contents.
Run after silver_to_gold.py to verify outputs.
"""

import sys
import os

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "../../streaming/utils")
)

from spark_session import get_spark_session
from pyspark.sql   import functions as F

GOLD_HOURLY   = "/tmp/roip/lakehouse/gold/fact_hourly_orders"
GOLD_SLA      = "/tmp/roip/lakehouse/gold/fact_daily_sla"
GOLD_PAYMENTS = "/tmp/roip/lakehouse/gold/fact_payment_health"

def inspect():
    spark = get_spark_session(app_name="ROIP-Gold-Inspector")

    print("\n── Hourly Orders (top 10 by revenue) ────────────────────")
    spark.read.format("delta").load(GOLD_HOURLY) \
        .orderBy(F.col("gross_revenue").desc()) \
        .select("event_date","event_hour","city","category",
                "order_count","gross_revenue","avg_order_value") \
        .show(10, truncate=False)

    print("\n── Daily SLA by City ─────────────────────────────────────")
    spark.read.format("delta").load(GOLD_SLA) \
        .orderBy("city", "delivery_status") \
        .show(20, truncate=False)

    print("\n── Payment Health ────────────────────────────────────────")
    spark.read.format("delta").load(GOLD_PAYMENTS) \
        .orderBy("event_type") \
        .show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    inspect()