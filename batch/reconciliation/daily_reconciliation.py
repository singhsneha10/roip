"""
daily_reconciliation.py
────────────────────────
Compares expected vs. actual record counts between
Bronze and Silver for a given date.

In production this would also compare against source system
row counts fetched via API. Here we simulate the source count
using the Bronze layer as ground truth.

Reconciliation mismatches → alert → automatic backfill trigger.
This is what data engineering teams spend 40% of their time on.
"""

import sys
import logging
import argparse
from datetime import datetime, timedelta
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../streaming/utils"))

from spark_session import get_spark_session
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("reconciliation")

BRONZE_PATH = "/tmp/roip/lakehouse/bronze/orders"
SILVER_ORDERS = "/tmp/roip/lakehouse/silver/orders"
RECON_RESULTS = "/tmp/roip/lakehouse/monitoring/reconciliation"

TOLERANCE_PCT = 1.0  # allow 1% variance before alerting


def reconcile(spark, processing_date: str):
    # ── Count Bronze (raw, non-duplicate) ────────────────────────────
    bronze_count = (
        spark.read.format("delta")
        .load(BRONZE_PATH)
        .filter(F.col("ingestion_date") == processing_date)
        .filter(F.col("is_duplicate") == "false")
        .count()
    )

    # ── Count Silver ──────────────────────────────────────────────────
    try:
        silver_count = (
            spark.read.format("delta")
            .load(SILVER_ORDERS)
            .filter(F.col("processing_date") == processing_date)
            .count()
        )
    except Exception:
        silver_count = 0
        logger.warning("Silver table not found or empty")

    # ── Compute variance ──────────────────────────────────────────────
    variance_pct = abs(bronze_count - silver_count) / max(bronze_count, 1) * 100

    status = "PASS" if variance_pct <= TOLERANCE_PCT else "FAIL"

    result = {
        "processing_date": processing_date,
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "variance_pct": round(variance_pct, 4),
        "tolerance_pct": TOLERANCE_PCT,
        "status": status,
        "checked_at": datetime.utcnow().isoformat(),
    }

    logger.info(
        f"Reconciliation [{status}] | "
        f"bronze={bronze_count} silver={silver_count} "
        f"variance={variance_pct:.2f}%"
    )

    if status == "FAIL":
        logger.error(
            f"RECONCILIATION MISMATCH on {processing_date}! "
            f"Variance {variance_pct:.2f}% exceeds tolerance {TOLERANCE_PCT}%. "
            f"Missing {bronze_count - silver_count} records in Silver. "
            f"ACTION: Trigger backfill job for {processing_date}."
        )

    # ── Persist result ────────────────────────────────────────────────
    result_df = spark.createDataFrame([result])
    (result_df.write.format("delta").mode("append").save(RECON_RESULTS))

    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=(datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
    )
    args = parser.parse_args()

    spark = get_spark_session(app_name=f"ROIP-Recon-{args.date}")
    result = reconcile(spark, args.date)
    spark.stop()
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
