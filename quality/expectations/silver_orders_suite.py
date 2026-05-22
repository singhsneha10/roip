"""
silver_orders_suite.py
───────────────────────
Great Expectations suite for the Silver orders table.

Run standalone:  python silver_orders_suite.py --date 2024-01-15
Called by:       Airflow DAG after bronze_to_silver completes

Philosophy:
  We don't fail the entire pipeline on every DQ issue.
  We fail HARD on critical issues (null primary keys, zero records).
  We WARN on soft issues (high unknown city rate) and log metrics.
  This mirrors how mature data teams operate.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../streaming/utils"))
)

from pyspark.sql import functions as F
from spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("dq-silver-orders")

SILVER_ORDERS = "/tmp/roip/lakehouse/silver/orders"
REPORTS_PATH = "quality/reports"


# ── Define all expectations ────────────────────────────────────────────
def build_expectation_suite():
    """
    Returns a dict of expectations grouped by severity.

    CRITICAL: pipeline fails if any of these fail.
    WARNING:  pipeline continues but alert is triggered.

    In production these would live in a YAML config file
    and be versioned alongside the schema.
    """
    return {
        "critical": [
            {
                "name": "no_null_event_ids",
                "description": "event_id must never be null",
                "check": lambda df: df.filter(F.col("event_id").isNull()).count() == 0,
                "metric_name": "null_event_id_count",
                "metric_fn": lambda df: df.filter(F.col("event_id").isNull()).count(),
            },
            {
                "name": "no_null_order_ids",
                "description": "order_id must never be null",
                "check": lambda df: df.filter(F.col("order_id").isNull()).count() == 0,
                "metric_name": "null_order_id_count",
                "metric_fn": lambda df: df.filter(F.col("order_id").isNull()).count(),
            },
            {
                "name": "minimum_record_count",
                "description": "Silver must have at least 100 records per day",
                "check": lambda df: df.count() >= 100,
                "metric_name": "total_record_count",
                "metric_fn": lambda df: df.count(),
            },
            {
                "name": "valid_event_types_only",
                "description": "All event_type values must be in allowed set",
                "check": lambda df: df.filter(
                    ~F.col("event_type").isin(
                        [
                            "ORDER_PLACED",
                            "ORDER_CONFIRMED",
                            "ORDER_CANCELLED",
                            "PAYMENT_INITIATED",
                            "PAYMENT_SUCCESS",
                            "PAYMENT_FAILED",
                            "ORDER_SHIPPED",
                            "ORDER_DELIVERED",
                        ]
                    )
                ).count()
                == 0,
                "metric_name": "invalid_event_type_count",
                "metric_fn": lambda df: df.filter(
                    ~F.col("event_type").isin(
                        [
                            "ORDER_PLACED",
                            "ORDER_CONFIRMED",
                            "ORDER_CANCELLED",
                            "PAYMENT_INITIATED",
                            "PAYMENT_SUCCESS",
                            "PAYMENT_FAILED",
                            "ORDER_SHIPPED",
                            "ORDER_DELIVERED",
                        ]
                    )
                ).count(),
            },
            {
                "name": "no_future_events",
                "description": "event_timestamp must not be in the future",
                "check": lambda df: df.filter(
                    F.col("event_timestamp") > F.current_timestamp()
                ).count()
                == 0,
                "metric_name": "future_event_count",
                "metric_fn": lambda df: df.filter(
                    F.col("event_timestamp") > F.current_timestamp()
                ).count(),
            },
        ],
        "warning": [
            {
                "name": "amount_positive_rate",
                "description": ">=95% of amounts must be positive",
                "check": lambda df: (
                    df.filter(F.col("amount") > 0).count() / max(df.count(), 1)
                )
                >= 0.95,
                "metric_name": "negative_amount_pct",
                "metric_fn": lambda df: round(
                    df.filter(F.col("amount") <= 0).count() / max(df.count(), 1) * 100,
                    2,
                ),
            },
            {
                "name": "city_known_rate",
                "description": ">=90% of city values must not be UNKNOWN",
                "check": lambda df: (
                    df.filter(F.col("city") != "UNKNOWN").count() / max(df.count(), 1)
                )
                >= 0.90,
                "metric_name": "unknown_city_pct",
                "metric_fn": lambda df: round(
                    df.filter(F.col("city") == "UNKNOWN").count()
                    / max(df.count(), 1)
                    * 100,
                    2,
                ),
            },
            {
                "name": "schema_version_distribution",
                "description": "Track schema version spread (for evolution detection)",
                "check": lambda df: True,  # always passes — informational only
                "metric_name": "schema_v2_pct",
                "metric_fn": lambda df: round(
                    df.filter(F.col("schema_version") == "2.0").count()
                    / max(df.count(), 1)
                    * 100,
                    2,
                ),
            },
        ],
    }


# ── Run the suite ──────────────────────────────────────────────────────
def run_dq_suite(processing_date: str) -> dict:
    logger.info(f"Running DQ suite for Silver orders — date={processing_date}")
    spark = get_spark_session(app_name=f"ROIP-DQ-{processing_date}")

    try:
        df = (
            spark.read.format("delta")
            .load(SILVER_ORDERS)
            .filter(F.col("processing_date") == processing_date)
        )
    except Exception as e:
        logger.error(f"Cannot read Silver table: {e}")
        return {"status": "ERROR", "reason": str(e)}

    suite = build_expectation_suite()
    results = {
        "processing_date": processing_date,
        "run_timestamp": datetime.utcnow().isoformat(),
        "critical": [],
        "warning": [],
        "overall_status": "PASS",
    }

    # ── Run critical checks ────────────────────────────────────────────
    logger.info("Running CRITICAL checks...")
    for exp in suite["critical"]:
        metric_value = exp["metric_fn"](df)
        passed = exp["check"](df)

        result = {
            "name": exp["name"],
            "description": exp["description"],
            "passed": passed,
            "metric_name": exp["metric_name"],
            "metric_value": metric_value,
            "severity": "CRITICAL",
        }
        results["critical"].append(result)

        if passed:
            logger.info(f"  ✓ PASS [{exp['name']}] {exp['metric_name']}={metric_value}")
        else:
            logger.error(
                f"  ✗ FAIL [{exp['name']}] {exp['metric_name']}={metric_value}"
            )
            results["overall_status"] = "FAIL"

    # ── Run warning checks ─────────────────────────────────────────────
    logger.info("Running WARNING checks...")
    for exp in suite["warning"]:
        metric_value = exp["metric_fn"](df)
        passed = exp["check"](df)

        result = {
            "name": exp["name"],
            "description": exp["description"],
            "passed": passed,
            "metric_name": exp["metric_name"],
            "metric_value": metric_value,
            "severity": "WARNING",
        }
        results["warning"].append(result)

        if passed:
            logger.info(f"  ✓ PASS [{exp['name']}] {exp['metric_name']}={metric_value}")
        else:
            logger.warning(
                f"  ⚠ WARN [{exp['name']}] {exp['metric_name']}={metric_value}"
            )
            # Warning failures don't change overall_status to FAIL

    # ── Write report ───────────────────────────────────────────────────
    report_path = Path(REPORTS_PATH) / f"dq_silver_orders_{processing_date}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(results, indent=2))
    logger.info(f"DQ report written: {report_path}")

    # ── Summary ────────────────────────────────────────────────────────
    critical_fails = sum(1 for r in results["critical"] if not r["passed"])
    warning_fails = sum(1 for r in results["warning"] if not r["passed"])

    logger.info(
        f"DQ Summary: status={results['overall_status']} | "
        f"critical_fails={critical_fails} | "
        f"warning_fails={warning_fails}"
    )

    spark.stop()
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=(datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
    )
    args = parser.parse_args()

    results = run_dq_suite(args.date)

    # Exit with non-zero code if CRITICAL checks failed
    # This causes the Airflow task to fail and stop the DAG
    if results.get("overall_status") == "FAIL":
        logger.error("DQ CRITICAL FAILURE — pipeline halted.")
        sys.exit(1)

    logger.info("DQ PASSED — pipeline continues.")
    sys.exit(0)


if __name__ == "__main__":
    main()
