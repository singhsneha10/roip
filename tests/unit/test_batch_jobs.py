import pytest
import sys, os

sys.path.insert(0, "streaming/utils")
sys.path.insert(0, "batch/jobs")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("test-batch")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def make_silver_df(spark, rows, cols):
    return spark.createDataFrame(rows, cols)


SILVER_COLS = [
    "event_id",
    "event_type",
    "order_id",
    "customer_id",
    "event_timestamp",
    "event_date",
    "event_hour",
    "city",
    "category",
    "amount",
    "item_count",
    "discount_pct",
    "device",
    "pincode",
    "silver_loaded_at",
    "processing_date",
]


def test_hourly_orders_aggregation(spark):
    from silver_to_gold import build_hourly_orders
    from datetime import datetime

    rows = [
        (
            "e1",
            "ORDER_PLACED",
            "o1",
            "c1",
            datetime(2024, 1, 15, 14, 0),
            "2024-01-15",
            14,
            "Mumbai",
            "electronics",
            1000.0,
            2,
            10.0,
            "android",
            "400001",
            datetime.utcnow(),
            "2024-01-15",
        ),
        (
            "e2",
            "ORDER_PLACED",
            "o2",
            "c2",
            datetime(2024, 1, 15, 14, 30),
            "2024-01-15",
            14,
            "Mumbai",
            "electronics",
            2000.0,
            1,
            0.0,
            "ios",
            "400002",
            datetime.utcnow(),
            "2024-01-15",
        ),
    ]
    df = spark.createDataFrame(rows, SILVER_COLS)
    result = build_hourly_orders(df, "2024-01-15")

    assert result.count() == 1  # both orders in same hour+city+category
    row = result.first()
    assert row["order_count"] == 2
    assert row["gross_revenue"] == 3000.0
    assert row["city"] == "Mumbai"


def test_payment_health_groups_correctly(spark):
    from silver_to_gold import build_payment_health
    from datetime import datetime

    rows = [
        (
            "e3",
            "PAYMENT_SUCCESS",
            "o3",
            "c3",
            datetime(2024, 1, 15, 10, 0),
            "2024-01-15",
            10,
            "Delhi",
            "grocery",
            500.0,
            1,
            0.0,
            "web",
            "110001",
            datetime.utcnow(),
            "2024-01-15",
        ),
        (
            "e4",
            "PAYMENT_FAILED",
            "o4",
            "c4",
            datetime(2024, 1, 15, 11, 0),
            "2024-01-15",
            11,
            "Delhi",
            "grocery",
            300.0,
            1,
            0.0,
            "android",
            "110002",
            datetime.utcnow(),
            "2024-01-15",
        ),
        (
            "e5",
            "PAYMENT_FAILED",
            "o5",
            "c5",
            datetime(2024, 1, 15, 12, 0),
            "2024-01-15",
            12,
            "Delhi",
            "grocery",
            200.0,
            1,
            0.0,
            "ios",
            "110003",
            datetime.utcnow(),
            "2024-01-15",
        ),
    ]
    df = spark.createDataFrame(rows, SILVER_COLS)
    result = build_payment_health(df, "2024-01-15")

    event_counts = {row["event_type"]: row["event_count"] for row in result.collect()}
    assert event_counts["PAYMENT_SUCCESS"] == 1
    assert event_counts["PAYMENT_FAILED"] == 2


def test_reconciliation_pass(spark):
    from batch.reconciliation.daily_reconciliation import TOLERANCE_PCT

    bronze_count = 1000
    silver_count = 998
    variance_pct = abs(bronze_count - silver_count) / bronze_count * 100
    assert variance_pct < TOLERANCE_PCT  # 0.2% < 1.0% → PASS


def test_reconciliation_fail():
    from batch.reconciliation.daily_reconciliation import TOLERANCE_PCT

    bronze_count = 1000
    silver_count = 900
    variance_pct = abs(bronze_count - silver_count) / bronze_count * 100
    assert variance_pct > TOLERANCE_PCT  # 10% > 1.0% → FAIL
