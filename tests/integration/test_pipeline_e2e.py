"""
Integration test: simulates one micro-batch flowing through
Bronze validation → Silver transformation → DQ checks.

Uses local Spark with tiny synthetic data — no Kafka needed.
Runs in CI in about 60-90 seconds.
"""

import pytest
import sys
import os
from datetime import datetime

sys.path.insert(0, "streaming/utils")
sys.path.insert(0, "batch/jobs")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("roip-integration-test")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_bronze_df(spark):
    """Minimal Bronze-schema DataFrame for testing."""
    rows = [
        (
            "e001",
            "ORDER_PLACED",
            "ORD-001",
            "CUST-001",
            1704067200000,
            1704067200000,
            "1.0",
            "order-service",
            {
                "city": "Mumbai",
                "category": "electronics",
                "amount": "1500.0",
                "item_count": "2",
                "discount_pct": "10",
                "device": "android",
                "pincode": "400001",
            },
            "p1",
            "0",
            "1.0",
            1704067200000,
            "false",
            "2024-01-01",
        ),
        (
            "e002",
            "PAYMENT_SUCCESS",
            "ORD-001",
            "CUST-001",
            1704067260000,
            1704067260000,
            "1.0",
            "payment-service",
            {
                "city": "Mumbai",
                "category": "electronics",
                "amount": "1500.0",
                "item_count": "2",
                "discount_pct": "10",
                "device": "android",
                "pincode": "400001",
            },
            "p1",
            "1",
            "1.0",
            1704067260000,
            "false",
            "2024-01-01",
        ),
        (
            "e003",
            "ORDER_DELIVERED",
            "ORD-001",
            "CUST-001",
            1704153600000,
            1704153600000,
            "1.0",
            "delivery-service",
            {
                "city": "Mumbai",
                "category": "electronics",
                "amount": "1500.0",
                "item_count": "2",
                "discount_pct": "10",
                "device": "android",
                "pincode": "400001",
            },
            "p1",
            "2",
            "1.0",
            1704153600000,
            "false",
            "2024-01-01",
        ),
    ]
    cols = [
        "event_id",
        "event_type",
        "order_id",
        "customer_id",
        "occurred_at",
        "ingested_at",
        "schema_version",
        "source_system",
        "payload",
        "kafka_partition",
        "kafka_offset",
        "pipeline_version",
        "bronze_loaded_at",
        "is_duplicate",
        "ingestion_date",
    ]
    return spark.createDataFrame(rows, cols)


def test_cast_and_extract_produces_correct_types(spark, sample_bronze_df):
    from bronze_to_silver import cast_and_extract

    result = cast_and_extract(sample_bronze_df, "2026-05-21")

    assert "event_timestamp" in result.columns
    assert "event_date" in result.columns
    assert "city" in result.columns
    assert "amount" in result.columns

    row = result.filter(F.col("event_id") == "e001").first()
    assert row["city"] == "Mumbai"
    assert row["amount"] == 1500.0
    assert row["device"] == "android"


def test_silver_dq_passes_on_clean_data(spark, sample_bronze_df):
    from bronze_to_silver import cast_and_extract, run_silver_dq

    silver_df = cast_and_extract(sample_bronze_df, "2026-05-21")
    _, metrics = run_silver_dq(silver_df, "2024-01-01", spark)
    assert metrics["dq_passed"] is True
    assert metrics["null_customer_id_count"] == 0


def test_hourly_orders_count(spark, sample_bronze_df):
    from bronze_to_silver import cast_and_extract
    from silver_to_gold import build_hourly_orders

    silver_df = cast_and_extract(sample_bronze_df, "2026-05-21")
    gold_df = build_hourly_orders(silver_df, "2024-01-01")

    # Only ORDER_PLACED events count as orders
    order_count = gold_df.agg(F.sum("order_count")).first()[0]
    assert order_count == 1  # only e001 is ORDER_PLACED


def test_payment_health_counts(spark, sample_bronze_df):
    from bronze_to_silver import cast_and_extract
    from silver_to_gold import build_payment_health

    silver_df = cast_and_extract(sample_bronze_df, "2026-05-21")
    payment_df = build_payment_health(silver_df, "2024-01-01")

    success = payment_df.filter(F.col("event_type") == "PAYMENT_SUCCESS").first()
    assert success is not None
    assert success["event_count"] == 1
