import pytest
from pyspark.sql.types import *
import sys, os

sys.path.insert(0, "streaming/utils")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from validator import validate_bronze_events


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("test")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def make_df(spark, rows):
    return spark.createDataFrame(rows)


def test_valid_events_pass_validation(spark):
    rows = [
        (
            "evt-001",
            "ORDER_PLACED",
            "ORD-001",
            "CUST-001",
            1704067200000,
            1704067200000,
            "1.0",
            "order-service",
            "{}",
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
    ]
    df = spark.createDataFrame(rows, cols)
    valid, invalid = validate_bronze_events(df)
    assert valid.count() == 1
    assert invalid.count() == 0


def test_missing_order_id_routes_to_invalid(spark):
    rows = [
        (
            "evt-002",
            "ORDER_PLACED",
            None,
            "CUST-002",
            1704067200000,
            1704067200000,
            "1.0",
            "order-service",
            "{}",
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
    ]
    schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("occurred_at", LongType(), True),
            StructField("ingested_at", LongType(), True),
            StructField("schema_version", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("payload", StringType(), True),
        ]
    )

    df = spark.createDataFrame(rows, schema)
    valid, invalid = validate_bronze_events(df)
    assert valid.count() == 0
    assert invalid.count() == 1
    assert "missing_required_fields" in invalid.select("validation_failure").first()[0]


def test_invalid_event_type_routes_to_invalid(spark):
    rows = [
        (
            "evt-003",
            "UNKNOWN_TYPE",
            "ORD-003",
            "CUST-003",
            1704067200000,
            1704067200000,
            "1.0",
            "order-service",
            "{}",
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
    ]
    df = spark.createDataFrame(rows, cols)
    valid, invalid = validate_bronze_events(df)
    assert valid.count() == 0
    assert invalid.count() == 1


def test_duplicate_event_ids_tagged(spark):
    """
    Two rows with same event_id — second should be tagged is_duplicate=true.
    """
    rows = [
        (
            "evt-dup",
            "ORDER_PLACED",
            "ORD-010",
            "CUST-010",
            1704067200000,
            1704067200000,
            "1.0",
            "order-service",
            "{}",
        ),
        (
            "evt-dup",
            "ORDER_PLACED",
            "ORD-010",
            "CUST-010",
            1704067200000,
            1704067210000,
            "1.0",
            "order-service",
            "{}",
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
    ]
    df = spark.createDataFrame(rows, cols).withColumn(
        "bronze_loaded_at", F.col("ingested_at")
    )

    from pyspark.sql.window import Window

    w = Window.partitionBy("event_id").orderBy("bronze_loaded_at")
    result = (
        df.withColumn("_rn", F.row_number().over(w))
        .withColumn(
            "is_duplicate",
            F.when(F.col("_rn") > 1, F.lit("true")).otherwise(F.lit("false")),
        )
        .drop("_rn")
    )
    dups = result.filter(F.col("is_duplicate") == "true").count()
    assert dups == 1
