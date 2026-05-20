from pyspark.sql import DataFrame
from pyspark.sql import functions as F


REQUIRED_FIELDS   = ["event_id", "event_type", "order_id", "customer_id"]
VALID_EVENT_TYPES = [
    "ORDER_PLACED", "ORDER_CONFIRMED", "ORDER_CANCELLED",
    "PAYMENT_INITIATED", "PAYMENT_SUCCESS", "PAYMENT_FAILED",
    "ORDER_SHIPPED", "ORDER_DELIVERED",
]


def validate_bronze_events(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Split a DataFrame into (valid_df, invalid_df).

    Invalid events go to the DLQ path.
    We tag events rather than filter immediately so we have lineage.
    """
    # Check required fields are not null or empty
    null_checks = [
        F.col(field).isNotNull() & (F.col(field) != "")
        for field in REQUIRED_FIELDS
    ]
    all_valid = null_checks[0]
    for check in null_checks[1:]:
        all_valid = all_valid & check

    # Check event_type is in allowed set
    valid_type = F.col("event_type").isin(VALID_EVENT_TYPES)

    is_valid = all_valid & valid_type

    valid_df   = df.filter(is_valid)
    invalid_df = df.filter(~is_valid).withColumn(
        "validation_failure",
        F.when(~all_valid, F.lit("missing_required_fields"))
         .when(~valid_type, F.lit("invalid_event_type"))
         .otherwise(F.lit("unknown"))
    )

    return valid_df, invalid_df