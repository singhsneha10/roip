from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, MapType, TimestampType
)

# Raw Kafka message schema
# Kafka gives us: key, value, topic, partition, offset, timestamp
KAFKA_METADATA_SCHEMA = StructType([
    StructField("key",            StringType(),  True),
    StructField("value",          StringType(),  True),
    StructField("topic",          StringType(),  True),
    StructField("partition",      StringType(),  True),
    StructField("offset",         StringType(),  True),
    StructField("kafka_timestamp",LongType(),    True),
])

# The shape of an OrderEvent JSON payload
ORDER_EVENT_SCHEMA = StructType([
    StructField("event_id",       StringType(),  False),
    StructField("event_type",     StringType(),  False),
    StructField("order_id",       StringType(),  True),
    StructField("customer_id",    StringType(),  True),
    StructField("occurred_at",    LongType(),    True),   # epoch millis
    StructField("ingested_at",    LongType(),    True),
    StructField("schema_version", StringType(),  True),
    StructField("source_system",  StringType(),  True),
    StructField("payload",        MapType(StringType(), StringType()), True),
])

# Bronze table schema — raw + pipeline metadata columns added
BRONZE_SCHEMA = StructType([
    StructField("event_id",         StringType(),   False),
    StructField("event_type",       StringType(),   False),
    StructField("order_id",         StringType(),   True),
    StructField("customer_id",      StringType(),   True),
    StructField("occurred_at",      LongType(),     True),
    StructField("ingested_at",      LongType(),     True),
    StructField("schema_version",   StringType(),   True),
    StructField("source_system",    StringType(),   True),
    StructField("payload",          MapType(StringType(), StringType()), True),
    # Pipeline metadata — added by the streaming job
    StructField("kafka_partition",  StringType(),   True),
    StructField("kafka_offset",     StringType(),   True),
    StructField("pipeline_version", StringType(),   True),
    StructField("bronze_loaded_at", LongType(),     True),
    StructField("is_duplicate",     StringType(),   True),  # "true"/"false"
    StructField("ingestion_date",   StringType(),   True),  # partition column
])