from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "ROIP-Streaming", shuffle_partitions: int = 8
) -> SparkSession:
    """
    Returns a configured SparkSession.

    shuffle_partitions=8 is appropriate for local development.
    In production on a 10-node cluster you'd set this to 200-400.
    This is a common interview discussion point.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")  # use all CPU cores locally
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                    "io.delta:delta-spark_2.12:3.1.0",
                ]
            ),
        )
        # Delta Lake configuration
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Performance
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")  # AQE enabled
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Streaming
        .config("spark.sql.streaming.checkpointLocation", "/tmp/roip/checkpoints")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # Logging — reduce noise during development
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
