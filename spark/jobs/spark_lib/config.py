"""Shared SparkSession builder and Iceberg catalog configuration."""

from __future__ import annotations

import os

TABLES_TO_COMPACT: list[str] = [
    "normalized.book_ticker",
    "normalized.ohlcv_1m",
    "normalized.symbol_config",
]


def build_spark_session(app_name: str):  # type: ignore[return]
    """
    Build a SparkSession pointing at the Iceberg REST catalog on MinIO.
    All coordinates are read from environment variables so the same job runs
    locally (local[*]) and on the Spark standalone cluster.
    """
    from pyspark import SparkConf
    from pyspark.sql import SparkSession

    iceberg_conf: dict[str, str] = {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "rest",
        "spark.sql.catalog.iceberg.uri": os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181"),
        "spark.sql.catalog.iceberg.warehouse": os.getenv(
            "ICEBERG_WAREHOUSE", "s3://ticksense/warehouse"
        ),
        "spark.sql.catalog.iceberg.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.iceberg.s3.endpoint": os.getenv("S3_ENDPOINT", "http://minio:9000"),
        "spark.sql.catalog.iceberg.s3.path-style-access": "true",
        "spark.sql.catalog.iceberg.s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        "spark.sql.catalog.iceberg.s3.secret-access-key": os.getenv(
            "AWS_SECRET_ACCESS_KEY", "minioadmin"
        ),
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
    }

    master = os.getenv("SPARK_MASTER_URL", "local[*]")
    conf = SparkConf().setMaster(master).setAppName(app_name).setAll(list(iceberg_conf.items()))
    return SparkSession.builder.config(conf=conf).getOrCreate()
