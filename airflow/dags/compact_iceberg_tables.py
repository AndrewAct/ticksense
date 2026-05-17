"""compact_iceberg_tables — nightly Spark rewrite_data_files for silver tables."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import DAG

_JARS = "/opt/airflow/jars/iceberg-spark-runtime.jar,/opt/airflow/jars/iceberg-aws-bundle.jar"

_SPARK_CONF = {
    "spark.sql.extensions": ("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "rest",
    "spark.sql.catalog.iceberg.uri": "http://iceberg-rest:8181",
    "spark.sql.catalog.iceberg.warehouse": "s3://ticksense/warehouse",
    "spark.sql.catalog.iceberg.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.iceberg.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.iceberg.s3.path-style-access": "true",
    "spark.sql.catalog.iceberg.s3.access-key-id": "minioadmin",
    "spark.sql.catalog.iceberg.s3.secret-access-key": "minioadmin",
}

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="compact_iceberg_tables",
    start_date=datetime(2026, 5, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["maintenance", "iceberg", "spark"],
    doc_md=(
        "Nightly at 02:00 UTC. Merges small Parquet files written by Flink checkpoints "
        "into larger files for efficient Trino batch scans."
    ),
) as dag:
    SparkSubmitOperator(
        task_id="rewrite_data_files",
        application="/opt/airflow/spark-jobs/compact_tables.py",
        conn_id="spark_default",
        jars=_JARS,
        conf=_SPARK_CONF,
        env_vars={
            "ICEBERG_REST_URI": "http://iceberg-rest:8181",
            "ICEBERG_WAREHOUSE": "s3://ticksense/warehouse",
            "S3_ENDPOINT": "http://minio:9000",
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
        },
    )
