"""backfill_ohlcv_1m — manually triggered Spark OHLCV backfill for a date range."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
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
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
}

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def validate_params(**context: object) -> None:
    """Fail fast if date params are missing or malformed before Spark starts."""
    params = context["params"]
    start = params.get("start_date")
    end = params.get("end_date")
    if not start or not end:
        from airflow.exceptions import AirflowFailException

        raise AirflowFailException("Required params: start_date and end_date (YYYY-MM-DD)")
    from datetime import date

    s, e = date.fromisoformat(start), date.fromisoformat(end)
    if e <= s:
        from airflow.exceptions import AirflowFailException

        raise AirflowFailException(f"end_date ({e}) must be after start_date ({s})")


with DAG(
    dag_id="backfill_ohlcv_1m",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    params={"start_date": "2026-05-01", "end_date": "2026-05-02"},
    tags=["backfill", "spark", "ohlcv"],
    doc_md=(
        "Manually triggered. Recomputes normalized.ohlcv_1m from book_ticker "
        "for the given date range. Idempotent — reruns overwrite the same partitions."
    ),
) as dag:
    check = PythonOperator(task_id="validate_params", python_callable=validate_params)

    backfill = SparkSubmitOperator(
        task_id="spark_backfill_ohlcv",
        application="/opt/airflow/spark-jobs/backfill_ohlcv.py",
        application_args=[
            "--start-date",
            "{{ params.start_date }}",
            "--end-date",
            "{{ params.end_date }}",
        ],
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

    check >> backfill
