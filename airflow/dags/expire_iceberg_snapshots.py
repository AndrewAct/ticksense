"""expire_iceberg_snapshots — retain last 7 days of Iceberg snapshots."""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import trino
from airflow.operators.python import PythonOperator

from airflow import DAG

RETENTION_DAYS = 7

TABLES = [
    "normalized.book_ticker",
    "normalized.ohlcv_1m",
    "normalized.symbol_config",
]

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def expire_snapshots(**_: object) -> None:
    """Run Trino expire_snapshots procedure for each silver table."""
    with (
        trino.dbapi.connect(
            host=os.getenv("TRINO_HOST", "trino"),
            port=int(os.getenv("TRINO_PORT", "8080")),
            user="airflow",
            catalog="iceberg",
        ) as conn,
        conn.cursor() as cur,
    ):
        for table in TABLES:
            cur.execute(
                f"ALTER TABLE iceberg.{table} "
                f"EXECUTE expire_snapshots(retention_threshold => '{RETENTION_DAYS}d')"
            )
            cur.fetchall()


with DAG(
    dag_id="expire_iceberg_snapshots",
    start_date=datetime(2026, 5, 1),
    schedule="0 3 * * *",
    catchup=False,
    default_args=default_args,
    tags=["maintenance", "iceberg"],
    doc_md=f"Nightly at 03:00 UTC. Removes Iceberg snapshots older than {RETENTION_DAYS} days.",
) as dag:
    PythonOperator(task_id="expire_snapshots", python_callable=expire_snapshots)
