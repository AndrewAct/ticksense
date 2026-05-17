"""freshness_sla_check — alert if any symbol is stale > 35 seconds."""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import trino
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

from airflow import DAG

STALENESS_THRESHOLD_S = 35

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def check_freshness(**_: object) -> None:
    """Query Trino for any symbol whose latest tick is older than the SLA threshold."""
    with (
        trino.dbapi.connect(
            host=os.getenv("TRINO_HOST", "trino"),
            port=int(os.getenv("TRINO_PORT", "8080")),
            user="airflow",
            catalog="iceberg",
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(
            f"""
                SELECT symbol,
                       DATE_DIFF('second', MAX(exchange_event_ts), NOW()) AS staleness_s
                FROM   normalized.book_ticker
                GROUP  BY symbol
                HAVING DATE_DIFF('second', MAX(exchange_event_ts), NOW()) > {STALENESS_THRESHOLD_S}
                ORDER  BY staleness_s DESC
                """
        )
        stale = cur.fetchall()
    if stale:
        details = ", ".join(f"{row[0]}={row[1]}s" for row in stale)
        raise AirflowFailException(f"SLA breach — stale symbols: {details}")


with DAG(
    dag_id="freshness_sla_check",
    start_date=datetime(2026, 5, 1),
    schedule="*/5 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["sla", "monitoring"],
    doc_md="Runs every 5 min. Fails (alerts) if any symbol has no data within 35 s.",
) as dag:
    PythonOperator(task_id="check_freshness", python_callable=check_freshness)
