"""run_dbt_models — hourly dbt run + test against Iceberg silver tables."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

_DBT = "dbt --no-send-anonymous-usage-stats"
_OPTS = "--profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="run_dbt_models",
    start_date=datetime(2026, 5, 1),
    schedule="0 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["dbt", "analytics"],
    doc_md="Hourly dbt run + test. Rebuilds staging views and mart tables over Iceberg.",
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{_DBT} run {_OPTS}",
        env={"TRINO_HOST": "trino", "TRINO_PORT": "8080"},
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{_DBT} test {_OPTS}",
        env={"TRINO_HOST": "trino", "TRINO_PORT": "8080"},
    )

    dbt_run >> dbt_test
