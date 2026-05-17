"""run_great_expectations — data quality checks on Iceberg silver tables."""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import pandas as pd
import trino
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

from airflow import DAG

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# Expectations applied to every table (column → (min, max|None))
_PRICE_COLS = {"best_bid_price": (0, None), "best_ask_price": (0, None)}
_REQUIRED_COLS = ["exchange", "symbol", "exchange_event_ts"]


def _trino_conn() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user="airflow",
        catalog="iceberg",
    )


def _fetch_sample(table: str, minutes: int = 10) -> pd.DataFrame:
    """Fetch a recent sample from the given Iceberg table via Trino."""
    with _trino_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
                SELECT *
                FROM   iceberg.{table}
                WHERE  exchange_event_ts > NOW() - INTERVAL '{minutes}' MINUTE
                LIMIT  2000
                """
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def _run_suite(df: pd.DataFrame, table: str) -> list[str]:
    """Run GX expectations on a pandas DataFrame. Returns list of failure messages."""
    import great_expectations as gx

    ctx = gx.get_context(mode="ephemeral")
    ds = ctx.sources.add_pandas(name="trino_sample")
    asset = ds.add_dataframe_asset(name=table)
    batch_req = asset.build_batch_request(dataframe=df)

    ctx.add_or_update_expectation_suite(f"{table}_suite")
    validator = ctx.get_validator(batch_request=batch_req, expectation_suite_name=f"{table}_suite")

    for col in _REQUIRED_COLS:
        if col in df.columns:
            validator.expect_column_values_to_not_be_null(col)

    for col, (lo, hi) in _PRICE_COLS.items():
        if col in df.columns:
            validator.expect_column_values_to_be_between(col, min_value=lo, max_value=hi)

    results = validator.validate()
    return [
        r["expectation_config"]["expectation_type"] for r in results.results if not r["success"]
    ]


def run_gx_checks(**_: object) -> None:
    """Validate recent data in book_ticker and ohlcv_1m; fail if any expectation fails."""
    failures: dict[str, list[str]] = {}

    for table in ("normalized.book_ticker", "normalized.ohlcv_1m"):
        df = _fetch_sample(table)
        if df.empty:
            failures[table] = ["no_recent_data"]
            continue
        failed = _run_suite(df, table)
        if failed:
            failures[table] = failed

    if failures:
        raise AirflowFailException(f"GX validation failed: {failures}")


with DAG(
    dag_id="run_great_expectations",
    start_date=datetime(2026, 5, 1),
    schedule="30 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["quality", "great-expectations"],
    doc_md=(
        "Runs at :30 each hour. Fetches a recent sample from Iceberg via Trino "
        "and validates null constraints and price bounds with Great Expectations."
    ),
) as dag:
    PythonOperator(task_id="gx_checks", python_callable=run_gx_checks)
