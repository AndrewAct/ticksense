"""
backfill_ohlcv.py — Recompute normalized.ohlcv_1m for a date range.

Reads from normalized.book_ticker (silver), aggregates to 1-minute OHLCV,
and writes with dynamic partition overwrite — identical reruns are idempotent.

Usage (via spark-submit or Airflow SparkSubmitOperator):
    spark-submit backfill_ohlcv.py --start-date 2026-05-01 --end-date 2026-05-02
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

log = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill 1-minute OHLCV from book_ticker")
    p.add_argument(
        "--start-date",
        required=True,
        type=date.fromisoformat,
        help="Inclusive start date (YYYY-MM-DD)",
    )
    p.add_argument(
        "--end-date", required=True, type=date.fromisoformat, help="Exclusive end date (YYYY-MM-DD)"
    )
    return p.parse_args(argv)


def validate_date_range(start: date, end: date) -> None:
    if end <= start:
        raise ValueError(f"end_date ({end}) must be after start_date ({start})")
    if (end - start) > timedelta(days=90):
        raise ValueError("Range exceeds 90-day safety limit; split into smaller chunks")


def main() -> None:
    from spark_lib.config import build_spark_session
    from spark_lib.sql_runner import load_sql

    args = parse_args()
    validate_date_range(args.start_date, args.end_date)

    spark = build_spark_session("ticksense-backfill-ohlcv")
    spark.sparkContext.setLogLevel("WARN")

    sql = load_sql(
        "backfill_ohlcv.sql",
        start_ts=f"{args.start_date}T00:00:00",
        end_ts=f"{args.end_date}T00:00:00",
    )

    try:
        df = spark.sql(sql)
        df.writeTo("iceberg.normalized.ohlcv_1m").overwritePartitions()
        log.info("Backfill complete: %s → %s", args.start_date, args.end_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
