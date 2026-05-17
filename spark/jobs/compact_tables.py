"""
compact_tables.py — Nightly Iceberg rewrite_data_files for all silver tables.

Streaming writers (Flink) produce many small Parquet files per checkpoint.
This job merges them into larger files (~128 MB) so Trino scans are faster.
Safe to rerun — rewrite_data_files is idempotent.

Usage:
    spark-submit compact_tables.py
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def main() -> None:
    from spark_lib.config import TABLES_TO_COMPACT, build_spark_session
    from spark_lib.sql_runner import load_sql

    spark = build_spark_session("ticksense-compact-tables")
    spark.sparkContext.setLogLevel("WARN")

    try:
        for table in TABLES_TO_COMPACT:
            sql = load_sql("compact_table.sql", table=table)
            log.info("Compacting %s ...", table)
            spark.sql(sql).show(truncate=False)
            log.info("Done: %s", table)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
