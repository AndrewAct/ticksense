"""Unit tests for Spark job logic — no SparkSession required."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

# ── lib.sql_runner ────────────────────────────────────────────────────────────


def test_load_sql_substitutes_params() -> None:
    from spark_lib.sql_runner import load_sql

    sql = load_sql(
        "backfill_ohlcv.sql", start_ts="2026-05-01T00:00:00", end_ts="2026-05-02T00:00:00"
    )
    assert "2026-05-01T00:00:00" in sql
    assert "2026-05-02T00:00:00" in sql
    assert "{start_ts}" not in sql
    assert "{end_ts}" not in sql


def test_load_sql_no_params_returns_raw() -> None:
    from spark_lib.sql_runner import load_sql

    sql = load_sql("compact_table.sql")
    assert "{table}" in sql  # unsubstituted — caller must pass param


def test_sql_files_exist() -> None:
    sql_dir = Path(__file__).parent.parent / "jobs" / "sql"
    assert (sql_dir / "backfill_ohlcv.sql").exists()
    assert (sql_dir / "compact_table.sql").exists()


# ── lib.config ────────────────────────────────────────────────────────────────


def test_tables_to_compact_nonempty() -> None:
    from spark_lib.config import TABLES_TO_COMPACT

    assert len(TABLES_TO_COMPACT) > 0
    assert all("." in t for t in TABLES_TO_COMPACT), "each entry must be schema.table"


# ── backfill_ohlcv.parse_args ─────────────────────────────────────────────────


def test_parse_args_valid() -> None:
    from backfill_ohlcv import parse_args

    args = parse_args(["--start-date", "2026-05-01", "--end-date", "2026-05-02"])
    assert args.start_date == date(2026, 5, 1)
    assert args.end_date == date(2026, 5, 2)


def test_parse_args_missing_required(monkeypatch: pytest.MonkeyPatch) -> None:
    from backfill_ohlcv import parse_args

    with pytest.raises(SystemExit):
        parse_args(["--start-date", "2026-05-01"])  # end-date missing


# ── backfill_ohlcv.validate_date_range ───────────────────────────────────────


def test_validate_date_range_valid() -> None:
    from backfill_ohlcv import validate_date_range

    validate_date_range(date(2026, 5, 1), date(2026, 5, 2))  # no exception


def test_validate_date_range_end_before_start() -> None:
    from backfill_ohlcv import validate_date_range

    with pytest.raises(ValueError, match="must be after"):
        validate_date_range(date(2026, 5, 2), date(2026, 5, 1))


def test_validate_date_range_same_day() -> None:
    from backfill_ohlcv import validate_date_range

    with pytest.raises(ValueError, match="must be after"):
        validate_date_range(date(2026, 5, 1), date(2026, 5, 1))


def test_validate_date_range_exceeds_limit() -> None:
    from backfill_ohlcv import validate_date_range

    with pytest.raises(ValueError, match="90-day"):
        validate_date_range(date(2026, 1, 1), date(2026, 1, 1) + timedelta(days=91))
