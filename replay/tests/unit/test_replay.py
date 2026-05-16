"""Unit tests for the replay package.

Tests cover:
- config.py:  Settings defaults and env overrides
- main.py:    CLI argument parsing
- iceberg_reader.py: offset range logic (with mocked catalog)
- producer.py: ReplayStats dataclass defaults
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from replay.config import Settings
from replay.iceberg_reader import OffsetRange, _ts_to_ms, read_offset_range
from replay.main import _parse_ts, build_parser
from replay.producer import ReplayStats

# ── Settings ──────────────────────────────────────────────────────────────────


class TestSettings:
    def test_defaults(self):
        s = Settings()
        assert s.kafka_bootstrap_servers == "localhost:19092"
        assert s.source_topic == "market.raw.orderbook"
        assert s.target_topic == "market.raw.orderbook"

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
        monkeypatch.setenv("REPLAY_SOURCE_TOPIC", "custom.topic")
        s = Settings()
        assert s.kafka_bootstrap_servers == "broker:9092"
        assert s.source_topic == "custom.topic"


# ── CLI parsing ───────────────────────────────────────────────────────────────


class TestCLI:
    def test_dry_run_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--dry-run"])
        assert args.dry_run is True

    def test_events_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--events", "10000", "--dry-run"])
        assert args.events == 10000

    def test_symbol_and_timestamps(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "--symbol",
                "BTCUSDT",
                "--start-ts",
                "2026-05-15T10:00:00Z",
                "--end-ts",
                "2026-05-15T10:05:00Z",
            ]
        )
        assert args.symbol == "BTCUSDT"
        assert args.start_ts.year == 2026
        assert args.end_ts.minute == 5

    def test_no_args_uses_defaults(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.dry_run is False
        assert args.symbol is None
        assert args.events is None

    def test_parse_ts_iso8601(self):
        ts = _parse_ts("2026-05-15T10:00:00Z")
        assert ts.year == 2026
        assert ts.tzinfo is not None

    def test_parse_ts_epoch(self):
        ts = _parse_ts("1747267200.0")
        assert ts.tzinfo is not None
        assert ts.year >= 2025

    def test_parse_ts_invalid(self):
        with pytest.raises(argparse.ArgumentTypeError):
            _parse_ts("not-a-timestamp")


# ── OffsetRange ───────────────────────────────────────────────────────────────


class TestOffsetRange:
    def test_frozen(self):
        r = OffsetRange(partition=0, start_offset=100, end_offset=200)
        with pytest.raises((AttributeError, TypeError)):
            r.partition = 1  # type: ignore[misc]

    def test_fields(self):
        r = OffsetRange(partition=3, start_offset=500, end_offset=600)
        assert r.partition == 3
        assert r.start_offset == 500
        assert r.end_offset == 600


# ── _ts_to_ms ─────────────────────────────────────────────────────────────────


class TestTsToMs:
    def test_aware_datetime(self):
        ts = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
        ms = _ts_to_ms(ts)
        assert ms == int(ts.timestamp() * 1000)

    def test_naive_datetime_treated_as_utc(self):
        naive = datetime(2026, 5, 15, 10, 0, 0)
        aware = datetime(2026, 5, 15, 10, 0, 0, tzinfo=UTC)
        assert _ts_to_ms(naive) == _ts_to_ms(aware)


# ── iceberg_reader (mocked) ───────────────────────────────────────────────────


class TestIcebergReaderMocked:
    """Test read_offset_range() with a mocked catalog.

    The production path requires a running Iceberg REST catalog + MinIO.
    These tests cover the logic branches (all -1 offsets → empty result,
    valid offsets → OffsetRange list) using mocked PyIceberg objects.
    """

    def _make_mock_arrow(self, partitions: list[int], offsets: list[int]):
        """Return a mock Arrow table with kafka_partition and kafka_offset columns."""
        import pyarrow as pa

        return pa.table(
            {
                "kafka_partition": partitions,
                "kafka_offset": offsets,
            }
        )

    def test_all_minus_one_returns_empty(self):
        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)

        mock_arrow = self._make_mock_arrow([-1, -1, -1], [-1, -1, -1])

        with patch("replay.iceberg_reader._iceberg_catalog") as mock_cat_fn:
            mock_catalog = MagicMock()
            mock_table = MagicMock()
            mock_scan = MagicMock()
            mock_scan.to_arrow.return_value = mock_arrow
            mock_table.scan.return_value = mock_scan
            mock_catalog.load_table.return_value = mock_table
            mock_cat_fn.return_value = mock_catalog

            result = read_offset_range(settings, symbol=None, start_ts=start_ts, end_ts=end_ts)

        assert result == []

    def test_valid_offsets_returns_ranges(self):
        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)

        # 3 records across 2 partitions with real offsets.
        mock_arrow = self._make_mock_arrow([0, 0, 1], [100, 200, 50])

        with patch("replay.iceberg_reader._iceberg_catalog") as mock_cat_fn:
            mock_catalog = MagicMock()
            mock_table = MagicMock()
            mock_scan = MagicMock()
            mock_scan.to_arrow.return_value = mock_arrow
            mock_table.scan.return_value = mock_scan
            mock_catalog.load_table.return_value = mock_table
            mock_cat_fn.return_value = mock_catalog

            result = read_offset_range(settings, symbol="BTCUSDT", start_ts=start_ts, end_ts=end_ts)

        assert len(result) == 2
        by_partition = {r.partition: r for r in result}
        assert by_partition[0].start_offset == 100
        assert by_partition[0].end_offset == 201  # max+1
        assert by_partition[1].start_offset == 50
        assert by_partition[1].end_offset == 51

    def test_catalog_error_returns_empty(self):
        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)

        with patch("replay.iceberg_reader._iceberg_catalog") as mock_cat_fn:
            mock_cat_fn.side_effect = ConnectionError("iceberg unreachable")
            result = read_offset_range(settings, symbol=None, start_ts=start_ts, end_ts=end_ts)

        assert result == []

    def test_empty_table_returns_empty(self):
        import pyarrow as pa

        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)

        empty_arrow = pa.table(
            {
                "kafka_partition": pa.array([], type=pa.int32()),
                "kafka_offset": pa.array([], type=pa.int64()),
            }
        )

        with patch("replay.iceberg_reader._iceberg_catalog") as mock_cat_fn:
            mock_catalog = MagicMock()
            mock_table = MagicMock()
            mock_scan = MagicMock()
            mock_scan.to_arrow.return_value = empty_arrow
            mock_table.scan.return_value = mock_scan
            mock_catalog.load_table.return_value = mock_table
            mock_cat_fn.return_value = mock_catalog

            result = read_offset_range(settings, symbol=None, start_ts=start_ts, end_ts=end_ts)

        assert result == []


# ── ReplayStats defaults ──────────────────────────────────────────────────────


class TestReplayStats:
    def test_initial_counts_are_zero(self):
        start = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end = start + timedelta(hours=1)
        stats = ReplayStats(
            dry_run=True,
            source_topic="market.raw.orderbook",
            target_topic="market.raw.orderbook",
            start_ts=start,
            end_ts=end,
            symbol=None,
        )
        assert stats.events_read == 0
        assert stats.events_produced == 0
        assert stats.partitions_replayed == []
