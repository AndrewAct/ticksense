"""Unit tests for replay/main.py — _main() async entry point."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

from replay.main import _main
from replay.producer import ReplayStats


def _make_stats(**kwargs) -> ReplayStats:
    defaults = dict(
        dry_run=True,
        source_topic="market.raw.orderbook",
        target_topic="market.raw.orderbook",
        start_ts=datetime(2026, 5, 15, 10, 0, tzinfo=UTC),
        end_ts=datetime(2026, 5, 15, 10, 5, tzinfo=UTC),
        symbol=None,
        offset_source="iceberg",
        events_read=42,
        events_produced=0,
        partitions_replayed=[0, 1],
    )
    return ReplayStats(**{**defaults, **kwargs})


def _args(**kwargs) -> argparse.Namespace:
    """Build a Namespace with sensible defaults for _main() testing."""
    defaults = dict(
        symbol=None,
        start_ts=datetime(2026, 5, 15, 10, 0, tzinfo=UTC),
        end_ts=datetime(2026, 5, 15, 10, 5, tzinfo=UTC),
        events=None,
        dry_run=True,
        source_topic=None,
        target_topic=None,
    )
    return argparse.Namespace(**{**defaults, **kwargs})


# ── _main() ───────────────────────────────────────────────────────────────────


class TestMain:
    async def test_returns_zero_on_success(self, capsys):
        stats = _make_stats()
        with patch("replay.main.run_replay", new=AsyncMock(return_value=stats)):
            rc = await _main(_args())
        assert rc == 0

    async def test_prints_summary_table(self, capsys):
        stats = _make_stats(events_read=100, events_produced=0)
        with patch("replay.main.run_replay", new=AsyncMock(return_value=stats)):
            await _main(_args())
        out = capsys.readouterr().out
        assert "Events read" in out
        assert "100" in out
        assert "DRY-RUN" in out

    async def test_live_mode_summary_has_no_dry_run_label(self, capsys):
        stats = _make_stats(dry_run=False, events_produced=5)
        with patch("replay.main.run_replay", new=AsyncMock(return_value=stats)):
            await _main(_args(dry_run=False))
        out = capsys.readouterr().out
        assert "DRY-RUN" not in out
        assert "Replay complete" in out

    async def test_invalid_window_returns_one(self):
        """start_ts >= end_ts must return exit code 1 without calling run_replay."""
        start = datetime(2026, 5, 15, 10, 5, tzinfo=UTC)
        end = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)  # end before start
        with patch("replay.main.run_replay", new=AsyncMock()) as mock_run:
            rc = await _main(_args(start_ts=start, end_ts=end))
        assert rc == 1
        mock_run.assert_not_awaited()

    async def test_equal_timestamps_returns_one(self):
        """start_ts == end_ts is also invalid."""
        ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        with patch("replay.main.run_replay", new=AsyncMock()) as mock_run:
            rc = await _main(_args(start_ts=ts, end_ts=ts))
        assert rc == 1
        mock_run.assert_not_awaited()

    async def test_source_topic_override(self):
        stats = _make_stats(source_topic="custom.source")
        with patch("replay.main.run_replay", new=AsyncMock(return_value=stats)) as mock_run:
            await _main(_args(source_topic="custom.source"))
        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs["settings"].source_topic == "custom.source"

    async def test_target_topic_override(self):
        stats = _make_stats(target_topic="custom.target")
        with patch("replay.main.run_replay", new=AsyncMock(return_value=stats)) as mock_run:
            await _main(_args(target_topic="custom.target"))
        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs["settings"].target_topic == "custom.target"

    async def test_default_timestamps_set_when_none(self, monkeypatch):
        """args.start_ts=None and args.end_ts=None default to (now-1h, now)."""
        fixed_now = datetime(2026, 5, 15, 12, 0, tzinfo=UTC)

        import replay.main as main_mod

        monkeypatch.setattr(
            main_mod,
            "datetime",
            type(
                "FakeDatetime",
                (),
                {
                    "now": staticmethod(lambda tz=None: fixed_now),
                    "fromisoformat": datetime.fromisoformat,
                    "fromtimestamp": datetime.fromtimestamp,
                },
            ),
        )

        stats = _make_stats()
        with patch("replay.main.run_replay", new=AsyncMock(return_value=stats)) as mock_run:
            await _main(_args(start_ts=None, end_ts=None))

        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs["start_ts"] == fixed_now - timedelta(hours=1)
        assert call_kwargs["end_ts"] == fixed_now

    async def test_symbol_passed_to_run_replay(self):
        stats = _make_stats(symbol="BTCUSDT")
        with patch("replay.main.run_replay", new=AsyncMock(return_value=stats)) as mock_run:
            await _main(_args(symbol="BTCUSDT"))
        assert mock_run.call_args.kwargs["symbol"] == "BTCUSDT"

    async def test_max_events_passed_to_run_replay(self):
        stats = _make_stats(events_read=500)
        with patch("replay.main.run_replay", new=AsyncMock(return_value=stats)) as mock_run:
            await _main(_args(events=500))
        assert mock_run.call_args.kwargs["max_events"] == 500

    async def test_dry_run_passed_to_run_replay(self):
        stats = _make_stats(dry_run=True)
        with patch("replay.main.run_replay", new=AsyncMock(return_value=stats)) as mock_run:
            await _main(_args(dry_run=True))
        assert mock_run.call_args.kwargs["dry_run"] is True
