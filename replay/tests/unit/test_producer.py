"""Unit tests for replay/producer.py.

All Kafka I/O is mocked — no running broker required.

Async iterator helper
---------------------
AIOKafkaConsumer supports `async for record in consumer`.
We mock this by replacing __aiter__ with an async generator that yields
pre-built MockRecord objects and then stops.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from replay.config import Settings
from replay.iceberg_reader import OffsetRange
from replay.producer import (
    ReplayStats,
    _kafka_time_offset_ranges,
    _partitions_for_topic,
    _replay_partition,
    run_replay,
)

# ── Test helpers ──────────────────────────────────────────────────────────────


@dataclass
class MockRecord:
    offset: int
    value: bytes = b'{"symbol":"BTCUSDT"}'
    key: bytes | None = b"binance#BTCUSDT"
    partition: int = 0
    timestamp: int = 0


@dataclass
class MockOffsetAndTimestamp:
    """Minimal stand-in for aiokafka's OffsetAndTimestamp namedtuple."""

    offset: int
    timestamp: int = 0


async def _async_stream(*records: MockRecord):
    """Async generator that yields records then stops — used to mock consumer iteration."""
    for r in records:
        yield r


def _make_consumer(
    *,
    partitions: set[int] | None = None,
    offsets_side_effect: list | None = None,
    records: list[MockRecord] | None = None,
) -> AsyncMock:
    """Build a fully mocked AIOKafkaConsumer."""
    mock = AsyncMock()
    mock.assign = MagicMock()
    mock.seek = MagicMock()

    if partitions is not None:
        mock.partitions_for = AsyncMock(return_value=partitions)

    if offsets_side_effect is not None:
        mock.offsets_for_times = AsyncMock(side_effect=offsets_side_effect)

    if records is not None:
        gen = _async_stream(*records)
        mock.__aiter__ = MagicMock(return_value=gen)

    return mock


# ── _partitions_for_topic ─────────────────────────────────────────────────────


class TestPartitionsForTopic:
    async def test_returns_sorted_list(self):
        settings = Settings()
        mock_consumer = _make_consumer(partitions={2, 0, 1})

        with patch("replay.producer.AIOKafkaConsumer", return_value=mock_consumer):
            result = await _partitions_for_topic(settings, "test-topic")

        assert result == [0, 1, 2]
        mock_consumer.start.assert_awaited_once()
        mock_consumer.stop.assert_awaited_once()

    async def test_returns_empty_when_none(self):
        settings = Settings()
        mock_consumer = _make_consumer()
        mock_consumer.partitions_for = AsyncMock(return_value=None)

        with patch("replay.producer.AIOKafkaConsumer", return_value=mock_consumer):
            result = await _partitions_for_topic(settings, "test-topic")

        assert result == []

    async def test_stop_called_on_exception(self):
        settings = Settings()
        mock_consumer = _make_consumer()
        mock_consumer.partitions_for = AsyncMock(side_effect=RuntimeError("broker down"))

        with (
            patch("replay.producer.AIOKafkaConsumer", return_value=mock_consumer),
            pytest.raises(RuntimeError),
        ):
            await _partitions_for_topic(settings, "test-topic")

        mock_consumer.stop.assert_awaited_once()


# ── _kafka_time_offset_ranges ─────────────────────────────────────────────────


class TestKafkaTimeOffsetRanges:
    """Mock both _partitions_for_topic (patch) and the offsets consumer."""

    def _tp(self, topic: str, partition: int):
        from aiokafka.structs import TopicPartition

        return TopicPartition(topic, partition)

    async def test_normal_two_partitions(self):
        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)

        tp0 = self._tp(settings.source_topic, 0)
        tp1 = self._tp(settings.source_topic, 1)

        offsets_responses = [
            # offsets_for_times(start_map)
            {tp0: MockOffsetAndTimestamp(100), tp1: MockOffsetAndTimestamp(50)},
            # offsets_for_times(end_map)
            {tp0: MockOffsetAndTimestamp(200), tp1: MockOffsetAndTimestamp(80)},
        ]
        mock_consumer = _make_consumer(offsets_side_effect=offsets_responses)

        with (
            patch("replay.producer._partitions_for_topic", return_value=[0, 1]),
            patch("replay.producer.AIOKafkaConsumer", return_value=mock_consumer),
        ):
            result = await _kafka_time_offset_ranges(
                settings, settings.source_topic, start_ts, end_ts
            )

        assert len(result) == 2
        by_part = {r.partition: r for r in result}
        assert by_part[0] == OffsetRange(0, 100, 200)
        assert by_part[1] == OffsetRange(1, 50, 80)

    async def test_no_partitions_returns_empty(self):
        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)

        with patch("replay.producer._partitions_for_topic", return_value=[]):
            result = await _kafka_time_offset_ranges(
                settings, settings.source_topic, start_ts, end_ts
            )

        assert result == []

    async def test_partition_skipped_when_start_result_none(self):
        """Partition with no data at start_ts is excluded."""
        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)

        tp0 = self._tp(settings.source_topic, 0)
        tp1 = self._tp(settings.source_topic, 1)

        offsets_responses = [
            {tp0: None, tp1: MockOffsetAndTimestamp(50)},  # tp0 has no data
            {tp0: None, tp1: MockOffsetAndTimestamp(80)},
        ]
        mock_consumer = _make_consumer(offsets_side_effect=offsets_responses)

        with (
            patch("replay.producer._partitions_for_topic", return_value=[0, 1]),
            patch("replay.producer.AIOKafkaConsumer", return_value=mock_consumer),
        ):
            result = await _kafka_time_offset_ranges(
                settings, settings.source_topic, start_ts, end_ts
            )

        assert len(result) == 1
        assert result[0].partition == 1

    async def test_end_result_none_uses_max_offset(self):
        """When no records exist at end_ts, end_offset is set to max int."""
        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)

        tp0 = self._tp(settings.source_topic, 0)

        offsets_responses = [
            {tp0: MockOffsetAndTimestamp(100)},
            {tp0: None},  # no records at end_ts → use 2**63-1
        ]
        mock_consumer = _make_consumer(offsets_side_effect=offsets_responses)

        with (
            patch("replay.producer._partitions_for_topic", return_value=[0]),
            patch("replay.producer.AIOKafkaConsumer", return_value=mock_consumer),
        ):
            result = await _kafka_time_offset_ranges(
                settings, settings.source_topic, start_ts, end_ts
            )

        assert len(result) == 1
        assert result[0].end_offset == 2**63 - 1

    async def test_empty_window_partition_skipped(self):
        """Partition where start_offset >= end_offset is excluded."""
        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)

        tp0 = self._tp(settings.source_topic, 0)

        offsets_responses = [
            {tp0: MockOffsetAndTimestamp(200)},
            {tp0: MockOffsetAndTimestamp(100)},  # end < start → empty window
        ]
        mock_consumer = _make_consumer(offsets_side_effect=offsets_responses)

        with (
            patch("replay.producer._partitions_for_topic", return_value=[0]),
            patch("replay.producer.AIOKafkaConsumer", return_value=mock_consumer),
        ):
            result = await _kafka_time_offset_ranges(
                settings, settings.source_topic, start_ts, end_ts
            )

        assert result == []

    async def test_naive_timestamps_treated_as_utc(self):
        """Naive datetimes are converted to UTC before computing epoch ms."""
        settings = Settings()
        start_naive = datetime(2026, 5, 15, 10, 0)  # no tzinfo
        end_naive = datetime(2026, 5, 15, 10, 5)

        mock_consumer = _make_consumer(offsets_side_effect=[{}, {}])

        with (
            patch("replay.producer._partitions_for_topic", return_value=[0]),
            patch("replay.producer.AIOKafkaConsumer", return_value=mock_consumer),
        ):
            # Should not raise; naive datetimes are handled gracefully
            result = await _kafka_time_offset_ranges(
                settings, settings.source_topic, start_naive, end_naive
            )

        assert result == []


# ── _replay_partition ─────────────────────────────────────────────────────────


class TestReplayPartition:
    async def test_dry_run_counts_but_does_not_produce(self):
        settings = Settings()
        offset_range = OffsetRange(partition=0, start_offset=0, end_offset=3)
        records = [
            MockRecord(offset=0),
            MockRecord(offset=1),
            MockRecord(offset=2),
        ]
        mock_consumer = _make_consumer(records=records)

        with patch("replay.producer.AIOKafkaConsumer", return_value=mock_consumer):
            read, produced = await _replay_partition(
                settings=settings,
                offset_range=offset_range,
                symbol_filter=None,
                max_events=None,
                dry_run=True,
                producer=None,
            )

        assert read == 3
        assert produced == 0

    async def test_live_mode_produces_records(self):
        settings = Settings()
        offset_range = OffsetRange(partition=0, start_offset=0, end_offset=3)
        records = [MockRecord(offset=0), MockRecord(offset=1), MockRecord(offset=2)]
        mock_consumer = _make_consumer(records=records)
        mock_producer = AsyncMock()

        with patch("replay.producer.AIOKafkaConsumer", return_value=mock_consumer):
            read, produced = await _replay_partition(
                settings=settings,
                offset_range=offset_range,
                symbol_filter=None,
                max_events=None,
                dry_run=False,
                producer=mock_producer,
            )

        assert read == 3
        assert produced == 3
        assert mock_producer.send.await_count == 3

    async def test_stops_at_end_offset(self):
        """Records at or past end_offset are NOT read."""
        settings = Settings()
        offset_range = OffsetRange(partition=0, start_offset=0, end_offset=2)
        # Records at offsets 0, 1, 2 — only 0 and 1 should be read
        records = [MockRecord(offset=0), MockRecord(offset=1), MockRecord(offset=2)]
        mock_consumer = _make_consumer(records=records)

        with patch("replay.producer.AIOKafkaConsumer", return_value=mock_consumer):
            read, _ = await _replay_partition(
                settings=settings,
                offset_range=offset_range,
                symbol_filter=None,
                max_events=None,
                dry_run=True,
                producer=None,
            )

        assert read == 2

    async def test_stops_at_max_events(self):
        settings = Settings()
        offset_range = OffsetRange(partition=0, start_offset=0, end_offset=100)
        records = [MockRecord(offset=i) for i in range(10)]
        mock_consumer = _make_consumer(records=records)

        with patch("replay.producer.AIOKafkaConsumer", return_value=mock_consumer):
            read, _ = await _replay_partition(
                settings=settings,
                offset_range=offset_range,
                symbol_filter=None,
                max_events=3,
                dry_run=True,
                producer=None,
            )

        assert read == 3

    async def test_consumer_stopped_on_completion(self):
        settings = Settings()
        offset_range = OffsetRange(partition=0, start_offset=0, end_offset=1)
        records = [MockRecord(offset=0)]
        mock_consumer = _make_consumer(records=records)

        with patch("replay.producer.AIOKafkaConsumer", return_value=mock_consumer):
            await _replay_partition(
                settings=settings,
                offset_range=offset_range,
                symbol_filter=None,
                max_events=None,
                dry_run=True,
                producer=None,
            )

        mock_consumer.stop.assert_awaited_once()

    async def test_empty_partition_returns_zero(self):
        settings = Settings()
        offset_range = OffsetRange(partition=0, start_offset=0, end_offset=10)
        mock_consumer = _make_consumer(records=[])  # no records

        with patch("replay.producer.AIOKafkaConsumer", return_value=mock_consumer):
            read, produced = await _replay_partition(
                settings=settings,
                offset_range=offset_range,
                symbol_filter=None,
                max_events=None,
                dry_run=True,
                producer=None,
            )

        assert read == 0
        assert produced == 0


# ── run_replay ────────────────────────────────────────────────────────────────


class TestRunReplay:
    def _make_stats(self, **kwargs) -> ReplayStats:
        defaults = dict(
            dry_run=True,
            source_topic="market.raw.orderbook",
            target_topic="market.raw.orderbook",
            start_ts=datetime(2026, 5, 15, 10, 0, tzinfo=UTC),
            end_ts=datetime(2026, 5, 15, 10, 5, tzinfo=UTC),
            symbol=None,
        )
        return ReplayStats(**{**defaults, **kwargs})

    async def test_uses_iceberg_ranges_when_available(self):
        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)
        iceberg_ranges = [OffsetRange(0, 100, 200)]

        with (
            patch("replay.producer.read_offset_range", return_value=iceberg_ranges),
            patch("replay.producer._replay_partition", return_value=(5, 0)) as mock_replay,
        ):
            stats = await run_replay(
                settings=settings, start_ts=start_ts, end_ts=end_ts, dry_run=True
            )

        assert stats.offset_source == "iceberg"
        assert stats.events_read == 5
        assert 0 in stats.partitions_replayed
        mock_replay.assert_awaited_once()

    async def test_falls_back_to_kafka_time_when_iceberg_empty(self):
        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)
        kafka_ranges = [OffsetRange(0, 0, 100)]

        with (
            patch("replay.producer.read_offset_range", return_value=[]),
            patch("replay.producer._kafka_time_offset_ranges", return_value=kafka_ranges),
            patch("replay.producer._replay_partition", return_value=(10, 0)),
        ):
            stats = await run_replay(
                settings=settings, start_ts=start_ts, end_ts=end_ts, dry_run=True
            )

        assert stats.offset_source == "kafka_time"
        assert stats.events_read == 10

    async def test_returns_empty_stats_when_no_ranges(self):
        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)

        with (
            patch("replay.producer.read_offset_range", return_value=[]),
            patch("replay.producer._kafka_time_offset_ranges", return_value=[]),
        ):
            stats = await run_replay(
                settings=settings, start_ts=start_ts, end_ts=end_ts, dry_run=True
            )

        assert stats.events_read == 0
        assert stats.events_produced == 0
        assert stats.partitions_replayed == []

    async def test_dry_run_does_not_start_producer(self):
        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)

        with (
            patch("replay.producer.read_offset_range", return_value=[OffsetRange(0, 0, 10)]),
            patch("replay.producer._replay_partition", return_value=(3, 0)),
            patch("replay.producer.AIOKafkaProducer") as mock_producer_cls,
        ):
            await run_replay(settings=settings, start_ts=start_ts, end_ts=end_ts, dry_run=True)

        mock_producer_cls.assert_not_called()

    async def test_live_mode_starts_and_stops_producer(self):
        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)
        mock_producer = AsyncMock()

        with (
            patch("replay.producer.read_offset_range", return_value=[OffsetRange(0, 0, 10)]),
            patch("replay.producer._replay_partition", return_value=(3, 3)),
            patch("replay.producer.AIOKafkaProducer", return_value=mock_producer),
        ):
            stats = await run_replay(
                settings=settings, start_ts=start_ts, end_ts=end_ts, dry_run=False
            )

        mock_producer.start.assert_awaited_once()
        mock_producer.stop.assert_awaited_once()
        assert stats.events_produced == 3

    async def test_max_events_limits_total(self):
        """max_events is distributed across partitions; stops early when hit."""
        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)
        ranges = [OffsetRange(0, 0, 100), OffsetRange(1, 0, 100)]

        call_count = 0

        async def fake_replay(**kwargs):
            nonlocal call_count
            call_count += 1
            return (kwargs["max_events"], 0)  # consume exactly max_events

        with (
            patch("replay.producer.read_offset_range", return_value=ranges),
            patch("replay.producer._replay_partition", side_effect=fake_replay),
        ):
            stats = await run_replay(
                settings=settings, start_ts=start_ts, end_ts=end_ts, dry_run=True, max_events=5
            )

        # First partition consumes 5 → remaining=0 → second partition skipped
        assert call_count == 1
        assert stats.events_read == 5

    async def test_symbol_filter_passed_through(self):
        settings = Settings()
        start_ts = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
        end_ts = start_ts + timedelta(minutes=5)

        with (
            patch("replay.producer.read_offset_range", return_value=[]) as mock_iceberg,
            patch("replay.producer._kafka_time_offset_ranges", return_value=[]),
        ):
            await run_replay(
                settings=settings, start_ts=start_ts, end_ts=end_ts, symbol="BTCUSDT", dry_run=True
            )

        mock_iceberg.assert_called_once_with(settings, "BTCUSDT", start_ts, end_ts)
