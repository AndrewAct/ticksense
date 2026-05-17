"""
producer.py — Kafka offset-seek replay producer.

Replay protocol
---------------
1. Determine which Kafka offsets correspond to the requested time window:
   a. Try Iceberg reader (reads kafka_offset from normalized.book_ticker).
   b. Fall back to Kafka's offsets_for_times() when Iceberg returns no ranges
      (e.g. rows written before the KafkaRecordDeserializer fix still have -1).
2. For each partition in the offset range:
   a. Seek a temporary consumer to start_offset.
   b. Read records up to end_offset (or max_events limit).
   c. In dry-run mode: count records without re-producing.
   d. In live mode: re-produce each record to the target topic.
      Downstream dedup (Flink normalize job) handles duplicates by
      last_update_id so re-producing is idempotent.

Key safety properties
---------------------
- The replay consumer uses a unique, ephemeral group ID (never committed),
  so it never moves the production consumer group offsets.
- Re-produced messages go to the same topic by default (target == source).
  The Flink normalize job's dedup key (exchange#symbol#last_update_id)
  ensures idempotency: replaying the same events twice → same Iceberg row count.
- dry-run=True reads and counts but never produces.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

from .config import Settings
from .iceberg_reader import OffsetRange, read_offset_range

log = logging.getLogger(__name__)


@dataclass
class ReplayStats:
    """Summary produced after a replay run."""

    dry_run: bool
    source_topic: str
    target_topic: str
    start_ts: datetime
    end_ts: datetime
    symbol: str | None
    events_read: int = 0
    events_produced: int = 0
    partitions_replayed: list[int] = field(default_factory=list)
    offset_source: str = "unknown"  # "iceberg" | "kafka_time"


async def _partitions_for_topic(
    settings: Settings,
    topic: str,
) -> list[int]:
    """Return partition IDs for a topic."""
    consumer = AIOKafkaConsumer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await consumer.start()
    try:
        parts = await consumer.partitions_for(topic)
        return sorted(parts or [])
    finally:
        await consumer.stop()


async def _kafka_time_offset_ranges(
    settings: Settings,
    topic: str,
    start_ts: datetime,
    end_ts: datetime,
) -> list[OffsetRange]:
    """Use Kafka's offsets_for_times() to find offset ranges for a time window.

    offsets_for_times(ts) returns the first offset whose record timestamp >= ts.
    We call it for both start_ts and end_ts to get the inclusive range.

    Returned offsets are (start_offset, end_offset) pairs per partition.
    If a partition has no records in the window, it is omitted.
    """
    if start_ts.tzinfo is None:
        start_ts = start_ts.replace(tzinfo=UTC)
    if end_ts.tzinfo is None:
        end_ts = end_ts.replace(tzinfo=UTC)

    start_ms = int(start_ts.timestamp() * 1000)
    end_ms = int(end_ts.timestamp() * 1000)

    partitions = await _partitions_for_topic(settings, topic)
    if not partitions:
        log.warning("kafka_time_offset.no_partitions topic=%s", topic)
        return []

    consumer = AIOKafkaConsumer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await consumer.start()
    try:
        tps = [TopicPartition(topic, p) for p in partitions]

        start_map = {tp: start_ms for tp in tps}
        end_map = {tp: end_ms for tp in tps}

        start_offsets = await consumer.offsets_for_times(start_map)
        end_offsets = await consumer.offsets_for_times(end_map)

        log.debug(
            "kafka_time_offset.raw start_offsets=%s end_offsets=%s",
            start_offsets,
            end_offsets,
        )

        ranges: list[OffsetRange] = []
        for tp in tps:
            start_result = start_offsets.get(tp)
            end_result = end_offsets.get(tp)

            if start_result is None:
                # No records at or after start_ts in this partition.
                log.debug("kafka_time_offset.skip partition=%d: no data at start_ts", tp.partition)
                continue

            start_off = start_result.offset
            # end_result=None means no records at or after end_ts → use end-of-log offset.
            end_off = end_result.offset if end_result is not None else (2**63 - 1)

            if start_off >= end_off:
                log.debug(
                    "kafka_time_offset.empty_window partition=%d start=%d end=%d",
                    tp.partition,
                    start_off,
                    end_off,
                )
                continue

            ranges.append(
                OffsetRange(partition=tp.partition, start_offset=start_off, end_offset=end_off)
            )
            log.info(
                "kafka_time_offset.range partition=%d start_offset=%d end_offset=%d",
                tp.partition,
                start_off,
                end_off,
            )

        return ranges

    finally:
        await consumer.stop()


async def _replay_partition(
    settings: Settings,
    offset_range: OffsetRange,
    symbol_filter: str | None,
    max_events: int | None,
    dry_run: bool,
    producer: AIOKafkaProducer | None,
) -> tuple[int, int]:
    """Replay one partition.  Returns (events_read, events_produced)."""
    tp = TopicPartition(settings.source_topic, offset_range.partition)

    # Ephemeral group ID — never committed, never interferes with production consumers.
    ephemeral_group = f"replay-{uuid.uuid4().hex[:8]}"
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=ephemeral_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        consumer.assign([tp])
        consumer.seek(tp, offset_range.start_offset)
        log.info(
            "replay_partition.start partition=%d start_offset=%d end_offset=%d group=%s",
            offset_range.partition,
            offset_range.start_offset,
            offset_range.end_offset,
            ephemeral_group,
        )

        events_read = 0
        events_produced = 0

        async for record in consumer:
            # Stop conditions: past end_offset or max_events reached.
            if record.offset >= offset_range.end_offset:
                log.debug(
                    "replay_partition.end_offset_reached partition=%d offset=%d",
                    offset_range.partition,
                    record.offset,
                )
                break
            if max_events is not None and events_read >= max_events:
                log.debug("replay_partition.max_events_reached events_read=%d", events_read)
                break

            events_read += 1

            if not dry_run and producer is not None:
                await producer.send(
                    settings.target_topic,
                    value=record.value,
                    key=record.key,
                )
                events_produced += 1

            if events_read % 1000 == 0:
                log.info(
                    "replay_partition.progress partition=%d read=%d produced=%d offset=%d",
                    offset_range.partition,
                    events_read,
                    events_produced,
                    record.offset,
                )

        log.info(
            "replay_partition.done partition=%d events_read=%d events_produced=%d",
            offset_range.partition,
            events_read,
            events_produced,
        )
        return events_read, events_produced

    finally:
        await consumer.stop()


async def run_replay(
    settings: Settings,
    start_ts: datetime,
    end_ts: datetime,
    symbol: str | None = None,
    max_events: int | None = None,
    dry_run: bool = False,
) -> ReplayStats:
    """Top-level replay coordinator.

    Steps:
    1. Try Iceberg reader for offset ranges.
    2. Fall back to Kafka offsets_for_times() if Iceberg returns no data.
    3. Replay each partition (dry_run or live).
    """
    log.info(
        "replay.start source=%s target=%s start=%s end=%s symbol=%s max_events=%s dry_run=%s",
        settings.source_topic,
        settings.target_topic,
        start_ts.isoformat(),
        end_ts.isoformat(),
        symbol,
        max_events,
        dry_run,
    )

    stats = ReplayStats(
        dry_run=dry_run,
        source_topic=settings.source_topic,
        target_topic=settings.target_topic,
        start_ts=start_ts,
        end_ts=end_ts,
        symbol=symbol,
    )

    # ── Step 1: Try Iceberg for offset ranges ──────────────────────────────────
    offset_ranges = read_offset_range(settings, symbol, start_ts, end_ts)
    if offset_ranges:
        stats.offset_source = "iceberg"
        log.info("replay.offset_source=iceberg ranges=%d", len(offset_ranges))
    else:
        # ── Step 2: Fall back to Kafka time-based offset lookup ────────────────
        log.info("replay.offset_source=kafka_time (Iceberg returned no ranges)")
        offset_ranges = await _kafka_time_offset_ranges(
            settings, settings.source_topic, start_ts, end_ts
        )
        stats.offset_source = "kafka_time"

    if not offset_ranges:
        log.warning("replay.no_offset_ranges: nothing to replay for the requested window")
        return stats

    log.info("replay.offset_ranges_resolved count=%d", len(offset_ranges))

    # ── Step 3: Replay each partition ─────────────────────────────────────────
    producer: AIOKafkaProducer | None = None
    if not dry_run:
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.target_topic,
            acks="all",
            enable_idempotence=True,
        )
        await producer.start()
        log.info("replay.producer_started target=%s", settings.target_topic)

    try:
        for offset_range in offset_ranges:
            remaining = (max_events - stats.events_read) if max_events is not None else None
            if remaining is not None and remaining <= 0:
                break

            read, produced = await _replay_partition(
                settings=settings,
                offset_range=offset_range,
                symbol_filter=symbol,
                max_events=remaining,
                dry_run=dry_run,
                producer=producer,
            )
            stats.events_read += read
            stats.events_produced += produced
            stats.partitions_replayed.append(offset_range.partition)
    finally:
        if producer is not None:
            await producer.stop()
            log.info("replay.producer_stopped")

    log.info(
        "replay.complete read=%d produced=%d partitions=%s source=%s dry_run=%s",
        stats.events_read,
        stats.events_produced,
        stats.partitions_replayed,
        stats.offset_source,
        stats.dry_run,
    )
    return stats
