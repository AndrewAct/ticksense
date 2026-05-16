"""
main.py — CLI entry point for the replay producer.

Usage
-----
    # Dry-run: count 10k events from the last hour without re-producing.
    python -m replay.main --events 10000 --dry-run

    # Replay a specific time window for one symbol:
    python -m replay.main \\
        --symbol BTCUSDT \\
        --start-ts 2026-05-15T10:00:00Z \\
        --end-ts   2026-05-15T10:05:00Z

    # Dry-run with event limit:
    python -m replay.main --events 500 --dry-run --symbol ETHUSDT

make replay-sample
------------------
    uv run python -m replay.main --events 10000 --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import UTC, datetime, timedelta

from .config import Settings
from .producer import run_replay

UTC = UTC

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


def _parse_ts(value: str) -> datetime:
    """Parse ISO-8601 timestamp or epoch seconds."""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        pass
    try:
        return datetime.fromtimestamp(float(value), tz=UTC)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Cannot parse timestamp: {value!r}. Use ISO-8601 (e.g. 2026-05-15T10:00:00Z) "
            "or epoch seconds."
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m replay.main",
        description="Replay Kafka events from a time window back to the source topic.",
    )
    p.add_argument(
        "--symbol",
        metavar="SYM",
        help="Filter by symbol (e.g. BTCUSDT). Omit to replay all symbols.",
    )
    p.add_argument(
        "--start-ts",
        metavar="TS",
        type=_parse_ts,
        help="Replay window start (ISO-8601 or epoch seconds). Defaults to 1 hour ago.",
    )
    p.add_argument(
        "--end-ts",
        metavar="TS",
        type=_parse_ts,
        help="Replay window end (ISO-8601 or epoch seconds). Defaults to now.",
    )
    p.add_argument(
        "--events",
        metavar="N",
        type=int,
        help="Maximum number of events to replay. No limit if omitted.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Count events without re-producing them to Kafka.",
    )
    p.add_argument(
        "--source-topic",
        metavar="TOPIC",
        help="Override source Kafka topic (default from REPLAY_SOURCE_TOPIC env var).",
    )
    p.add_argument(
        "--target-topic",
        metavar="TOPIC",
        help="Override target Kafka topic (default from REPLAY_TARGET_TOPIC env var).",
    )
    return p


async def _main(args: argparse.Namespace) -> int:
    settings = Settings()

    if args.source_topic:
        settings = settings.model_copy(update={"source_topic": args.source_topic})
    if args.target_topic:
        settings = settings.model_copy(update={"target_topic": args.target_topic})

    now = datetime.now(UTC)
    start_ts = args.start_ts or (now - timedelta(hours=1))
    end_ts = args.end_ts or now

    if start_ts >= end_ts:
        log.error("replay.invalid_window: start_ts=%s must be before end_ts=%s", start_ts, end_ts)
        return 1

    log.info(
        "replay.cli args: symbol=%s start=%s end=%s events=%s dry_run=%s",
        args.symbol,
        start_ts.isoformat(),
        end_ts.isoformat(),
        args.events,
        args.dry_run,
    )

    stats = await run_replay(
        settings=settings,
        start_ts=start_ts,
        end_ts=end_ts,
        symbol=args.symbol,
        max_events=args.events,
        dry_run=args.dry_run,
    )

    print()
    print("─" * 60)
    print(f"  Replay {'DRY-RUN ' if stats.dry_run else ''}complete")
    print(f"  Source topic     : {stats.source_topic}")
    print(f"  Target topic     : {stats.target_topic}")
    print(f"  Symbol filter    : {stats.symbol or '(all)'}")
    print(f"  Window           : {stats.start_ts.isoformat()} → {stats.end_ts.isoformat()}")
    print(f"  Offset source    : {stats.offset_source}")
    print(f"  Events read      : {stats.events_read:,}")
    print(f"  Events produced  : {stats.events_produced:,}")
    print(f"  Partitions       : {stats.partitions_replayed}")
    print("─" * 60)

    return 0


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    sys.exit(asyncio.run(_main(args)))


if __name__ == "__main__":
    main()
