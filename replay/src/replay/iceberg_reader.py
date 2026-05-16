"""
iceberg_reader.py — Query Iceberg for Kafka offset ranges by time window.

Strategy
--------
1. Open the Iceberg REST catalog via PyIceberg.
2. Scan the normalized.book_ticker table (which preserves kafka_partition and
   kafka_offset from the Flink normalize job).
3. Filter by (symbol, exchange_event_ts) to find rows inside the requested
   time window.
4. Return min/max (partition, offset) per partition.

Known limitation: normalize.py currently sets kafka_partition=-1 and
kafka_offset=-1 because SimpleStringSchema doesn't expose Kafka record metadata.
When offsets are -1, this reader falls back to Kafka's offsets_for_times() API,
which finds the first offset whose timestamp is >= the requested time.  The
Kafka path is always tried as a verification step even when Iceberg offsets are
available.

See the Known Limitations section in docs/DEBUGGING_PHASE3.md for the fix
(implement KafkaRecordDeserializationSchema in normalize.py).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

from .config import Settings

log = logging.getLogger(__name__)

UTC = UTC


@dataclass(frozen=True)
class OffsetRange:
    """Kafka offset range for one partition."""

    partition: int
    start_offset: int
    end_offset: int  # exclusive upper bound


def _iceberg_catalog(settings: Settings) -> RestCatalog:
    return RestCatalog(
        name="iceberg_cat",
        uri=settings.iceberg_rest_uri,
        warehouse=settings.iceberg_warehouse,
        **{
            "s3.endpoint": settings.s3_endpoint,
            "s3.access-key-id": settings.s3_access_key,
            "s3.secret-access-key": settings.s3_secret_key,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


def _ts_to_ms(ts: datetime) -> int:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return int(ts.timestamp() * 1000)


def read_offset_range(
    settings: Settings,
    symbol: str | None,
    start_ts: datetime,
    end_ts: datetime,
) -> list[OffsetRange]:
    """Query Iceberg normalized.book_ticker for kafka offsets in [start_ts, end_ts).

    Returns one OffsetRange per Kafka partition.  If kafka_offset=-1 (the current
    normalize.py limitation), returns an empty list so the caller can fall back
    to Kafka's offsets_for_times().
    """
    log.info(
        "iceberg_reader.scan table=%s.%s symbol=%s start=%s end=%s",
        settings.iceberg_namespace,
        settings.iceberg_table,
        symbol,
        start_ts.isoformat(),
        end_ts.isoformat(),
    )

    try:
        catalog = _iceberg_catalog(settings)
        table = catalog.load_table(f"{settings.iceberg_namespace}.{settings.iceberg_table}")
    except Exception as exc:
        log.warning(
            "iceberg_reader.catalog_error error=%s — falling back to Kafka time lookup", exc
        )
        return []

    start_ms = _ts_to_ms(start_ts)
    end_ms = _ts_to_ms(end_ts)

    # Iceberg predicate pushdown on exchange_event_ts (stored as epoch ms in Parquet).
    # PyIceberg uses microseconds for TIMESTAMP_LTZ internally, so multiply by 1000.
    # pyiceberg expression stubs don't match the runtime signature; ignore mypy here.
    filter_expr = And(
        GreaterThanOrEqual("exchange_event_ts", value=start_ms * 1000),  # type: ignore
        LessThan("exchange_event_ts", value=end_ms * 1000),  # type: ignore
    )
    if symbol:
        from pyiceberg.expressions import EqualTo

        filter_expr = And(filter_expr, EqualTo("symbol", value=symbol))  # type: ignore

    log.debug("iceberg_reader.filter expr=%s", filter_expr)

    try:
        scan = table.scan(
            row_filter=filter_expr, selected_fields=("kafka_partition", "kafka_offset")
        )
        arrow_table = scan.to_arrow()
    except Exception as exc:
        log.warning("iceberg_reader.scan_error error=%s — falling back to Kafka time lookup", exc)
        return []

    row_count = len(arrow_table)
    log.info("iceberg_reader.scan_complete rows=%d", row_count)

    if row_count == 0:
        log.info("iceberg_reader.no_rows: time window produced no Iceberg rows")
        return []

    # Check for the known -1 limitation.
    partitions = arrow_table["kafka_partition"].to_pylist()
    offsets = arrow_table["kafka_offset"].to_pylist()

    if all(p == -1 for p in partitions):
        log.info(
            "iceberg_reader.offsets_unavailable: kafka_partition=-1 in all %d rows. "
            "normalize.py uses SimpleStringSchema which does not expose Kafka metadata. "
            "Falling back to Kafka offsets_for_times(). "
            "See docs/DEBUGGING_PHASE3.md for the fix.",
            row_count,
        )
        return []

    # Build one OffsetRange per partition from actual Iceberg data.
    by_partition: dict[int, list[int]] = {}
    for part, off in zip(partitions, offsets, strict=True):
        if part == -1 or off == -1:
            continue
        by_partition.setdefault(part, []).append(off)

    ranges = [
        OffsetRange(
            partition=part,
            start_offset=min(offs),
            end_offset=max(offs) + 1,
        )
        for part, offs in sorted(by_partition.items())
    ]

    log.info(
        "iceberg_reader.offset_ranges count=%d ranges=%s",
        len(ranges),
        [(r.partition, r.start_offset, r.end_offset) for r in ranges],
    )
    return ranges
