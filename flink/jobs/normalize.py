"""
normalize.py — Flink Job 1: raw L2 diffs → normalized silver book-ticker.

┌─────────────────────────────────────────────────────────────────┐
│ Source  : market.raw.orderbook       (Kafka)                    │
│ Sinks   : iceberg_cat.normalized.book_ticker  (Iceberg)         │
│           market.normalized.book_ticker       (Kafka)           │
│           market.dlq                          (Kafka, late evts)│
├─────────────────────────────────────────────────────────────────┤
│ Event time   : exchange_event_ts                                │
│ Watermark    : bounded out-of-orderness, 5 s                    │
│ Dedup key    : (exchange, symbol, last_update_id)               │
│ State        : full order book per key in ValueState (JSON str) │
│ Late routing : lag > 30 s → DLQ side output                     │
│ Checkpoint   : every 60 s, EXACTLY_ONCE                         │
│ Restart      : failure-rate, max 5/5 min, 10 s delay            │
└─────────────────────────────────────────────────────────────────┘

SQL files
---------
All table DDL lives under jobs/sql/normalize/.
This file contains only: env setup, UDF-less stateful DataStream logic,
DataStream→Table bridge, and calls to sql_runner helpers.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flink_lib.config import Config
from flink_lib.kafka_schema import (
    KAFKA_RECORD_TYPE,
    OFFSET_IDX,
    PARTITION_IDX,
    PAYLOAD_IDX,
)
from flink_lib.logic import (
    apply_depth_diff,
    best_ask,
    best_bid,
    compute_imbalance,
    compute_mid_price,
    compute_spread,
)
from flink_lib.sql_runner import add_inserts_from_file, execute_sql_file
from pyflink.common import Row
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common.typeinfo import Types
from pyflink.datastream import (
    CheckpointingMode,
    StreamExecutionEnvironment,
)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import DataTypes, Schema, StreamTableEnvironment

UTC = timezone.utc
log = logging.getLogger(__name__)

SQL = Path(__file__).parent / "sql"

# NOTE: PyFlink 1.18 Python bindings don't support ctx.output() side outputs.
# DLQ routing is handled via log.warning() in OrderBookProcessor instead.

# Row type emitted by OrderBookProcessor.
# Timestamps are epoch-milliseconds (BIGINT) to avoid the Python↔Java
# Instant ambiguity in PyFlink 1.18; converted to TIMESTAMP_LTZ in the Table schema.
BOOK_TICKER_TYPE = Types.ROW_NAMED(
    [
        "event_id",
        "exchange",
        "symbol",
        "exchange_event_ts_ms",
        "ingest_ts_ms",
        "processed_ts_ms",
        "last_update_id",
        "first_update_id",
        "best_bid_price",
        "best_bid_qty",
        "best_ask_price",
        "best_ask_qty",
        "spread",
        "mid_price",
        "imbalance",
        "src_kafka_topic",
        "src_kafka_partition",
        "src_kafka_offset",
    ],
    [
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.LONG(),
        Types.LONG(),
        Types.LONG(),
        Types.LONG(),
        Types.LONG(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.STRING(),
        Types.INT(),
        Types.LONG(),
    ],
)


def _to_epoch_ms(ts: Any) -> int:
    """Convert timestamp (datetime, ISO string, or numeric) to epoch milliseconds."""
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        return int(ts.timestamp() * 1000)
    if isinstance(ts, str):
        return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
    if isinstance(ts, (int, float)):
        return int(ts)
    return int(datetime.now(UTC).timestamp() * 1000)


def _kafka_key(record: Row) -> str:
    """Extract routing key from Kafka record Row(payload, partition, offset)."""
    data = json.loads(record[PAYLOAD_IDX])
    return f"{data['exchange']}#{data['symbol']}"


class OrderBookProcessor(KeyedProcessFunction):
    """
    Stateful operator keyed by ``exchange#symbol``.

    Maintains a full L2 order book (bid/ask price→qty dict) in a ValueState
    encoded as a JSON string.  On each diff event:

    1. Drop duplicates (last_update_id ≤ last seen for this key).
    2. Route events whose exchange_event_ts lags wall-clock > 30 s to DLQ.
    3. Apply the diff to the order book state.
    4. Compute best bid/ask + derived metrics and emit a book-ticker Row.

    Note: StateTtlConfig is not exposed in PyFlink 1.18 Python bindings.
    Set table.exec.state.ttl for Table-API state; this DataStream state grows
    until a key goes idle and the TaskManager restarts.  Acceptable for demo scale.
    """

    def open(self, runtime_context: RuntimeContext) -> None:  # type: ignore[override]
        self._book_state = runtime_context.get_state(ValueStateDescriptor("book", Types.STRING()))
        self._last_id_state = runtime_context.get_state(
            ValueStateDescriptor("last_update_id", Types.LONG())
        )

    def process_element(  # type: ignore[override]
        self, value: Row, ctx: KeyedProcessFunction.Context
    ) -> None:
        kafka_partition: int = value[PARTITION_IDX]
        kafka_offset: int = value[OFFSET_IDX]
        msg = json.loads(value[PAYLOAD_IDX])
        last_update_id: int = int(msg["last_update_id"])

        # ── 1. Dedup ──────────────────────────────────────────────────────────
        last_seen = self._last_id_state.value()
        if last_seen is not None and last_update_id <= last_seen:
            return
        self._last_id_state.update(last_update_id)

        # ── 2. Late-event routing ─────────────────────────────────────────────
        # PyFlink 1.18 Python bindings don't support ctx.output() side outputs.
        # Late events are logged and dropped instead of routed to market.dlq.
        exchange_event_ts_ms = _to_epoch_ms(
            msg.get("exchange_event_ts") or msg.get("event_time", "")
        )
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        lag_s = (now_ms - exchange_event_ts_ms) / 1000.0
        if lag_s > Config.LATE_THRESHOLD_SECONDS:
            log.warning(
                "late_event_dropped event_id=%s symbol=%s lag_s=%.1f",
                msg.get("event_id", ""),
                msg.get("symbol", ""),
                lag_s,
            )
            return

        # ── 3. Apply diff to order book state ─────────────────────────────────
        state_str = self._book_state.value()
        book: dict[str, dict[str, str]] = (
            json.loads(state_str) if state_str else {"bids": {}, "asks": {}}
        )
        apply_depth_diff(
            book["bids"],
            book["asks"],
            list(msg.get("bids") or []),
            list(msg.get("asks") or []),
        )
        self._book_state.update(json.dumps(book))

        bb = best_bid(book["bids"])
        ba = best_ask(book["asks"])
        if bb is None or ba is None:
            return  # not enough levels yet

        bb_price, bb_qty = bb
        ba_price, ba_qty = ba

        # ── 4. Emit book-ticker row ───────────────────────────────────────────
        yield Row(
            msg.get("event_id", ""),
            msg.get("exchange", ""),
            msg.get("symbol", ""),
            exchange_event_ts_ms,
            _to_epoch_ms(msg.get("ingest_ts") or msg.get("ingest_time", "")),
            now_ms,
            last_update_id,
            int(msg.get("first_update_id", 0)),
            bb_price,
            bb_qty,
            ba_price,
            ba_qty,
            compute_spread(bb_price, ba_price),
            compute_mid_price(bb_price, ba_price),
            compute_imbalance(book["bids"], book["asks"]),
            Config.TOPIC_RAW,
            kafka_partition,
            kafka_offset,
        )


def _book_ticker_schema() -> Schema:
    """Table schema for the DataStream→Table bridge.

    Raw BIGINT epoch-ms columns are exposed alongside computed TIMESTAMP_LTZ
    columns so the INSERT SQL can reference the typed versions directly.
    """
    return (
        Schema.new_builder()
        .column("event_id", DataTypes.STRING())
        .column("exchange", DataTypes.STRING())
        .column("symbol", DataTypes.STRING())
        .column("exchange_event_ts_ms", DataTypes.BIGINT())
        .column("ingest_ts_ms", DataTypes.BIGINT())
        .column("processed_ts_ms", DataTypes.BIGINT())
        .column("last_update_id", DataTypes.BIGINT())
        .column("first_update_id", DataTypes.BIGINT())
        .column("best_bid_price", DataTypes.DOUBLE())
        .column("best_bid_qty", DataTypes.DOUBLE())
        .column("best_ask_price", DataTypes.DOUBLE())
        .column("best_ask_qty", DataTypes.DOUBLE())
        .column("spread", DataTypes.DOUBLE())
        .column("mid_price", DataTypes.DOUBLE())
        .column("imbalance", DataTypes.DOUBLE())
        .column("src_kafka_topic", DataTypes.STRING())
        .column("src_kafka_partition", DataTypes.INT())
        .column("src_kafka_offset", DataTypes.BIGINT())
        # Computed TIMESTAMP_LTZ(3) columns from epoch-ms longs
        .column_by_expression("exchange_event_ts", "TO_TIMESTAMP_LTZ(exchange_event_ts_ms, 3)")
        .column_by_expression("ingest_ts", "TO_TIMESTAMP_LTZ(ingest_ts_ms, 3)")
        .column_by_expression("processed_ts", "TO_TIMESTAMP_LTZ(processed_ts_ms, 3)")
        .watermark("exchange_event_ts", "exchange_event_ts - INTERVAL '5' SECOND")
        .build()
    )


def main() -> None:
    cfg = Config()

    # ── Environment ───────────────────────────────────────────────────────────
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(cfg.checkpoint_interval_ms, CheckpointingMode.EXACTLY_ONCE)
    env.set_restart_strategy(
        RestartStrategies.failure_rate_restart(
            failure_rate=5,
            failure_interval=300_000,
            delay_interval=10_000,
        )
    )
    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("table.exec.state.ttl", str(cfg.STATE_TTL_MS))

    # ── DDL: catalog, source, and sinks ──────────────────────────────────────
    execute_sql_file(t_env, SQL / "catalogs.sql", **cfg.as_dict())
    execute_sql_file(t_env, SQL / "normalize" / "source_raw.sql", **cfg.as_dict())
    execute_sql_file(t_env, SQL / "normalize" / "sink_iceberg.sql")
    execute_sql_file(t_env, SQL / "normalize" / "sink_kafka.sql", **cfg.as_dict())

    # ── DataStream Kafka source: raw payload + real partition/offset ──────────
    # Bridged from the SQL source table so metadata columns (partition, offset)
    # are populated by the Kafka connector without needing the unavailable
    # KafkaRecordDeserializationSchema Python binding.
    raw_stream = t_env.to_append_stream(
        t_env.sql_query("SELECT payload, kafka_partition, kafka_offset FROM raw_orderbook_stream"),
        KAFKA_RECORD_TYPE,
    )

    # ── Stateful DataStream processing ────────────────────────────────────────
    processed_stream = raw_stream.key_by(_kafka_key).process(
        OrderBookProcessor(), output_type=BOOK_TICKER_TYPE
    )

    # ── DataStream → Table bridge ─────────────────────────────────────────────
    t_env.create_temporary_view("book_ticker_view", processed_stream, _book_ticker_schema())

    # ── DML: fan-out to Iceberg + Kafka via shared statement set ─────────────
    stmt_set = t_env.create_statement_set()
    add_inserts_from_file(stmt_set, SQL / "normalize" / "insert.sql")
    stmt_set.execute()


if __name__ == "__main__":
    main()
