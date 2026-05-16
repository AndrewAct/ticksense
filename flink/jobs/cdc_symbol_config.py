"""
cdc_symbol_config.py — Flink Job 3: Postgres CDC → bronze + normalized symbol config.

┌───────────────────────────────────────────────────────────────────────────────┐
│  Source A (DataStream)  : postgres.public.symbol_config  (Kafka)               │
│    Consumer group       : flink-cdc-bronze                                     │
│    Sink                 : iceberg_cat.raw.symbol_config_cdc  (Iceberg v1)      │
│                           append-only; full Debezium envelope preserved       │
│                                                                               │
│  Source B (Table API)   : cdc_symbol_config_source  (Kafka, debezium-json)     │
│    Consumer group       : flink-cdc-normalized                                 │
│    Sink                 : iceberg_cat.normalized.symbol_config (Iceberg v2)    │
│                           upsert by PRIMARY KEY symbol                        │
├───────────────────────────────────────────────────────────────────────────────┤
│  Debezium op codes handled:                                                   │
│    r = snapshot read  → bronze append + normalized INSERT                     │
│    c = create         → bronze append + normalized INSERT                     │
│    u = update         → bronze append + normalized UPDATE (upsert)            │
│    d = delete         → bronze append + normalized DELETE (equality delete)   │
│                                                                               │
│  Tombstones (null value): suppressed in connector (tombstones.on.delete=false)│
│    Defensive None check in CdcEnvelopeProcessor as belt-and-suspenders.       │
│                                                                               │
│  Checkpoint : every 60s, EXACTLY_ONCE                                         │
│  Restart    : failure-rate, max 5/5 min, 10s delay                            │
└───────────────────────────────────────────────────────────────────────────────┘

Debezium envelope (value.converter.schemas.enable=false):
    {
        "before": { <row> | null },          -- null for r and c ops
        "after":  { <row> | null },          -- null for d ops
        "source": {
            "lsn":   <int>,                  -- Postgres WAL byte position
            "ts_ms": <long>,                 -- Postgres commit timestamp (epoch ms)
            "txId":  <int>                   -- Postgres transaction ID
        },
        "op":    "r"|"c"|"u"|"d",
        "ts_ms": <long>                      -- Debezium processing timestamp
    }

Column type mapping from Debezium:
    NUMERIC(20,8) → DOUBLE   (decimal.handling.mode=double)
    TIMESTAMPTZ   → BIGINT   (epoch ms, time.precision.mode=connect)

SQL files
---------
All DDL lives under jobs/sql/cdc_symbol_config/.
This file contains: env setup, CdcEnvelopeProcessor, DataStream→Table bridge,
Table API source registration, and StatementSet execution.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from lib.config import Config
from lib.sql_runner import add_inserts_from_file, execute_sql_file
from pyflink.common import Row, WatermarkStrategy
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import (
    CheckpointingMode,
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import FlatMapFunction
from pyflink.table import DataTypes, Schema, StreamTableEnvironment

UTC = timezone.utc
log = logging.getLogger(__name__)

SQL = Path(__file__).parent / "sql"

# ── Bronze row type ───────────────────────────────────────────────────────────
# Emitted by CdcEnvelopeProcessor for every CDC event.
# ingest_ts_ms is a BIGINT epoch-ms; converted to TIMESTAMP_LTZ in the schema.

BRONZE_TYPE = Types.ROW_NAMED(
    [
        "op",
        "symbol",
        "before_json",
        "after_json",
        "source_lsn",
        "source_ts_ms",
        "source_tx_id",
        "debezium_ts_ms",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "ingest_ts_ms",
    ],
    [
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),  # nullable — null for r/c ops
        Types.STRING(),  # nullable — null for d ops
        Types.LONG(),
        Types.LONG(),
        Types.LONG(),
        Types.LONG(),
        Types.STRING(),
        Types.INT(),
        Types.LONG(),
        Types.LONG(),
    ],
)


class CdcEnvelopeProcessor(FlatMapFunction):
    """
    Parse a Debezium CDC JSON envelope and emit one bronze Row per event.

    Handles all four op codes (r/c/u/d).  The normalized upsert path is handled
    separately by a Table API source with debezium-json format — this processor
    only feeds the bronze append-only table.

    Tombstone handling:
        tombstones.on.delete=false in the connector suppresses null-value messages.
        The None check below is a defensive guard in case tombstones arrive from
        other connectors or connector config changes.

    Error handling:
        - Parse errors → log warning, skip (don't crash the job)
        - Missing 'op' field → log warning, skip
        - Missing payload (both before and after null) → log warning, skip
    """

    def flat_map(self, value: str) -> None:  # type: ignore[override]
        now_ms = int(datetime.now(UTC).timestamp() * 1000)

        # Tombstone guard: null Kafka value (should not arrive; tombstones.on.delete=false)
        if value is None:
            log.warning("cdc_tombstone_received: null value skipped (connector misconfigured?)")
            return

        # ── Parse Debezium envelope ────────────────────────────────────────────
        try:
            envelope = json.loads(value)
        except (json.JSONDecodeError, ValueError) as exc:
            log.warning("cdc_parse_error error=%s msg_preview=%.200s", exc, value)
            return

        op: str = envelope.get("op", "")
        if not op:
            log.warning("cdc_missing_op_field envelope_preview=%.200s", value)
            return

        if op not in ("r", "c", "u", "d"):
            log.warning("cdc_unknown_op op=%s envelope_preview=%.200s", op, value)
            return

        source: dict = envelope.get("source") or {}
        lsn: int = int(source.get("lsn") or 0)
        source_ts_ms: int = int(source.get("ts_ms") or 0)
        tx_id: int = int(source.get("txId") or 0)
        debezium_ts_ms: int = int(envelope.get("ts_ms") or 0)

        before: dict | None = envelope.get("before")
        after: dict | None = envelope.get("after")

        # ── Extract symbol for partitioning ───────────────────────────────────
        # after is present for r/c/u; before is present for d.
        row_data: dict | None = after if after is not None else before
        if row_data is None:
            log.warning("cdc_empty_payload op=%s lsn=%d: both before and after are null", op, lsn)
            return

        symbol: str = str(row_data.get("symbol") or "UNKNOWN")

        log.info(
            "cdc_event op=%s symbol=%s lsn=%d source_ts_ms=%d tx_id=%d debezium_ts_ms=%d",
            op,
            symbol,
            lsn,
            source_ts_ms,
            tx_id,
            debezium_ts_ms,
        )

        # ── Emit bronze row ────────────────────────────────────────────────────
        # kafka_partition and kafka_offset are -1 because SimpleStringSchema
        # doesn't expose Kafka record metadata (same limitation as normalize.py).
        # The source_lsn from Debezium's source block is the canonical ordering key
        # for replay purposes.
        yield Row(
            op,
            symbol,
            json.dumps(before) if before is not None else None,
            json.dumps(after) if after is not None else None,
            lsn,
            source_ts_ms,
            tx_id,
            debezium_ts_ms,
            Config.TOPIC_CDC,  # kafka_topic
            -1,  # kafka_partition (not available via SimpleStringSchema)
            -1,  # kafka_offset    (not available via SimpleStringSchema)
            now_ms,
        )


def _bronze_schema() -> Schema:
    """Table schema for the DataStream → Table bridge (bronze path).

    ingest_ts_ms (epoch ms BIGINT) is exposed as a computed TIMESTAMP_LTZ column
    so insert_bronze.sql can write TIMESTAMP_LTZ(3) to Iceberg directly.
    """
    return (
        Schema.new_builder()
        .column("op", DataTypes.STRING())
        .column("symbol", DataTypes.STRING())
        .column("before_json", DataTypes.STRING())
        .column("after_json", DataTypes.STRING())
        .column("source_lsn", DataTypes.BIGINT())
        .column("source_ts_ms", DataTypes.BIGINT())
        .column("source_tx_id", DataTypes.BIGINT())
        .column("debezium_ts_ms", DataTypes.BIGINT())
        .column("kafka_topic", DataTypes.STRING())
        .column("kafka_partition", DataTypes.INT())
        .column("kafka_offset", DataTypes.BIGINT())
        .column("ingest_ts_ms", DataTypes.BIGINT())
        .column_by_expression("ingest_ts", "TO_TIMESTAMP_LTZ(ingest_ts_ms, 3)")
        .build()
    )


def main() -> None:
    cfg = Config()

    # ── Environment ───────────────────────────────────────────────────────────
    log.info("cdc_job_starting kafka=%s iceberg=%s", cfg.kafka_brokers, cfg.iceberg_uri)

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

    # ── DDL: catalog, bronze sink, normalized sink, normalized source ─────────
    # catalogs.sql creates iceberg_cat + iceberg_cat.raw + iceberg_cat.normalized
    execute_sql_file(t_env, SQL / "catalogs.sql", **cfg.as_dict())
    log.info("cdc_ddl catalog registered")

    execute_sql_file(t_env, SQL / "cdc_symbol_config" / "sink_bronze.sql")
    log.info("cdc_ddl sink raw.symbol_config_cdc registered")

    execute_sql_file(t_env, SQL / "cdc_symbol_config" / "sink_normalized.sql")
    log.info("cdc_ddl sink normalized.symbol_config (v2 upsert) registered")

    # Normalized source: debezium-json Kafka table (separate consumer group from bronze).
    # Flink uses this changelog stream's RowKind metadata to drive Iceberg upserts.
    execute_sql_file(t_env, SQL / "cdc_symbol_config" / "source_normalized.sql", **cfg.as_dict())
    log.info("cdc_ddl source cdc_symbol_config_source (debezium-json) registered")

    # ── DataStream: bronze path (raw JSON → CdcEnvelopeProcessor) ────────────
    # Uses a separate consumer group (flink-cdc-bronze) so it progresses
    # independently from the Table API normalized path.
    raw_stream = env.from_source(
        KafkaSource.builder()
        .set_bootstrap_servers(cfg.kafka_brokers)
        .set_topics(cfg.TOPIC_CDC)
        .set_group_id(cfg.GROUP_CDC_BRONZE)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build(),
        WatermarkStrategy.no_watermarks(),
        "cdc-raw-source",
    )
    log.info(
        "cdc_source DataStream Kafka source created topic=%s group=%s",
        cfg.TOPIC_CDC,
        cfg.GROUP_CDC_BRONZE,
    )

    bronze_stream = raw_stream.flat_map(CdcEnvelopeProcessor(), output_type=BRONZE_TYPE)

    # Bridge bronze DataStream → Table view so insert_bronze.sql can reference it.
    t_env.create_temporary_view("bronze_view", bronze_stream, _bronze_schema())
    log.info("cdc_bridge bronze_view registered")

    # ── StatementSet: execute bronze + normalized INSERTs together ────────────
    # Both INSERT statements are added to one StatementSet so they share a single
    # Flink job graph and a single checkpoint cycle.
    stmt_set = t_env.create_statement_set()
    add_inserts_from_file(stmt_set, SQL / "cdc_symbol_config" / "insert_bronze.sql")
    add_inserts_from_file(stmt_set, SQL / "cdc_symbol_config" / "insert_normalized.sql")
    log.info("cdc_job submitting statement set (bronze + normalized)")
    stmt_set.execute()


if __name__ == "__main__":
    main()
