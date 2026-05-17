"""
ohlcv_1m.py — Flink Job 2: book-ticker ticks → 1-minute OHLCV silver table.

┌─────────────────────────────────────────────────────────────────┐
│ Source : market.normalized.book_ticker  (Kafka)                 │
│ Sink   : iceberg_cat.normalized.ohlcv_1m  (Iceberg)             │
├─────────────────────────────────────────────────────────────────┤
│ Window     : TUMBLE, 1 minute, event time                       │
│ Key        : (exchange, symbol)                                 │
│ Volume     : best_bid_qty proxy (L2 book ticks, not trades)     │
│ Checkpoint : every 60 s, EXACTLY_ONCE                           │
│ Restart    : failure-rate, max 5/5 min, 10 s delay              │
└─────────────────────────────────────────────────────────────────┘

Pure Table-API job — no DataStream or stateful operators needed.
All SQL lives in jobs/sql/ohlcv_1m/*.sql.
"""

from __future__ import annotations

import logging
from pathlib import Path

from flink_lib.config import Config
from flink_lib.sql_runner import add_inserts_from_file, execute_sql_file
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.datastream import (
    CheckpointingMode,
    StreamExecutionEnvironment,
)
from pyflink.table import StreamTableEnvironment

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

SQL = Path(__file__).parent / "sql"


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

    # ── DDL: catalog, source, sink ────────────────────────────────────────────
    execute_sql_file(t_env, SQL / "catalogs.sql", **cfg.as_dict())
    execute_sql_file(t_env, SQL / "ohlcv_1m" / "source.sql", **cfg.as_dict())
    execute_sql_file(t_env, SQL / "ohlcv_1m" / "sink_iceberg.sql")

    # ── DML: tumbling-window OHLCV aggregation ────────────────────────────────
    stmt_set = t_env.create_statement_set()
    add_inserts_from_file(stmt_set, SQL / "ohlcv_1m" / "insert.sql")
    stmt_set.execute()


if __name__ == "__main__":
    main()
