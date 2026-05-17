"""Unit tests for check_freshness — Trino connection mocked out."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("airflow.operators.python", reason="apache-airflow not installed; skipping")

from airflow.exceptions import AirflowFailException  # noqa: E402
from freshness_sla_check import check_freshness  # noqa: E402


def _mock_conn(rows: list[tuple]) -> MagicMock:
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    return conn


def test_passes_when_all_fresh() -> None:
    with patch("freshness_sla_check.trino.dbapi.connect", return_value=_mock_conn([])):
        check_freshness()  # must not raise


def test_raises_when_symbol_stale() -> None:
    stale_rows = [("BTCUSDT", 120), ("ETHUSDT", 45)]
    with (
        patch("freshness_sla_check.trino.dbapi.connect", return_value=_mock_conn(stale_rows)),
        pytest.raises(AirflowFailException, match="SLA breach"),
    ):
        check_freshness()


def test_failure_message_contains_symbol() -> None:
    with (
        patch(
            "freshness_sla_check.trino.dbapi.connect",
            return_value=_mock_conn([("SOLUSDT", 99)]),
        ),
        pytest.raises(AirflowFailException, match="SOLUSDT"),
    ):
        check_freshness()
