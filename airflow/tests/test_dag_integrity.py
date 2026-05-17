"""DAG integrity tests — verify all 6 DAGs load without import errors."""

from __future__ import annotations

import pytest

pytest.importorskip(
    "airflow.operators.python", reason="apache-airflow not installed; skipping DAG tests"
)

from airflow.models import DagBag  # noqa: E402

EXPECTED_DAG_IDS = {
    "backfill_ohlcv_1m",
    "compact_iceberg_tables",
    "expire_iceberg_snapshots",
    "run_dbt_models",
    "run_great_expectations",
    "freshness_sla_check",
}


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    return DagBag(dag_folder="airflow/dags", include_examples=False)


def test_no_import_errors(dag_bag: DagBag) -> None:
    assert dag_bag.import_errors == {}, f"Import errors: {dag_bag.import_errors}"


def test_all_dags_present(dag_bag: DagBag) -> None:
    assert set(dag_bag.dag_ids) == EXPECTED_DAG_IDS


def test_all_dags_have_tags(dag_bag: DagBag) -> None:
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"DAG '{dag_id}' has no tags"


def test_all_dags_have_retries(dag_bag: DagBag) -> None:
    # schedule=None (manual) DAGs skip this check
    for dag_id, dag in dag_bag.dags.items():
        if dag.schedule_interval is None:
            continue
        for task in dag.tasks:
            assert task.retries > 0, f"{dag_id}.{task.task_id} has retries=0"
