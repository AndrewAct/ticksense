"""Add airflow/dags to sys.path so DAG modules are importable by name."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "dags"))
