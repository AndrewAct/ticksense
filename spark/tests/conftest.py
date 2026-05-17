"""Add spark/jobs to sys.path so job modules and lib are importable by name."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "jobs"))
