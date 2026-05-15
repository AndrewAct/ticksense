"""Make flink/jobs importable for Python 3.12 unit tests (no PyFlink needed)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "jobs"))
