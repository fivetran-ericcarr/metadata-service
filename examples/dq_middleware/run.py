#!/usr/bin/env python3
"""Entry point that makes the example runnable without packaging:

    uv run python examples/dq_middleware/run.py evaluate --snapshot metadata_snapshots/latest.json
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dq_middleware.cli import app  # noqa: E402

if __name__ == "__main__":
    app()
