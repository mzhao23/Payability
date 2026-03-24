"""
CLI entry: run from the HealthData directory so `health_risk` resolves.

  cd HealthData && python risk_pipeline.py --dry-run
  cd HealthData && python -m health_risk.cli --dry-run
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from health_risk.cli import main

if __name__ == "__main__":
    main()
