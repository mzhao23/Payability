from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

HEALTHDATA_ROOT = Path(__file__).resolve().parents[1]
RISK_PIPELINE = HEALTHDATA_ROOT / "risk_pipeline.py"


def test_risk_pipeline_script_help_exits_zero() -> None:
    p = subprocess.run(
        [sys.executable, str(RISK_PIPELINE), "--help"],
        cwd=HEALTHDATA_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(HEALTHDATA_ROOT)},
    )
    assert p.returncode == 0, p.stderr
    assert "--dry-run" in p.stdout
    assert "--report-date" in p.stdout
    assert "--no-llm-narrative" in p.stdout


def test_module_health_risk_cli_help_exits_zero() -> None:
    p = subprocess.run(
        [sys.executable, "-m", "health_risk.cli", "--help"],
        cwd=HEALTHDATA_ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(HEALTHDATA_ROOT)},
    )
    assert p.returncode == 0, p.stderr
    assert "--dry-run" in p.stdout


@pytest.mark.integration
def test_risk_pipeline_dry_run_subprocess_real_env() -> None:
    if os.environ.get("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 with GCP + Supabase env to run this test.")

    report_date = os.environ.get("INTEGRATION_REPORT_DATE", "2024-01-01")
    p = subprocess.run(
        [
            sys.executable,
            str(RISK_PIPELINE),
            "--dry-run",
            "--report-date",
            report_date,
            "--no-export-json",
        ],
        cwd=HEALTHDATA_ROOT,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    assert p.returncode == 0, f"stderr:\n{p.stderr}\nstdout:\n{p.stdout}"
