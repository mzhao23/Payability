from __future__ import annotations

import sys
from datetime import date
from unittest.mock import MagicMock

import pytest


def test_main_dry_run_with_report_date_matches_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_pipeline = MagicMock()
    monkeypatch.setattr("health_risk.cli.build_default_pipeline", lambda: mock_pipeline)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "risk_pipeline.py",
            "--dry-run",
            "--report-date",
            "2025-06-01",
            "--no-export-json",
        ],
    )

    from health_risk.cli import main

    main()

    mock_pipeline.run_for_date.assert_called_once()
    args, kwargs = mock_pipeline.run_for_date.call_args
    assert args[0] == date(2025, 6, 1)
    assert kwargs["dry_run"] is True
    assert kwargs["export_json"] is False
    assert kwargs["limit"] == 5000
    assert kwargs["chunk_size"] == 500
    assert kwargs["enable_llm_narrative"] is True


def test_main_dry_run_days_back_uses_latest_date(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_pipeline = MagicMock()
    mock_pipeline.get_latest_report_date.return_value = date(2025, 1, 10)
    monkeypatch.setattr("health_risk.cli.build_default_pipeline", lambda: mock_pipeline)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "risk_pipeline.py",
            "--dry-run",
            "--days-back",
            "2",
            "--no-export-json",
        ],
    )

    from health_risk.cli import main

    main()

    assert mock_pipeline.run_for_date.call_count == 3
    dates = [mock_pipeline.run_for_date.call_args_list[i][0][0] for i in range(3)]
    assert dates == [
        date(2025, 1, 10),
        date(2025, 1, 9),
        date(2025, 1, 8),
    ]
    for i in range(3):
        assert mock_pipeline.run_for_date.call_args_list[i][1]["dry_run"] is True


def test_main_no_llm_narrative_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_pipeline = MagicMock()
    monkeypatch.setattr("health_risk.cli.build_default_pipeline", lambda: mock_pipeline)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "risk_pipeline.py",
            "--dry-run",
            "--report-date",
            "2025-06-01",
            "--no-export-json",
            "--no-llm-narrative",
        ],
    )

    from health_risk.cli import main

    main()

    assert mock_pipeline.run_for_date.call_args[1]["enable_llm_narrative"] is False


def test_main_without_dry_run_passes_dry_run_false(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_pipeline = MagicMock()
    monkeypatch.setattr("health_risk.cli.build_default_pipeline", lambda: mock_pipeline)
    monkeypatch.setattr(
        sys,
        "argv",
        ["risk_pipeline.py", "--report-date", "2025-06-01", "--no-export-json"],
    )

    from health_risk.cli import main

    main()

    assert mock_pipeline.run_for_date.call_args[1]["dry_run"] is False
