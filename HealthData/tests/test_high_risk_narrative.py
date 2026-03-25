from __future__ import annotations

from unittest.mock import MagicMock, patch

from health_risk.config import Settings
from health_risk.llm.high_risk_narrative import (
    enrich_high_risk_narratives,
    strip_llm_narrative_for_supabase,
)


def _settings(llm: bool = True, key: str | None = "sk-test") -> Settings:
    return Settings(
        bq_project="p",
        bq_dataset="d",
        bq_table="t",
        bq_payability_table="x",
        supabase_url="http://t",
        supabase_key="k",
        supabase_output_table="o",
        supabase_mapping_table="m",
        consolidated_table="c",
        openai_api_key=key,
        openai_model="gpt-4o",
        llm_high_risk_narrative_enabled=llm,
        llm_narrative_max_workers=2,
        high_risk_narrative_threshold=5.0,
        store_llm_narrative_in_supabase=False,
    )


def test_strip_llm_narrative_for_supabase_removes_llm_keys() -> None:
    row = {"risk_score": 8.0, "high_risk_narrative_llm": "hello", "high_risk_narrative_error": None}
    out = strip_llm_narrative_for_supabase(row)
    assert "high_risk_narrative_llm" not in out
    assert "high_risk_narrative_error" not in out
    assert out["risk_score"] == 8.0


def test_enrich_skipped_when_disabled() -> None:
    payload = [{"report_date": "2025-01-01", "mp_sup_key": "m1", "risk_score": 9.0}]
    raw_index = {("2025-01-01", "m1"): {"orderWithDefects_60_rate": 0.04}}
    n = enrich_high_risk_narratives(
        payload,
        _settings(llm=False, key="sk-test"),
        raw_index,
        force_disable=False,
    )
    assert n == 0
    assert payload[0].get("high_risk_narrative_llm") is None


@patch("health_risk.llm.high_risk_narrative._call_openai")
def test_enrich_calls_openai_for_high_risk_only(mock_openai: MagicMock) -> None:
    mock_openai.return_value = "Narrative text."
    payload = [
        {"report_date": "2025-01-01", "mp_sup_key": "low", "risk_score": 2.0},
        {"report_date": "2025-01-01", "mp_sup_key": "high", "risk_score": 8.0},
    ]
    raw_index = {
        ("2025-01-01", "low"): {},
        ("2025-01-01", "high"): {"orderWithDefects_60_rate": 0.05},
    }
    n = enrich_high_risk_narratives(payload, _settings(), raw_index, force_disable=False)
    assert n == 1
    assert payload[0].get("high_risk_narrative_llm") is None
    assert payload[1].get("high_risk_narrative_llm") == "Narrative text."
    mock_openai.assert_called_once()


@patch("health_risk.llm.high_risk_narrative._call_openai")
def test_enrich_records_error_on_llm_failure(mock_openai: MagicMock) -> None:
    mock_openai.side_effect = RuntimeError("rate limit")
    payload = [{"report_date": "2025-01-01", "mp_sup_key": "high", "risk_score": 8.0}]
    raw_index = {("2025-01-01", "high"): {}}
    n = enrich_high_risk_narratives(payload, _settings(), raw_index, force_disable=False)
    assert n == 0
    assert payload[0]["high_risk_narrative_llm"] is None
    assert "rate limit" in (payload[0].get("high_risk_narrative_error") or "")
