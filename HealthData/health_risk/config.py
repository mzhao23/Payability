from __future__ import annotations

import os
from dataclasses import dataclass


def _env_truthy(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or not v.strip():
        return default
    try:
        return int(v)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    """Runtime configuration (tables, credentials)."""

    bq_project: str
    bq_dataset: str
    bq_table: str
    bq_payability_table: str
    supabase_url: str
    supabase_key: str
    supabase_output_table: str
    supabase_mapping_table: str
    consolidated_table: str
    reviewed_suppliers_table: str
    # High-risk narrative (OpenAI): rule-based score unchanged; LLM adds text only.
    openai_api_key: str | None
    openai_model: str
    llm_high_risk_narrative_enabled: bool
    llm_narrative_max_workers: int
    high_risk_narrative_threshold: float
    """Call LLM when risk_score > this value (aligned with flagged high-risk cutoff)."""
    store_llm_narrative_in_supabase: bool
    """If False, `high_risk_narrative_llm` is omitted from health_daily_risk upsert (no DB migration)."""

    @property
    def bq_full_table(self) -> str:
        return f"`{self.bq_project}.{self.bq_dataset}.{self.bq_table}`"


def load_settings() -> Settings:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.")

    api_key = os.getenv("OPENAI_API_KEY") or os.getenv("HEALTH_RISK_OPENAI_API_KEY")
    api_key = api_key.strip() if api_key else None

    llm_on = api_key is not None and _env_truthy("HEALTH_RISK_LLM_NARRATIVE", True)

    return Settings(
        bq_project="bigqueryexport-183608",
        bq_dataset="amazon",
        bq_table="customer_health_metrics",
        bq_payability_table="`bigqueryexport-183608.PayabilitySheets.v_supplier_summary`",
        supabase_url=url,
        supabase_key=key,
        supabase_output_table="health_daily_risk",
        supabase_mapping_table="suppliers",
        consolidated_table="consolidated_flagged_supplier_list",
        reviewed_suppliers_table="reviewed_suppliers",
        openai_api_key=api_key,
        openai_model=os.getenv("HEALTH_RISK_OPENAI_MODEL", "gpt-4o-mini"),
        llm_high_risk_narrative_enabled=llm_on,
        llm_narrative_max_workers=max(1, _env_int("HEALTH_RISK_LLM_MAX_WORKERS", 4)),
        high_risk_narrative_threshold=float(os.getenv("HEALTH_RISK_NARRATIVE_THRESHOLD", "6")),
        store_llm_narrative_in_supabase=_env_truthy("HEALTH_RISK_STORE_LLM_IN_SUPABASE", False),
    )
