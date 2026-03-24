from __future__ import annotations

import os
from dataclasses import dataclass


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

    @property
    def bq_full_table(self) -> str:
        return f"`{self.bq_project}.{self.bq_dataset}.{self.bq_table}`"


def load_settings() -> Settings:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.")

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
    )
