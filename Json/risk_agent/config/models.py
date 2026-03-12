"""config/models.py — Pydantic models for the risk report output schema."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class Metric(BaseModel):
    metric_id: str
    value: Any
    unit: Optional[str] = None


class RiskReport(BaseModel):
    table_name: str
    supplier_key: str
    supplier_name: str
    report_date: str  # ISO-8601 date string
    metrics: list[Metric] = Field(default_factory=list)
    trigger_reason: str
    overall_risk_score: int = Field(ge=1, le=10)

    # Internal housekeeping — stored in Supabase but not part of the public spec
    mp_sup_key: Optional[str] = None              # raw key from BQ source table
    created_at: Optional[str] = Field(default_factory=lambda: datetime.utcnow().isoformat())
    raw_error: Optional[str] = None          # populated when data column contained an error
    data_quality_flag: Optional[str] = None  # e.g. "login_error", "bank_page_error", "ok"

    def to_supabase_dict(self) -> dict:
        """Return only the columns that exist in the Supabase risk table."""
        return {
            "table_name":         self.table_name,
            "supplier_key":       self.supplier_key,
            "mp_sup_key":         self.mp_sup_key,
            "supplier_name":      self.supplier_name,
            "report_date":        self.report_date,
            "metrics":            [m.model_dump() for m in self.metrics],
            "trigger_reason":     self.trigger_reason,
            "overall_risk_score": self.overall_risk_score,
        }