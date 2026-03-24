"""Health risk scoring pipeline: BigQuery ingestion, scoring, Supabase sinks."""

from __future__ import annotations

from typing import Any

__all__ = ["HealthRiskPipeline", "build_default_pipeline"]


def __getattr__(name: str) -> Any:
    if name == "HealthRiskPipeline":
        from health_risk.pipeline import HealthRiskPipeline

        return HealthRiskPipeline
    if name == "build_default_pipeline":
        from health_risk.bootstrap import build_default_pipeline

        return build_default_pipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
