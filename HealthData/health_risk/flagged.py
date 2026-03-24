from __future__ import annotations

from typing import Any, Dict, List

from health_risk.utils import utc_now_iso


def is_high_risk(row: Dict[str, Any]) -> bool:
    score = row.get("risk_score")
    if score is None:
        return False
    return score > 5


def build_consolidated_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "supplier_key": row.get("supplier_key"),
        "source": "health_report",
        "supplier_name": row.get("supplier_name"),
        "created_at": utc_now_iso(),
        "run_id": None,
        "metrics": {
            "top_risk_drivers": row.get("top_risk_drivers"),
            "driver_1": row.get("driver_1"),
            "driver_2": row.get("driver_2"),
            "driver_3": row.get("driver_3"),
            "red_metric_count": row.get("red_metric_count"),
            "yellow_metric_count": row.get("yellow_metric_count"),
            "pipeline_version": row.get("pipeline_version"),
            "high_risk_narrative_llm": row.get("high_risk_narrative_llm"),
        },
        "reasons": row.get("top_risk_drivers") or [],
        "overall_risk_score": row.get("risk_score"),
        "status": "pending_review",
    }


def build_consolidated_flagged_rows(payload: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        build_consolidated_row(row)
        for row in payload
        if is_high_risk(row) and row.get("supplier_key")
    ]
