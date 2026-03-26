from __future__ import annotations

from typing import Any, Dict, List, Optional

from health_risk.utils import utc_now_iso


_METRIC_UNIT_MAP: Dict[str, str] = {
    "ORDER_DEFECT_RATE_60": "rate",
    "CHARGEBACK_RATE_90": "rate",
    "A_TO_Z_CLAIM_RATE_90": "rate",
    "NEGATIVE_FEEDBACK_RATE_90": "rate",
    "LATE_SHIPMENT_RATE_30": "rate",
    "PRE_FULFILL_CANCEL_RATE_30": "rate",
    "AVG_RESPONSE_HOURS_30": "hours",
    "NO_RESPONSE_OVER_24H_30": "count",
    "VALID_TRACKING_RATE_30": "rate",
    "ON_TIME_DELIVERY_RATE_30": "rate",
    "PRODUCT_SAFETY_STATUS": "status",
    "PRODUCT_AUTHENTICITY_STATUS": "status",
    "POLICY_VIOLATION_STATUS": "status",
    "LISTING_POLICY_STATUS": "status",
    "INTELLECTUAL_PROPERTY_STATUS": "status",
}


def is_high_risk(row: Dict[str, Any]) -> bool:
    score = row.get("risk_score")
    if score is None:
        return False
    return score > 6


def _format_reasons(row: Dict[str, Any]) -> List[str]:
    narrative = row.get("high_risk_narrative_llm")
    if narrative and isinstance(narrative, str):
        parts: List[str] = []
        for line in narrative.split("\n"):
            line = line.strip()
            if not line:
                continue
            if line.startswith("•"):
                line = line[1:].strip()
            parts.append(line)
        return parts if parts else [narrative]
    drivers = row.get("top_risk_drivers")
    if isinstance(drivers, list):
        return drivers
    return []


def _build_flagged_metrics(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build metrics list containing only unhealthy (subscore > 0) indicators."""
    subscores: Optional[Dict[str, float]] = row.get("_subscores")
    metric_values: Optional[Dict[str, Any]] = row.get("_metric_values")
    if not subscores or not metric_values:
        return []
    metrics: List[Dict[str, Any]] = []
    for metric_id, subscore in subscores.items():
        if subscore <= 0:
            continue
        raw = metric_values.get(metric_id)
        unit = _METRIC_UNIT_MAP.get(metric_id, "unknown")
        if unit == "rate" and isinstance(raw, (int, float)):
            raw = round(raw * 100, 2)
            unit = "%"
        metrics.append({
            "metric_id": metric_id.lower(),
            "value": raw,
            "unit": unit,
        })
    return metrics


def build_consolidated_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "supplier_key": row.get("supplier_key"),
        "source": "health_report",
        "supplier_name": row.get("supplier_name"),
        "created_at": utc_now_iso(),
        "run_id": None,
        "metrics": _build_flagged_metrics(row),
        "reasons": _format_reasons(row),
        "overall_risk_score": row.get("risk_score"),
        "status": "pending_review",
    }


def build_consolidated_flagged_rows(payload: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        build_consolidated_row(row)
        for row in payload
        if is_high_risk(row) and row.get("supplier_key")
    ]
