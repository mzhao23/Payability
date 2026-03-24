from __future__ import annotations

import json
from typing import Any, Dict, List

from health_risk.config import Settings


def build_unified_json_row(payload_row: Dict[str, Any], settings: Settings) -> Dict[str, Any]:
    row = {
        "table_name": settings.bq_table,
        "supplier_key": payload_row.get("supplier_key"),
        "mp_sup_key": payload_row.get("mp_sup_key"),
        "supplier_name": payload_row.get("supplier_name"),
        "payability_status": payload_row.get("payability_status"),
        "report_date": payload_row["report_date"],
        "overall_risk_score": payload_row["risk_score"],
        "risk_level": payload_row["risk_level"],
        "risk_reason": payload_row["risk_reason"],
        "top_risk_drivers": payload_row["top_risk_drivers"],
    }
    if payload_row.get("high_risk_narrative_llm"):
        row["high_risk_narrative_llm"] = payload_row["high_risk_narrative_llm"]
    return row


def export_unified_json(
    payload: List[Dict[str, Any]],
    settings: Settings,
    output_file: str = "risk_output.json",
) -> int:
    out = [build_unified_json_row(p, settings) for p in payload]
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[OK] Exported {len(out)} records to {output_file}")
    return len(out)
