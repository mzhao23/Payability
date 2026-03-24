from __future__ import annotations

from typing import Any, Dict, List


def filter_active_population(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Exclude Suspended and Pending accounts based on risk team feedback."""
    kept = []
    for row in rows:
        status = str(row.get("payability_status") or "").strip().lower()
        if status in {"suspended", "pending"}:
            continue
        kept.append(row)
    return kept
