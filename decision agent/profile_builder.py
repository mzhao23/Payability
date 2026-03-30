from __future__ import annotations

from datetime import date
from typing import Any

from db import get_history_7d, get_today_scores


def build_supplier_profile(
    supplier_key: str,
    supplier_name: str,
    report_date: date,
) -> dict[str, Any]:
    today = get_today_scores(supplier_key=supplier_key, report_date=report_date)
    history_7d = get_history_7d(supplier_key=supplier_key, report_date=report_date)
    resonance_count = sum(1 for item in today.values() if item.get("flagged") is True)

    return {
        "supplier_key": supplier_key,
        "supplier_name": supplier_name,
        "report_date": report_date.isoformat(),
        "today": today,
        "history_7d": history_7d,
        "resonance_count": resonance_count,
    }
