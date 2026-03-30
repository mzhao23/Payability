from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime

from dotenv import load_dotenv

from db import get_flagged_suppliers_today, upsert_decision_report
from llm import call_llm
from profile_builder import build_supplier_profile

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("decision-agent")

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))


def _resolve_report_date() -> date:
    raw = os.getenv("REPORT_DATE")
    if raw:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    return date.today()


def process_one(supplier: dict[str, str], report_date: date) -> None:
    supplier_key = supplier["supplier_key"]
    supplier_name = supplier.get("supplier_name") or ""
    try:
        profile = build_supplier_profile(supplier_key, supplier_name, report_date)
        llm_result = call_llm(profile)
        upsert_decision_report(
            supplier_key=supplier_key,
            supplier_name=supplier_name,
            report_date=report_date,
            final_score=llm_result["final_score"],
            reason=llm_result["reason"],
            today_scores=profile["today"],
            history_7d=profile["history_7d"],
            resonance_count=profile["resonance_count"],
        )
        log.info("Processed supplier_key=%s", supplier_key)
    except Exception as exc:  # noqa: BLE001
        log.exception("Failed supplier_key=%s error=%s", supplier_key, exc)


def main() -> None:
    report_date = _resolve_report_date()
    suppliers = get_flagged_suppliers_today(report_date)
    log.info("Start run report_date=%s supplier_count=%d", report_date.isoformat(), len(suppliers))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for supplier in suppliers:
            pool.submit(process_one, supplier, report_date)

    log.info("Run completed report_date=%s", report_date.isoformat())


if __name__ == "__main__":
    main()
