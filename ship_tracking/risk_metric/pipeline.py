"""
Risk Metrics Pipeline — Main Entry Point
Runs daily via Cron Job to calculate risk metrics and store in Supabase.

Usage:
    python pipeline.py

Cron (daily at 6am UTC):
    0 6 * * * cd /path/to/risk_metrics && python pipeline.py
"""

import logging
import uuid
from collections import defaultdict
from datetime import date

from core.bigquery_client import BigQueryClient
from core.supabase_client import SupabaseClient
from core import llm_scorer
from metrics import metric_1_untracked, metric_2_price, metric_3_pickup_lag

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def run_pipeline():
    logger.info("=" * 60)
    logger.info(f"Risk Metrics Pipeline — {date.today()}")
    logger.info("=" * 60)

    bq = BigQueryClient()
    sb = SupabaseClient()

    # ── Metric 1: Untracked Order Rate ──────────────────────
    rows_1, _ = metric_1_untracked.run(bq)

    # ── Metric 2: Price Escalation ───────────────────────────
    rows_2, _ = metric_2_price.run(bq)

    # ── Metric 3: FedEx Pickup Performance ──────────────────
    rows_3, _ = metric_3_pickup_lag.run(bq)

    # ── Group BQ rows by supplier for LLM scorer ─────────────
    run_id = str(uuid.uuid4())
    supplier_rows = defaultdict(list)
    for row in rows_1 + rows_2 + rows_3:
        if row.get("supplier_key"):
            supplier_rows[row["supplier_key"]].append(row)

    # ── LLM Risk Scoring ─────────────────────────────────────
    logger.info("\n[LLM Scorer] Running LLM risk scoring...")
    risk_scores = llm_scorer.run(dict(supplier_rows))
    for row in risk_scores:
        row["run_id"] = run_id

    # ── Backfill supplier names from BigQuery ─────────────────
    logger.info("\n[Supplier Names] Fetching name map from BigQuery...")
    name_rows = bq.run_query("""
        SELECT DISTINCT supplier_key, supplier_name
        FROM `bigqueryexport-183608.PayabilitySheets.vm_transaction_summary`
        WHERE supplier_name IS NOT NULL AND supplier_key IS NOT NULL
    """)
    name_map = {str(r["supplier_key"]): r["supplier_name"] for r in name_rows}
    logger.info(f"  → {len(name_map)} supplier names loaded")

    for row in risk_scores:
        row["supplier_name"] = name_map.get(str(row["supplier_key"]))

    sb.upsert("ship_risk_scores", risk_scores)

    # ── Write high-risk suppliers to alerts table ─────────────
    from datetime import datetime
    from zoneinfo import ZoneInfo
    alerts = [
        {
            "supplier_key": r["supplier_key"],
            "supplier_name": r["supplier_name"],
            "source": "ship_tracking",
            "created_at": datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S"),
            "metrics": r["metrics"],
            "reasons": [r["trigger_reason"]] if r.get("trigger_reason") else [],
            "overall_risk_score": r["overall_risk_score"],
        }
        for r in risk_scores if float(r.get("overall_risk_score", 0)) >= 5
    ]
    if alerts:
        logger.info(f"\n[Alerts] Writing {len(alerts)} high-risk suppliers to consolidated_flagged_supplier_list...")
        sb.upsert("consolidated_flagged_supplier_list", alerts)

    # ── Summary ──────────────────────────────────────────────
    logger.info("\n✅ Pipeline completed successfully!")
    logger.info(f"   Suppliers scored: {len(risk_scores)}")


if __name__ == "__main__":
    run_pipeline()
