"""
Risk Metrics Pipeline — Main Entry Point
Runs daily via Cron Job to calculate risk metrics and store in Supabase.

Usage:
    python pipeline.py

Cron (daily at 6am UTC):
    0 6 * * * cd /path/to/risk_metrics && python pipeline.py
"""

import logging
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

    # ── Write unified daily snapshot ─────────────────────────
    logger.info("\n[supplier_daily_metrics] Merging and writing daily snapshot...")
    merged = {}
    for row in rows_1 + rows_2 + rows_3:
        key = (row["run_date"], row["supplier_key"], row["carrier"])
        if key not in merged:
            merged[key] = row
        else:
            merged[key].update(row)

    sb.upsert("supplier_daily_metrics", list(merged.values()))

    # ── LLM Risk Scoring ─────────────────────────────────────
    logger.info("\n[LLM Scorer] Running LLM risk scoring...")
    risk_scores = llm_scorer.run(sb)
    sb.upsert("supplier_risk_scores", risk_scores)

    # ── Summary ──────────────────────────────────────────────
    logger.info("\n✅ Pipeline completed successfully!")
    logger.info(f"   Suppliers scored: {len(risk_scores)}")


if __name__ == "__main__":
    run_pipeline()
