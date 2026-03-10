import logging
import os
from datetime import date
from core.bigquery_client import BigQueryClient
from config.settings import PARAMS, BQ_TABLE

logger = logging.getLogger(__name__)

SQL_PATH = os.path.join(os.path.dirname(__file__), "../queries/metric_1_untracked.sql")


def run(bq: BigQueryClient) -> tuple[list, dict]:
    """
    Run Metric 1: Untracked Order Rate.
    Returns (unified_rows, risk_dict) where:
      - unified_rows: list of dicts with m1_* columns for supplier_daily_metrics
      - risk_dict: { supplier_key -> latest row } for risk scoring
    """
    logger.info("[Metric 1] Running Untracked Order Rate...")

    rows = bq.load_and_run(SQL_PATH, {
        "table": BQ_TABLE,
        "ship_sla_days": PARAMS["ship_sla_days"],
        "window_days": PARAMS["window_days"],
    })

    run_date = date.today()

    # Build unified rows for supplier_daily_metrics
    unified_rows = []
    for row in rows:
        unified_rows.append({
            "run_date": run_date,
            "supplier_key": row.get("supplier_key"),
            "carrier": row.get("carrier_normalized", "ALL"),
            "m1_total_orders": row.get("total_orders"),
            "m1_untracked_orders": row.get("untracked_orders"),
            "m1_untracked_rate": row.get("untracked_rate"),
            "m1_rolling_avg_30d": row.get("rolling_avg_30d"),
            "m1_diff": row.get("diff"),
            "m1_result_type": row.get("result_type"),
        })

    # Return only by_supplier rows, latest per supplier, for risk scoring
    supplier_rows = [r for r in rows if r.get("result_type") == "by_supplier"]
    latest = {}
    for row in supplier_rows:
        key = row["supplier_key"]
        if key not in latest or row["order_date"] > latest[key]["order_date"]:
            latest[key] = row

    logger.info(f"  → {len(latest)} suppliers processed")
    return unified_rows, latest
