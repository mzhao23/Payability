import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from core.bigquery_client import BigQueryClient
from config.settings import PARAMS, BQ_TABLE

logger = logging.getLogger(__name__)

SQL_PATH = os.path.join(os.path.dirname(__file__), "../queries/metric_1_untracked.sql")


def run(bq: BigQueryClient) -> tuple[list, dict, dict]:
    """
    Run Metric 1: Untracked Order Rate.
    Returns (unified_rows, risk_dict, carrier_baseline) where:
      - unified_rows: list of dicts with m1_* columns for supplier_daily_metrics
      - risk_dict: { supplier_key -> row } for risk scoring (target date only)
      - carrier_baseline: { carrier_normalized -> row } carrier-level rates for target date
    """
    logger.info("[Metric 1] Running Untracked Order Rate...")

    rows = bq.load_and_run(SQL_PATH, {
        "table": BQ_TABLE,
        "ship_sla_days": PARAMS["ship_sla_days"],
        "window_days": PARAMS["window_days"],
    })

    run_date = datetime.now(ZoneInfo("America/New_York")).date()

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
            "m1_order_volume_7d": row.get("order_volume_7d"),
            "m1_order_volume_7d_change_rate": row.get("order_volume_7d_change_rate"),
            "m1_result_type": row.get("result_type"),
            "last_purchase_date": row.get("order_date"),
        })

    # Return only by_supplier rows for the target date (CURRENT_DATE - ship_sla_days) for risk scoring
    # Suppliers with no orders on the target date are excluded
    target_date = run_date - timedelta(days=PARAMS["ship_sla_days"])
    latest = {
        row["supplier_key"]: row
        for row in rows
        if row.get("result_type") == "by_supplier" and row.get("order_date") == target_date
    }

    # Carrier-level baseline for the same target date (for systemic anomaly detection)
    carrier_baseline = {
        row["carrier_normalized"]: row
        for row in rows
        if row.get("result_type") == "by_carrier" and row.get("order_date") == target_date
    }

    logger.info(f"  → {len(latest)} suppliers processed, {len(carrier_baseline)} carriers in baseline")
    return unified_rows, latest, carrier_baseline
