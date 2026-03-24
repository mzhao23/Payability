import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from core.bigquery_client import BigQueryClient
from config.settings import PARAMS, BQ_TABLE

logger = logging.getLogger(__name__)

SQL_PATH = os.path.join(os.path.dirname(__file__), "../queries/metric_2_price.sql")


def run(bq: BigQueryClient) -> tuple[list, dict]:
    """
    Run Metric 2: Price Escalation Detection.
    Returns (unified_rows, risk_dict) where:
      - unified_rows: list of dicts with m2_* columns for supplier_daily_metrics
      - risk_dict: { supplier_key -> latest row } for risk scoring
    """
    logger.info("[Metric 2] Running Price Escalation Detection...")

    rows = bq.load_and_run(SQL_PATH, {
        "table": BQ_TABLE,
        "baseline_days": PARAMS["baseline_days"],
        "zscore_threshold": PARAMS["zscore_threshold"],
        "ship_sla_days": PARAMS["ship_sla_days"],
    })

    run_date = datetime.now(ZoneInfo("America/New_York")).date()

    # Build unified rows for supplier_daily_metrics
    unified_rows = []
    for row in rows:
        unified_rows.append({
            "run_date": run_date,
            "supplier_key": row.get("supplier_key"),
            "carrier": "ALL",
            "m2_avg_order_value": row.get("avg_order_value"),
            "m2_total_orders": row.get("total_orders"),
            "m2_zscore": row.get("zscore"),
            "m2_max_zscore": row.get("max_zscore"),
            "m2_avg_of_avg": row.get("avg_of_avg"),
            "m2_high_price_risk": row.get("high_price_risk"),
            "last_purchase_date": row.get("order_date"),
        })

    # Return rows for the target date only (aligned with metric_1)
    target_date = run_date - timedelta(days=PARAMS["ship_sla_days"])
    latest = {
        row["supplier_key"]: row
        for row in rows
        if row.get("order_date") == target_date
    }

    logger.info(f"  → {len(latest)} suppliers processed")
    return unified_rows, latest
