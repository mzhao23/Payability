import logging
import os
from datetime import date
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
    })

    run_date = date.today()

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
            "m2_high_price_risk": row.get("high_price_risk"),
        })

    # Return latest row per supplier for risk scoring
    latest = {}
    for row in rows:
        key = row["supplier_key"]
        if key not in latest or row["order_date"] > latest[key]["order_date"]:
            latest[key] = row

    logger.info(f"  → {len(latest)} suppliers processed")
    return unified_rows, latest
