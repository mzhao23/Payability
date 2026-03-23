import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from core.bigquery_client import BigQueryClient
from config.settings import PARAMS, BQ_TABLE

logger = logging.getLogger(__name__)

SQL_3A_PATH = os.path.join(os.path.dirname(__file__), "../queries/metric_3_pickup_lag.sql")
SQL_3B_PATH = os.path.join(os.path.dirname(__file__), "../queries/metric_3b_stuck_orders.sql")


def run(bq: BigQueryClient) -> tuple[list, dict]:
    """
    Run Metric 3: FedEx Pickup Performance.
    3A: Init to Pickup Lag Trend
    3B: Stuck Orders (no pickup after X days)
    Returns (unified_rows, risk_dict) where:
      - unified_rows: list of dicts with m3a_*/m3b_* columns for supplier_daily_metrics
      - risk_dict: { supplier_key -> { pickup_lag: row, stuck_orders: row } } for risk scoring
    """

    run_date = datetime.now(ZoneInfo("America/New_York")).date()

    # ── 3A: Pickup Lag Trend ─────────────────────────────────
    logger.info("[Metric 3A] Running FedEx Init to Pickup Lag...")
    rows_3a = bq.load_and_run(SQL_3A_PATH, {
        "table": BQ_TABLE,
    })

    # Build unified rows for 3A (by_supplier only)
    unified_by_supplier = {}
    for row in rows_3a:
        if row.get("result_type") != "by_supplier":
            continue
        key = row["supplier_key"]
        if key not in unified_by_supplier or row["order_date"] > unified_by_supplier[key]["order_date"]:
            unified_by_supplier[key] = row

    unified_3a = {
        key: {
            "run_date": run_date,
            "supplier_key": key,
            "carrier": "ALL",
            "m3a_avg_pickup_lag": row.get("avg_pickup_lag"),
            "m3a_max_pickup_lag": row.get("max_pickup_lag"),
            "m3a_total_packages": row.get("total_packages"),
            "m3a_rolling_avg_30d": row.get("rolling_avg_30d"),
            "m3a_diff": row.get("diff"),
            "last_purchase_date": row.get("order_date"),
        }
        for key, row in unified_by_supplier.items()
    }

    # Risk scoring dict for 3A
    latest_3a = {}
    for row in rows_3a:
        if row.get("result_type") != "by_supplier":
            continue
        key = row["supplier_key"]
        if key not in latest_3a or row["order_date"] > latest_3a[key]["order_date"]:
            latest_3a[key] = row
    logger.info(f"  → {len(latest_3a)} suppliers processed for 3A")

    # ── 3B: Stuck Orders ─────────────────────────────────────
    logger.info("[Metric 3B] Running FedEx Stuck Orders...")
    rows_3b = bq.load_and_run(SQL_3B_PATH, {
        "table": BQ_TABLE,
        "stuck_days": PARAMS["stuck_days"],
    })
    logger.info(f"  → {len(rows_3b)} suppliers with stuck orders")

    # Build unified rows for 3B
    unified_3b = {
        row["supplier_key"]: {
            "run_date": run_date,
            "supplier_key": row["supplier_key"],
            "carrier": "ALL",
            "m3b_stuck_order_count": row.get("stuck_order_count"),
            "m3b_total_fedex_orders": row.get("total_fedex_orders"),
            "m3b_stuck_rate": row.get("stuck_rate"),
        }
        for row in rows_3b
    }

    # Merge 3A and 3B unified rows by supplier_key
    all_suppliers = set(unified_3a.keys()) | set(unified_3b.keys())
    merged_unified = []
    for key in all_suppliers:
        row = {"run_date": run_date, "supplier_key": key, "carrier": "ALL"}
        if key in unified_3a:
            row.update(unified_3a[key])
        if key in unified_3b:
            row.update(unified_3b[key])
        merged_unified.append(row)

    # Merge risk scoring dicts
    stuck_by_supplier = {r["supplier_key"]: r for r in rows_3b}
    all_risk_suppliers = set(latest_3a.keys()) | set(stuck_by_supplier.keys())
    risk_dict = {
        key: {
            "pickup_lag": latest_3a.get(key, {}),
            "stuck_orders": stuck_by_supplier.get(key, {}),
        }
        for key in all_risk_suppliers
    }

    return merged_unified, risk_dict
