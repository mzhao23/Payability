import logging
from config.settings import RISK_THRESHOLDS

logger = logging.getLogger(__name__)


def calculate_risk_level(metric_results: dict) -> dict:
    """
    Calculate overall risk level per supplier based on all metric results.

    Args:
        metric_results: {
            "metric_1": { supplier_key -> row },
            "metric_2": { supplier_key -> row },
            "metric_3": { supplier_key -> { pickup_lag: row, stuck_orders: row } },
        }

    Returns:
        List of risk score dicts, one per supplier
    """
    all_suppliers = set()
    for key, rows in metric_results.items():
        all_suppliers.update(rows.keys())

    risk_scores = []

    for supplier_key in all_suppliers:
        flags = []

        # ── Metric 1: Untracked rate ─────────────────────────
        m1 = metric_results.get("metric_1", {}).get(supplier_key, {})
        if m1:
            diff = m1.get("diff")
            untracked_rate = m1.get("untracked_rate")
            if diff is not None and diff >= RISK_THRESHOLDS["untracked_diff_high"]:
                flags.append("UNTRACKED_SPIKE")
            if untracked_rate is not None and untracked_rate >= RISK_THRESHOLDS["untracked_rate_high"]:
                flags.append("HIGH_UNTRACKED_RATE")

        # ── Metric 2: Price escalation ───────────────────────
        # Note: zscore = None means insufficient history (<30 days or only 1 data point)
        # These suppliers are skipped and not flagged
        m2 = metric_results.get("metric_2", {}).get(supplier_key, {})
        if m2:
            zscore = m2.get("zscore")
            max_zscore = m2.get("max_zscore")
            if zscore is not None and zscore >= RISK_THRESHOLDS["zscore_high"]:
                flags.append("PRICE_ESCALATION_AVG")
            if max_zscore is not None and max_zscore >= RISK_THRESHOLDS["max_zscore_high"]:
                flags.append("PRICE_ESCALATION_MAX")

        # ── Metric 3: FedEx Pickup Performance ───────────────
        m3 = metric_results.get("metric_3", {}).get(supplier_key, {})
        if m3:
            # 3A: Pickup lag spike
            lag_row = m3.get("pickup_lag", {})
            lag_diff = lag_row.get("diff")
            if lag_diff is not None and lag_diff >= RISK_THRESHOLDS["pickup_lag_diff_high"]:
                flags.append("PICKUP_LAG_SPIKE")

            # 3B: Stuck orders (FedEx baseline = 0, any stuck = HIGH)
            stuck_row = m3.get("stuck_orders", {})
            stuck_count = stuck_row.get("stuck_order_count")
            if stuck_count is not None and stuck_count > 0:
                flags.append("FEDEX_STUCK_ORDERS")

        # ── Overall risk level ───────────────────────────────
        flag_count = len(flags)
        if flag_count == 0:
            risk_level = "LOW"
        elif flag_count == 1:
            risk_level = "MEDIUM"
        elif flag_count == 2:
            risk_level = "HIGH"
        else:
            risk_level = "CRITICAL"

        risk_scores.append({
            "supplier_key": supplier_key,
            "risk_level": risk_level,
            "risk_flags": flags,
            "flag_count": flag_count,
            # Metric 1 snapshot
            "untracked_rate": m1.get("untracked_rate"),
            "untracked_diff": m1.get("diff"),
            # Metric 2 snapshot
            "avg_order_value": m2.get("avg_order_value"),
            "price_zscore": m2.get("zscore"),
            "price_max_zscore": m2.get("max_zscore"),
            # Metric 3 snapshot
            "pickup_lag_diff": m3.get("pickup_lag", {}).get("diff") if m3 else None,
            "stuck_order_count": m3.get("stuck_orders", {}).get("stuck_order_count") if m3 else None,
        })

    logger.info(f"  → Scored {len(risk_scores)} suppliers")
    return risk_scores