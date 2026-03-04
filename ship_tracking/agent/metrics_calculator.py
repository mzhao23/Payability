# ============================================================
# metrics_calculator.py
# Calculates summary metrics and extracts anomalies from
# raw SQL results using statistical methods.
#
# Responsibilities:
# 1. Calculate fixed metrics for Supabase storage
# 2. Use statistics (mean + std dev) to find true anomalies
# 3. Build compact Gemini input (never raw rows)
# ============================================================

import yaml
import statistics
from pathlib import Path

CONFIG_DIR = Path(__file__).parent / "config"
TOP_N_ANOMALIES = 10  # Max anomalies per category to send to Gemini


def load_thresholds() -> dict:
    with open(CONFIG_DIR / "risk_focus.yaml") as f:
        config = yaml.safe_load(f)
    return config["thresholds"]


# ── Helper Functions ──────────────────────────────────────

def safe_mean(values: list) -> float | None:
    cleaned = [v for v in values if v is not None]
    return round(statistics.mean(cleaned), 4) if len(cleaned) > 1 else None


def safe_stdev(values: list) -> float | None:
    cleaned = [v for v in values if v is not None]
    return round(statistics.stdev(cleaned), 4) if len(cleaned) > 1 else None


def find_outliers(rows: list, field: str, top_n: int = TOP_N_ANOMALIES) -> list:
    """
    Find outliers using mean + 1.5 standard deviations.
    Returns top N worst offenders sorted by severity.
    """
    values = [r.get(field) for r in rows if r.get(field) is not None]
    if len(values) < 3:
        return []

    avg = statistics.mean(values)
    std = statistics.stdev(values)
    threshold = avg + 1.5 * std

    outliers = [r for r in rows if r.get(field) and r.get(field) > threshold]
    outliers_sorted = sorted(outliers, key=lambda x: x.get(field, 0), reverse=True)
    return outliers_sorted[:top_n]


# ── Step 1: Calculate Fixed Metrics for Supabase ─────────

def calculate_metrics(sql_results: dict) -> dict:
    """
    Extract key summary metrics from SQL results.
    These are the fixed fields stored in Supabase dashboard.
    All values are aggregated across all suppliers.
    """
    metrics = {
        "untracked_rate": None,
        "untracked_rate_delta": None,
        "avg_order_value": None,
        "avg_order_value_delta": None,
        "avg_init_to_pickup_hours": None,
        "avg_pickup_to_delivery_hours": None,
        "overdue_unpickup_count": None,
    }

    # ── Untracked Orders ──────────────────────────────────
    rows = sql_results.get("untracked_orders", [])
    if isinstance(rows, list) and rows:
        current_rates = [r.get("untracked_rate") for r in rows
                         if r.get("untracked_rate") is not None]
        hist_rates = [r.get("historical_untracked_rate") for r in rows
                      if r.get("historical_untracked_rate") is not None]

        if current_rates:
            avg_current = statistics.mean(current_rates)
            metrics["untracked_rate"] = round(avg_current, 4)

        if current_rates and hist_rates:
            avg_hist = statistics.mean(hist_rates)
            metrics["untracked_rate_delta"] = round(
                avg_current - avg_hist, 4)

    # ── High Value Items ──────────────────────────────────
    rows = sql_results.get("high_value_items", [])
    if isinstance(rows, list) and rows:
        values_7d = [r.get("avg_order_value_7d") for r in rows
                     if r.get("avg_order_value_7d") is not None]
        values_30d = [r.get("avg_order_value_30d") for r in rows
                      if r.get("avg_order_value_30d") is not None]

        if values_7d:
            avg_7d = statistics.mean(values_7d)
            metrics["avg_order_value"] = round(avg_7d, 2)

        if values_7d and values_30d:
            avg_30d = statistics.mean(values_30d)
            if avg_30d > 0:
                metrics["avg_order_value_delta"] = round(
                    (avg_7d - avg_30d) / avg_30d, 4)

    # ── Logistics Timing ──────────────────────────────────
    rows = sql_results.get("logistics_timing", [])
    if isinstance(rows, list) and rows:
        pickup_times = [r.get("avg_init_to_pickup_hours") for r in rows
                        if r.get("avg_init_to_pickup_hours") is not None]
        delivery_times = [r.get("avg_pickup_to_delivery_hours") for r in rows
                          if r.get("avg_pickup_to_delivery_hours") is not None]
        overdue_counts = [r.get("overdue_unpickup_count") for r in rows
                          if r.get("overdue_unpickup_count") is not None
                          and r.get("overdue_unpickup_count") > 0]

        if pickup_times:
            metrics["avg_init_to_pickup_hours"] = round(
                statistics.mean(pickup_times), 2)
        if delivery_times:
            metrics["avg_pickup_to_delivery_hours"] = round(
                statistics.mean(delivery_times), 2)

        # Count of suppliers with at least one overdue order today
        metrics["overdue_unpickup_count"] = len(overdue_counts)

    return metrics


# ── Step 2: Extract Anomalies for Gemini ─────────────────

def extract_anomalies(sql_results: dict) -> dict:
    """
    Use statistical methods to find true anomalies.
    Returns compact summaries, never raw rows.
    Max TOP_N_ANOMALIES per category.
    """
    thresholds = load_thresholds()

    anomalies = {
        "untracked_orders": {},
        "high_value_items": {},
        "logistics_timing": {},
        "carrier_breakdown": {},
        "order_package_ratio": {},
    }

    # ── Untracked Orders ──────────────────────────────────
    rows = sql_results.get("untracked_orders", [])
    if isinstance(rows, list) and rows:
        current_rates = [r.get("untracked_rate") for r in rows
                         if r.get("untracked_rate") is not None]
        avg = safe_mean(current_rates)
        std = safe_stdev(current_rates)

        # Flag: above alert threshold OR statistical outlier
        flagged = []
        for r in rows:
            rate = r.get("untracked_rate")
            hist = r.get("historical_untracked_rate")
            if rate is None:
                continue
            is_above_threshold = rate > thresholds["untracked_rate_alert"]
            is_outlier = avg and std and rate > avg + 1.5 * std
            is_spike = hist and rate > hist * 2  # doubled vs historical

            if is_above_threshold or is_outlier or is_spike:
                flagged.append({
                    "supplier_key": r.get("supplier_key"),
                    "untracked_rate": round(rate, 4),
                    "historical_rate": round(hist, 4) if hist else None,
                })

        flagged_sorted = sorted(
            flagged, key=lambda x: x["untracked_rate"], reverse=True
        )[:TOP_N_ANOMALIES]

        anomalies["untracked_orders"] = {
            "total_suppliers": len(rows),
            "flagged_count": len(flagged),
            "overall_avg_rate": avg,
            "top_offenders": flagged_sorted,
        }

    # ── High Value Items ──────────────────────────────────
    rows = sql_results.get("high_value_items", [])
    if isinstance(rows, list) and rows:
        flagged = []
        for r in rows:
            val_7d = r.get("avg_order_value_7d")
            val_30d = r.get("avg_order_value_30d")
            if val_7d is None or val_30d is None or val_30d == 0:
                continue
            pct_change = (val_7d - val_30d) / val_30d
            if pct_change > thresholds["cost_increase_alert"]:
                flagged.append({
                    "supplier_key": r.get("supplier_key"),
                    "avg_7d": round(val_7d, 2),
                    "avg_30d": round(val_30d, 2),
                    "pct_increase": round(pct_change, 4),
                })

        flagged_sorted = sorted(
            flagged, key=lambda x: x["pct_increase"], reverse=True
        )[:TOP_N_ANOMALIES]

        anomalies["high_value_items"] = {
            "total_suppliers": len(rows),
            "flagged_count": len(flagged),
            "top_offenders": flagged_sorted,
        }

    # ── Logistics Timing ──────────────────────────────────
    rows = sql_results.get("logistics_timing", [])
    if isinstance(rows, list) and rows:

        # Find statistical outliers for pickup time
        pickup_outliers = find_outliers(rows, "avg_init_to_pickup_hours")

        # Find suppliers with overdue orders
        overdue_suppliers = [
            {
                "supplier_key": r.get("supplier_key"),
                "overdue_count": r.get("overdue_unpickup_count"),
                "avg_init_to_pickup_hours": r.get("avg_init_to_pickup_hours"),
            }
            for r in rows
            if r.get("overdue_unpickup_count") and
            r.get("overdue_unpickup_count") > 0
        ]
        overdue_sorted = sorted(
            overdue_suppliers,
            key=lambda x: x["overdue_count"],
            reverse=True
        )[:TOP_N_ANOMALIES]

        all_pickup = [r.get("avg_init_to_pickup_hours") for r in rows
                      if r.get("avg_init_to_pickup_hours") is not None]
        all_delivery = [r.get("avg_pickup_to_delivery_hours") for r in rows
                        if r.get("avg_pickup_to_delivery_hours") is not None]

        anomalies["logistics_timing"] = {
            "total_suppliers": len(rows),
            "overall_avg_init_to_pickup_hours": safe_mean(all_pickup),
            "overall_avg_pickup_to_delivery_hours": safe_mean(all_delivery),
            "suppliers_with_overdue_orders": len(overdue_suppliers),
            "pickup_time_outliers_count": len(pickup_outliers),
            "top_overdue_suppliers": overdue_sorted,
            "top_slow_pickup_suppliers": [
                {
                    "supplier_key": r.get("supplier_key"),
                    "avg_init_to_pickup_hours": r.get("avg_init_to_pickup_hours"),
                }
                for r in pickup_outliers
            ],
        }

    # ── Carrier Breakdown ─────────────────────────────────
    rows = sql_results.get("carrier_breakdown", [])
    if isinstance(rows, list) and rows:

        # Group by carrier to detect systemic issues
        carrier_data = {}
        for r in rows:
            carrier = r.get("carrier")
            if not carrier:
                continue
            if carrier not in carrier_data:
                carrier_data[carrier] = {
                    "pickup_times": [],
                    "delivery_times": [],
                    "supplier_count": 0,
                }
            carrier_data[carrier]["supplier_count"] += 1
            if r.get("avg_init_to_pickup_hours"):
                carrier_data[carrier]["pickup_times"].append(
                    r.get("avg_init_to_pickup_hours"))
            if r.get("avg_pickup_to_delivery_hours"):
                carrier_data[carrier]["delivery_times"].append(
                    r.get("avg_pickup_to_delivery_hours"))

        # Summarize per carrier
        carrier_summary = []
        for carrier, data in carrier_data.items():
            avg_pickup = safe_mean(data["pickup_times"])
            avg_delivery = safe_mean(data["delivery_times"])
            carrier_summary.append({
                "carrier": carrier,
                "affected_suppliers": data["supplier_count"],
                "avg_init_to_pickup_hours": avg_pickup,
                "avg_pickup_to_delivery_hours": avg_delivery,
            })

        # Sort by pickup time to highlight slowest carriers
        carrier_summary_sorted = sorted(
            carrier_summary,
            key=lambda x: x.get("avg_init_to_pickup_hours") or 0,
            reverse=True
        )

        anomalies["carrier_breakdown"] = {
            "total_carriers": len(carrier_data),
            "carrier_summary": carrier_summary_sorted,
        }

    # ── Order Package Ratio ───────────────────────────────
    rows = sql_results.get("order_package_ratio", [])
    if isinstance(rows, list) and rows:
        flagged = []
        for r in rows:
            current = r.get("avg_packages_per_order")
            historical = r.get("historical_avg_packages_per_order")
            if current is None or historical is None or historical == 0:
                continue
            pct_change = (current - historical) / historical
            if pct_change > 0.2:
                flagged.append({
                    "supplier_key": r.get("supplier_key"),
                    "current_avg": round(current, 2),
                    "historical_avg": round(historical, 2),
                    "pct_increase": round(pct_change, 4),
                })

        flagged_sorted = sorted(
            flagged, key=lambda x: x["pct_increase"], reverse=True
        )[:TOP_N_ANOMALIES]

        anomalies["order_package_ratio"] = {
            "total_suppliers": len(rows),
            "flagged_count": len(flagged),
            "top_offenders": flagged_sorted,
        }

    return anomalies


# ── Step 3: Build Compact Gemini Input ────────────────────

def build_gemini_input(sql_results: dict, metrics: dict) -> dict:
    """
    Build compact input for Gemini analysis.
    Always a fixed small size regardless of supplier count.
    """
    anomalies = extract_anomalies(sql_results)

    # Count total flagged issues
    total_flagged = sum([
        anomalies["untracked_orders"].get("flagged_count", 0),
        anomalies["high_value_items"].get("flagged_count", 0),
        anomalies["logistics_timing"].get("suppliers_with_overdue_orders", 0),
        anomalies["logistics_timing"].get("pickup_time_outliers_count", 0),
        anomalies["order_package_ratio"].get("flagged_count", 0),
    ])

    return {
        "summary_metrics": metrics,
        "total_issues_detected": total_flagged,
        "anomalies": anomalies,
    }