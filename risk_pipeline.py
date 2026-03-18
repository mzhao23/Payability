import os
import math
import json
import time
import argparse
from datetime import date, timedelta, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery
from supabase import create_client


# ============================================================
# CONFIG
# ============================================================
BQ_PROJECT = "bigqueryexport-183608"
BQ_DATASET = "amazon"
BQ_TABLE = "customer_health_metrics"
BQ_FULL_TABLE = f"`{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"

SUPABASE_TABLE = "supplier_daily_risk"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.")

bq = bigquery.Client(project=BQ_PROJECT)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ============================================================
# METRIC CONFIG
# ============================================================
METRIC_CONFIG: List[Dict[str, Any]] = [
    # --------------------------
    # Outcome metrics
    # --------------------------
    {
        "metric_id": "ORDER_DEFECT_RATE_60",
        "source_column": "orderWithDefects_60_rate",
        "direction": "higher_is_worse",
        "group": "outcome",
        "description": "Core supplier health outcome metric.",
    },
    {
        "metric_id": "CHARGEBACK_RATE_90",
        "source_column": "chargebacks_90_rate",
        "direction": "higher_is_worse",
        "group": "outcome",
        "description": "Financial dispute sub-signal.",
    },
    {
        "metric_id": "A_TO_Z_CLAIM_RATE_90",
        "source_column": "a_z_claims_90_rate",
        "direction": "higher_is_worse",
        "group": "outcome",
        "description": "A-to-z claim sub-signal.",
    },
    {
        "metric_id": "NEGATIVE_FEEDBACK_RATE_90",
        "source_column": "negativeFeedbacks_90_rate",
        "direction": "higher_is_worse",
        "group": "outcome",
        "description": "Negative feedback sub-signal.",
    },

    # --------------------------
    # Operational metrics
    # --------------------------
    {
        "metric_id": "LATE_SHIPMENT_RATE_30",
        "source_column": "lateShipment_30_rate",
        "direction": "higher_is_worse",
        "group": "operational",
        "description": "Operational delay signal.",
    },
    {
        "metric_id": "PRE_FULFILL_CANCEL_RATE_30",
        "source_column": "preFulfillmentCancellation_30_rate",
        "direction": "higher_is_worse",
        "group": "operational",
        "description": "Inventory / pre-ship cancellation risk.",
    },
    {
        "metric_id": "AVG_RESPONSE_HOURS_30",
        "source_column": "averageResponseTimeInHours_30",
        "direction": "higher_is_worse",
        "group": "operational",
        "description": "Customer support response speed.",
    },
    {
        "metric_id": "NO_RESPONSE_OVER_24H_30",
        "source_column": "noResponseForContactsOlderThan24Hours_30",
        "direction": "higher_is_worse",
        "group": "operational",
        "description": "Severe support failure signal.",
    },
    {
        "metric_id": "VALID_TRACKING_RATE_30",
        "source_column": "validTracking_rate_30",
        "direction": "lower_is_worse",
        "group": "operational",
        "description": "Tracking compliance metric.",
    },
    {
        "metric_id": "ON_TIME_DELIVERY_RATE_30",
        "source_column": "onTimeDelivery_rate_30",
        "direction": "lower_is_worse",
        "group": "operational",
        "description": "Final fulfillment success metric.",
    },

    # --------------------------
    # Compliance metrics
    # --------------------------
    {
        "metric_id": "PRODUCT_SAFETY_STATUS",
        "source_column": "productSafetyStatus_status",
        "direction": "status",
        "group": "compliance",
        "description": "Product safety compliance risk.",
    },
    {
        "metric_id": "PRODUCT_AUTHENTICITY_STATUS",
        "source_column": "productAuthenticityStatus_status",
        "direction": "status",
        "group": "compliance",
        "description": "Product authenticity compliance risk.",
    },
    {
        "metric_id": "POLICY_VIOLATION_STATUS",
        "source_column": "policyViolation_status",
        "direction": "status",
        "group": "compliance",
        "description": "Policy violation compliance risk.",
    },
    {
        "metric_id": "LISTING_POLICY_STATUS",
        "source_column": "listingPolicyStatus_status",
        "direction": "status",
        "group": "compliance",
        "description": "Listing policy compliance risk.",
    },
    {
        "metric_id": "INTELLECTUAL_PROPERTY_STATUS",
        "source_column": "intellectualProperty_status",
        "direction": "status",
        "group": "compliance",
        "description": "Intellectual property compliance risk.",
    },
]


METRIC_ID_TO_GROUP = {m["metric_id"]: m["group"] for m in METRIC_CONFIG}


# ============================================================
# OPTIONAL STATUS SNAPSHOT
# ============================================================
BQ_STATUS_COLS = [
    "policyViolation_status",
    "listingPolicyStatus_status",
    "customerServiceDissatisfactionRate_status",
    "returnDissatisfactionRate_status",
    "contactResponseTime_status",
    "productSafetyStatus_status",
    "lateShipmentRate_status",
    "intellectualProperty_status",
    "orderCancellationRate_status",
    "orderDefectRate_status",
    "productAuthenticityStatus_status",
]


# ============================================================
# UTILS
# ============================================================
def iso(v: Any) -> Optional[str]:
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def backoff_sleep(attempt: int, base: float = 0.5, cap: float = 5.0) -> None:
    time.sleep(min(cap, base * (2 ** attempt)))

def pct_to_ratio(v: Any) -> Optional[float]:
    """
    Convert BigQuery percentage-style values to 0~1 ratio.
    Example:
      0.418  -> 0.00418   (0.418%)
      99.74  -> 0.9974    (99.74%)
    """
    x = safe_float(v)
    if x is None:
        return None
    return x / 100.0

# ============================================================
# BIGQUERY
# ============================================================
def get_latest_report_date() -> date:
    q = f"""
    SELECT MAX(DATE(snapshot_date)) AS report_date
    FROM {BQ_FULL_TABLE}
    """
    rows = list(bq.query(q).result())
    if not rows or rows[0]["report_date"] is None:
        raise RuntimeError("No snapshot_date found in BigQuery table.")
    return rows[0]["report_date"]


def fetch_latest_snapshot_per_supplier(report_date: date, limit: int = 5000) -> List[Dict[str, Any]]:
    metric_cols = [m["source_column"] for m in METRIC_CONFIG]
    # add order volume columns for activity adjustment
    metric_cols += ["orders_count_60", "orders_count_30", "orders_count_90"]

    selected_cols = ["mp_sup_key", "snapshot_date"] + metric_cols + BQ_STATUS_COLS
    selected_cols = list(dict.fromkeys(selected_cols))
    select_sql = ",\n        ".join([f"`{c}`" for c in selected_cols])

    q = f"""
    WITH ranked AS (
      SELECT
        DATE(snapshot_date) AS report_date,
        {select_sql},
        ROW_NUMBER() OVER (
          PARTITION BY mp_sup_key, DATE(snapshot_date)
          ORDER BY snapshot_date DESC
        ) AS rn
      FROM {BQ_FULL_TABLE}
      WHERE DATE(snapshot_date) = @report_date
    )
    SELECT *
    FROM ranked
    WHERE rn = 1
    LIMIT @lim
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("report_date", "DATE", report_date),
            bigquery.ScalarQueryParameter("lim", "INT64", limit),
        ]
    )

    rows = bq.query(q, job_config=job_config).result()
    return [dict(r) for r in rows]


# ============================================================
# SCORE FUNCTIONS
# ============================================================
def score_odr(odr: Optional[float]) -> float:
    if odr is None:
        return 0.0
    if odr == 0:
        return 0.0
    if odr < 0.005:
        return 1.0
    if odr < 0.01:
        return 3.0
    if odr < 0.02:
        return 6.0
    if odr < 0.03:
        return 8.0
    return 10.0


def score_chargeback(rate: Optional[float]) -> float:
    if rate is None:
        return 0.0
    if rate == 0:
        return 0.0
    if rate < 0.001:
        return 1.0
    if rate < 0.002:
        return 3.0
    if rate < 0.005:
        return 6.0
    return 9.0


def score_a_to_z(rate: Optional[float]) -> float:
    if rate is None:
        return 0.0
    if rate == 0:
        return 0.0
    if rate < 0.001:
        return 1.0
    if rate < 0.003:
        return 3.0
    if rate < 0.008:
        return 6.0
    return 9.0


def score_negative_feedback(rate: Optional[float]) -> float:
    if rate is None:
        return 0.0
    if rate == 0:
        return 0.0
    if rate < 0.005:
        return 1.0
    if rate < 0.01:
        return 3.0
    if rate < 0.03:
        return 6.0
    return 8.0


def score_late_shipment(rate: Optional[float]) -> float:
    if rate is None:
        return 0.0
    if rate < 0.02:
        return 0.0
    if rate < 0.04:
        return 3.0
    if rate < 0.08:
        return 6.0
    return 9.0


def score_cancellation(rate: Optional[float]) -> float:
    if rate is None:
        return 0.0
    if rate < 0.01:
        return 0.0
    if rate < 0.03:
        return 3.0
    if rate < 0.05:
        return 6.0
    return 9.0


def score_response_hours(hours: Optional[float]) -> float:
    if hours is None:
        return 0.0
    if hours < 12:
        return 0.0
    if hours < 24:
        return 3.0
    if hours < 48:
        return 6.0
    return 9.0


def score_no_response(count: Optional[float]) -> float:
    if count is None:
        return 0.0
    if count == 0:
        return 0.0
    if count < 5:
        return 3.0
    if count < 20:
        return 6.0
    return 9.0


def score_valid_tracking(rate: Optional[float]) -> float:
    if rate is None:
        return 0.0
    if rate >= 0.97:
        return 0.0
    if rate >= 0.94:
        return 3.0
    if rate >= 0.90:
        return 6.0
    return 9.0


def score_on_time_delivery(rate: Optional[float]) -> float:
    if rate is None:
        return 0.0
    if rate >= 0.95:
        return 0.0
    if rate >= 0.90:
        return 3.0
    if rate >= 0.85:
        return 6.0
    return 9.0


def score_status(value: Any) -> float:
    if value is None:
        return 0.0
    v = str(value).strip().lower()
    if v in {"good", "ok", "healthy"}:
        return 0.0
    if v in {"fair", "warning", "watch"}:
        return 5.0
    return 10.0


def activity_gate(order_count_60: Optional[float]) -> float:
    if order_count_60 is None:
        return 0.2
    if order_count_60 == 0:
        return 0.2
    if order_count_60 < 25:
        return 0.4
    if order_count_60 < 100:
        return 0.6
    if order_count_60 < 500:
        return 0.8
    return 1.0


def inactivity_penalty(order_count_60: Optional[float]) -> float:
    if order_count_60 is None:
        return 4.0
    if order_count_60 == 0:
        return 4.0
    if order_count_60 < 25:
        return 2.0
    return 0.0


def risk_level_from_score(score: float) -> str:
    if score < 2:
        return "Healthy"
    if score < 4:
        return "Watch"
    if score < 7:
        return "Risky"
    return "Critical"


# ============================================================
# CORE SCORING
# ============================================================
def score_supplier_row(row: Dict[str, Any]) -> Dict[str, Any]:
    # --------------------------
    # raw values
    # --------------------------
    metric_values: Dict[str, Any] = {
        "ORDER_DEFECT_RATE_60": pct_to_ratio(row.get("orderWithDefects_60_rate")),
        "CHARGEBACK_RATE_90": pct_to_ratio(row.get("chargebacks_90_rate")),
        "A_TO_Z_CLAIM_RATE_90": pct_to_ratio(row.get("a_z_claims_90_rate")),
        "NEGATIVE_FEEDBACK_RATE_90": pct_to_ratio(row.get("negativeFeedbacks_90_rate")),
        "LATE_SHIPMENT_RATE_30": pct_to_ratio(row.get("lateShipment_30_rate")),
        "PRE_FULFILL_CANCEL_RATE_30": pct_to_ratio(row.get("preFulfillmentCancellation_30_rate")),
        "AVG_RESPONSE_HOURS_30": safe_float(row.get("averageResponseTimeInHours_30")),
        "NO_RESPONSE_OVER_24H_30": safe_float(row.get("noResponseForContactsOlderThan24Hours_30")),
        "VALID_TRACKING_RATE_30": pct_to_ratio(row.get("validTracking_rate_30")),
        "ON_TIME_DELIVERY_RATE_30": pct_to_ratio(row.get("onTimeDelivery_rate_30")),
        "PRODUCT_SAFETY_STATUS": row.get("productSafetyStatus_status"),
        "PRODUCT_AUTHENTICITY_STATUS": row.get("productAuthenticityStatus_status"),
        "POLICY_VIOLATION_STATUS": row.get("policyViolation_status"),
        "LISTING_POLICY_STATUS": row.get("listingPolicyStatus_status"),
        "INTELLECTUAL_PROPERTY_STATUS": row.get("intellectualProperty_status"),
    }

    orders_count_60 = safe_float(row.get("orders_count_60"))
    orders_count_30 = safe_float(row.get("orders_count_30"))
    orders_count_90 = safe_float(row.get("orders_count_90"))

    # --------------------------
    # sub scores
    # --------------------------
    outcome_subscores = {
        "ORDER_DEFECT_RATE_60": score_odr(metric_values["ORDER_DEFECT_RATE_60"]),
        "CHARGEBACK_RATE_90": score_chargeback(metric_values["CHARGEBACK_RATE_90"]),
        "A_TO_Z_CLAIM_RATE_90": score_a_to_z(metric_values["A_TO_Z_CLAIM_RATE_90"]),
        "NEGATIVE_FEEDBACK_RATE_90": score_negative_feedback(metric_values["NEGATIVE_FEEDBACK_RATE_90"]),
    }

    operational_subscores = {
        "LATE_SHIPMENT_RATE_30": score_late_shipment(metric_values["LATE_SHIPMENT_RATE_30"]),
        "PRE_FULFILL_CANCEL_RATE_30": score_cancellation(metric_values["PRE_FULFILL_CANCEL_RATE_30"]),
        "AVG_RESPONSE_HOURS_30": score_response_hours(metric_values["AVG_RESPONSE_HOURS_30"]),
        "NO_RESPONSE_OVER_24H_30": score_no_response(metric_values["NO_RESPONSE_OVER_24H_30"]),
        "VALID_TRACKING_RATE_30": score_valid_tracking(metric_values["VALID_TRACKING_RATE_30"]),
        "ON_TIME_DELIVERY_RATE_30": score_on_time_delivery(metric_values["ON_TIME_DELIVERY_RATE_30"]),
    }

    compliance_subscores = {
        "PRODUCT_SAFETY_STATUS": score_status(metric_values["PRODUCT_SAFETY_STATUS"]),
        "PRODUCT_AUTHENTICITY_STATUS": score_status(metric_values["PRODUCT_AUTHENTICITY_STATUS"]),
        "POLICY_VIOLATION_STATUS": score_status(metric_values["POLICY_VIOLATION_STATUS"]),
        "LISTING_POLICY_STATUS": score_status(metric_values["LISTING_POLICY_STATUS"]),
        "INTELLECTUAL_PROPERTY_STATUS": score_status(metric_values["INTELLECTUAL_PROPERTY_STATUS"]),
    }

    # --------------------------
    # grouped scores
    # --------------------------
    outcome_score = round(
        0.70 * outcome_subscores["ORDER_DEFECT_RATE_60"]
        + 0.15 * outcome_subscores["CHARGEBACK_RATE_90"]
        + 0.10 * outcome_subscores["A_TO_Z_CLAIM_RATE_90"]
        + 0.05 * outcome_subscores["NEGATIVE_FEEDBACK_RATE_90"],
        2,
    )

    operational_score = round(
        0.25 * operational_subscores["LATE_SHIPMENT_RATE_30"]
        + 0.15 * operational_subscores["PRE_FULFILL_CANCEL_RATE_30"]
        + 0.15 * operational_subscores["AVG_RESPONSE_HOURS_30"]
        + 0.15 * operational_subscores["NO_RESPONSE_OVER_24H_30"]
        + 0.15 * operational_subscores["VALID_TRACKING_RATE_30"]
        + 0.15 * operational_subscores["ON_TIME_DELIVERY_RATE_30"],
        2,
    )

    comp_values = list(compliance_subscores.values())
    comp_max = max(comp_values) if comp_values else 0.0
    comp_avg = sum(comp_values) / len(comp_values) if comp_values else 0.0
    compliance_score = round(0.7 * comp_max + 0.3 * comp_avg, 2)

    # --------------------------
    # activity / inactivity
    # --------------------------
    act_gate = activity_gate(orders_count_60)
    inact_penalty = inactivity_penalty(orders_count_60)

    base_risk = round(
        0.45 * outcome_score
        + 0.30 * operational_score
        + 0.20 * compliance_score
        + 0.05 * inact_penalty,
        2,
    )

    risk_score = round(
        act_gate * base_risk + (1.0 - act_gate) * inact_penalty,
        2,
    )
    risk_score = clamp(risk_score, 0.0, 10.0)
    risk_level = risk_level_from_score(risk_score)

    # --------------------------
    # drivers
    # use contribution after top-level weights
    # --------------------------
    driver_contributions: Dict[str, float] = {
        # outcome
        "ORDER_DEFECT_RATE_60": 0.45 * 0.70 * outcome_subscores["ORDER_DEFECT_RATE_60"],
        "CHARGEBACK_RATE_90": 0.45 * 0.15 * outcome_subscores["CHARGEBACK_RATE_90"],
        "A_TO_Z_CLAIM_RATE_90": 0.45 * 0.10 * outcome_subscores["A_TO_Z_CLAIM_RATE_90"],
        "NEGATIVE_FEEDBACK_RATE_90": 0.45 * 0.05 * outcome_subscores["NEGATIVE_FEEDBACK_RATE_90"],

        # operational
        "LATE_SHIPMENT_RATE_30": 0.30 * 0.25 * operational_subscores["LATE_SHIPMENT_RATE_30"],
        "PRE_FULFILL_CANCEL_RATE_30": 0.30 * 0.15 * operational_subscores["PRE_FULFILL_CANCEL_RATE_30"],
        "AVG_RESPONSE_HOURS_30": 0.30 * 0.15 * operational_subscores["AVG_RESPONSE_HOURS_30"],
        "NO_RESPONSE_OVER_24H_30": 0.30 * 0.15 * operational_subscores["NO_RESPONSE_OVER_24H_30"],
        "VALID_TRACKING_RATE_30": 0.30 * 0.15 * operational_subscores["VALID_TRACKING_RATE_30"],
        "ON_TIME_DELIVERY_RATE_30": 0.30 * 0.15 * operational_subscores["ON_TIME_DELIVERY_RATE_30"],

        # compliance
        "PRODUCT_SAFETY_STATUS": 0.20 * compliance_subscores["PRODUCT_SAFETY_STATUS"],
        "PRODUCT_AUTHENTICITY_STATUS": 0.20 * compliance_subscores["PRODUCT_AUTHENTICITY_STATUS"],
        "POLICY_VIOLATION_STATUS": 0.20 * compliance_subscores["POLICY_VIOLATION_STATUS"],
        "LISTING_POLICY_STATUS": 0.20 * compliance_subscores["LISTING_POLICY_STATUS"],
        "INTELLECTUAL_PROPERTY_STATUS": 0.20 * compliance_subscores["INTELLECTUAL_PROPERTY_STATUS"],
    }

    top_risk_drivers = [
        k for k, v in sorted(driver_contributions.items(), key=lambda x: x[1], reverse=True) if v > 0
    ][:5]

    driver_1 = top_risk_drivers[0] if len(top_risk_drivers) > 0 else None
    driver_2 = top_risk_drivers[1] if len(top_risk_drivers) > 1 else None
    driver_3 = top_risk_drivers[2] if len(top_risk_drivers) > 2 else None

    outcome_drivers = [d for d in top_risk_drivers if METRIC_ID_TO_GROUP.get(d) == "outcome"]
    operational_drivers = [d for d in top_risk_drivers if METRIC_ID_TO_GROUP.get(d) == "operational"]
    compliance_drivers = [d for d in top_risk_drivers if METRIC_ID_TO_GROUP.get(d) == "compliance"]

    reason_parts = []
    if outcome_drivers:
        reason_parts.append("outcome risk: " + ", ".join(outcome_drivers[:2]))
    if operational_drivers:
        reason_parts.append("operational risk: " + ", ".join(operational_drivers[:2]))
    if compliance_drivers:
        reason_parts.append("compliance risk: " + ", ".join(compliance_drivers[:2]))

    if reason_parts:
        risk_reason = "Top risk drivers include " + "; ".join(reason_parts)
    else:
        risk_reason = "No significant risk signals detected."

    # --------------------------
    # band mapping for storage / explainability
    # --------------------------
    def score_to_band(score: float) -> str:
        if score == 0:
            return "green"
        if score < 6:
            return "yellow"
        return "red"

    metric_bands = {}
    metric_scores = {}
    weighted_metric_scores = {}

    # store sub-score bands/scores
    for metric_id, score in outcome_subscores.items():
        metric_bands[metric_id] = score_to_band(score)
        metric_scores[metric_id] = round(score, 2)
        weighted_metric_scores[metric_id] = round(driver_contributions[metric_id], 4)

    for metric_id, score in operational_subscores.items():
        metric_bands[metric_id] = score_to_band(score)
        metric_scores[metric_id] = round(score, 2)
        weighted_metric_scores[metric_id] = round(driver_contributions[metric_id], 4)

    for metric_id, score in compliance_subscores.items():
        metric_bands[metric_id] = score_to_band(score)
        metric_scores[metric_id] = round(score, 2)
        weighted_metric_scores[metric_id] = round(driver_contributions[metric_id], 4)

    metric_weights = {
        "ORDER_DEFECT_RATE_60": 0.45 * 0.70,
        "CHARGEBACK_RATE_90": 0.45 * 0.15,
        "A_TO_Z_CLAIM_RATE_90": 0.45 * 0.10,
        "NEGATIVE_FEEDBACK_RATE_90": 0.45 * 0.05,
        "LATE_SHIPMENT_RATE_30": 0.30 * 0.25,
        "PRE_FULFILL_CANCEL_RATE_30": 0.30 * 0.15,
        "AVG_RESPONSE_HOURS_30": 0.30 * 0.15,
        "NO_RESPONSE_OVER_24H_30": 0.30 * 0.15,
        "VALID_TRACKING_RATE_30": 0.30 * 0.15,
        "ON_TIME_DELIVERY_RATE_30": 0.30 * 0.15,
        "PRODUCT_SAFETY_STATUS": 0.20,
        "PRODUCT_AUTHENTICITY_STATUS": 0.20,
        "POLICY_VIOLATION_STATUS": 0.20,
        "LISTING_POLICY_STATUS": 0.20,
        "INTELLECTUAL_PROPERTY_STATUS": 0.20,
    }

    red_metric_count = sum(1 for v in metric_bands.values() if v == "red")
    yellow_metric_count = sum(1 for v in metric_bands.values() if v == "yellow")
    missing_metrics = [k for k, v in metric_values.items() if v is None]

    status_snapshot = {c: row.get(c) for c in BQ_STATUS_COLS}

    threshold_snapshot = {
        "version": "risk_formula_v2",
        "formula": {
            "final_score": "activity_gate * (0.45*outcome + 0.30*operational + 0.20*compliance + 0.05*inactivity_penalty) + (1-activity_gate)*inactivity_penalty"
        },
        "group_scores": {
            "outcome_score": outcome_score,
            "operational_score": operational_score,
            "compliance_score": compliance_score,
        },
        "activity": {
            "orders_count_60": orders_count_60,
            "orders_count_30": orders_count_30,
            "orders_count_90": orders_count_90,
            "activity_gate": act_gate,
            "inactivity_penalty": inact_penalty,
        },
    }

    raw_score_total = round(base_risk, 3)
    max_possible_score = 10.0

    return {
        "report_date": iso(row.get("report_date")),
        "mp_sup_key": str(row["mp_sup_key"]),
        "snapshot_timestamp": iso(row.get("snapshot_date")),
        "pipeline_version": "risk_formula_v2",
        "risk_score": round(risk_score, 2),
        "risk_level": risk_level,
        "risk_reason": risk_reason,
        "top_risk_drivers": top_risk_drivers,
        "driver_1": driver_1,
        "driver_2": driver_2,
        "driver_3": driver_3,
        "red_metric_count": red_metric_count,
        "yellow_metric_count": yellow_metric_count,
        "metric_values": metric_values,
        "metric_bands": metric_bands,
        "metric_scores": metric_scores,
        "weighted_metric_scores": weighted_metric_scores,
        "metric_weights": metric_weights,
        "threshold_snapshot": threshold_snapshot,
        "raw_score_total": raw_score_total,
        "max_possible_score": max_possible_score,
        "eligible_metric_count": len(metric_values) - len(missing_metrics),
        "missing_metrics": missing_metrics,
        "volume_flags": {
            "orders_count_60": orders_count_60,
            "activity_gate": act_gate,
            "inactivity_penalty": inact_penalty,
        },
        "status_snapshot": status_snapshot,
        "updated_at": utc_now_iso(),
    }


def build_payload(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in rows:
        if r.get("mp_sup_key") is None:
            continue
        item = score_supplier_row(r)
        dedup[(item["report_date"], item["mp_sup_key"])] = item
    return list(dedup.values())


# ============================================================
# SUPABASE
# ============================================================
def upsert_supabase(payload: List[Dict[str, Any]], chunk_size: int = 500, max_retries: int = 4) -> int:
    if not payload:
        print("[INFO] No rows to upsert.")
        return 0

    total_written = 0
    chunks = math.ceil(len(payload) / chunk_size)

    for i in range(chunks):
        part = payload[i * chunk_size: (i + 1) * chunk_size]

        for attempt in range(max_retries + 1):
            try:
                supabase.table(SUPABASE_TABLE).upsert(
                    part,
                    on_conflict="report_date,mp_sup_key",
                ).execute()

                total_written += len(part)
                print(f"[OK] Upsert chunk {i + 1}/{chunks}: {len(part)} rows")
                break
            except Exception as e:
                if attempt >= max_retries:
                    raise
                print(f"[WARN] Upsert failed chunk {i + 1}/{chunks}, attempt {attempt + 1}: {e}")
                backoff_sleep(attempt)

    print(f"[OK] Upserted total {total_written} rows into {SUPABASE_TABLE}.")
    return total_written


# ============================================================
# JSON EXPORT
# ============================================================
def build_unified_json(payload_row: Dict[str, Any]) -> Dict[str, Any]:
    metrics = []

    for metric in METRIC_CONFIG:
        metric_id = metric["metric_id"]
        unit = "status" if metric["direction"] == "status" else "raw_metric"

        metrics.append(
            {
                "metric_id": metric_id,
                "value": payload_row["metric_values"].get(metric_id),
                "unit": unit,
                "explanation": (
                    f"{metric_id} is in {payload_row['metric_bands'].get(metric_id, 'missing')} band "
                    f"with score {payload_row['metric_scores'].get(metric_id, 0)}."
                ),
            }
        )

    return {
        "table_name": BQ_TABLE,
        "supplier_key": payload_row["mp_sup_key"],
        "report_date": payload_row["report_date"],
        "metrics": metrics,
        "overall_risk_score": payload_row["risk_score"],
    }


def export_unified_json(payload: List[Dict[str, Any]], output_file: str = "risk_output.json") -> int:
    out = [build_unified_json(p) for p in payload]
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[OK] Exported {len(out)} records to {output_file}")
    return len(out)


# ============================================================
# PIPELINE
# ============================================================
def run_for_date(
    report_date: date,
    limit: int,
    chunk_size: int,
    export_json: bool,
    dry_run: bool,
) -> None:
    print("=" * 80)
    print(f"Report date: {report_date.isoformat()}")

    rows = fetch_latest_snapshot_per_supplier(report_date=report_date, limit=limit)
    print(f"[INFO] Latest snapshot rows fetched: {len(rows)}")

    if not rows:
        print("[INFO] No rows found. Skip.")
        return

    payload = build_payload(rows)
    print(f"[INFO] Payload rows prepared: {len(payload)}")

    if payload:
        scores = [float(p["risk_score"]) for p in payload]
        print(f"[INFO] Risk score range: min={min(scores):.2f}, max={max(scores):.2f}")

    if dry_run:
        print("[DRY-RUN] Skip Supabase write.")
    else:
        upsert_supabase(payload, chunk_size=chunk_size)

    if export_json:
        export_unified_json(payload, output_file="risk_output.json")

    print("[DONE]")


def main() -> None:
    parser = argparse.ArgumentParser(description="Supplier Daily Risk Pipeline (risk formula v2)")
    parser.add_argument("--report-date", type=str, default="", help="YYYY-MM-DD")
    parser.add_argument("--days-back", type=int, default=0, help="Run latest date and previous N days")
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--chunk-size", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-export-json", action="store_true")

    args = parser.parse_args()
    export_json = not args.no_export_json

    if args.report_date:
        rd = date.fromisoformat(args.report_date)
        run_for_date(
            report_date=rd,
            limit=args.limit,
            chunk_size=args.chunk_size,
            export_json=export_json,
            dry_run=args.dry_run,
        )
        return

    latest = get_latest_report_date()
    for i in range(args.days_back + 1):
        rd = latest - timedelta(days=i)
        run_for_date(
            report_date=rd,
            limit=args.limit,
            chunk_size=args.chunk_size,
            export_json=export_json,
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    main()