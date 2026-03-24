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

BQ_PAYABILITY_TABLE = "`bigqueryexport-183608.PayabilitySheets.v_supplier_summary`"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

SUPABASE_OUTPUT_TABLE = "health_daily_risk"
SUPABASE_MAPPING_TABLE = "suppliers"
CONSOLIDATED_TABLE = "consolidated_flagged_supplier_list"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.")

bq = bigquery.Client(project=BQ_PROJECT)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

print("SUPABASE_URL =", SUPABASE_URL)
print("SUPABASE_MAPPING_TABLE =", SUPABASE_MAPPING_TABLE)
print("CONSOLIDATED_TABLE =", CONSOLIDATED_TABLE)

# ============================================================
# METRIC CONFIG
# ============================================================
METRIC_CONFIG: List[Dict[str, Any]] = [
    # Outcome
    {
        "metric_id": "ORDER_DEFECT_RATE_60",
        "source_column": "orderWithDefects_60_rate",
        "direction": "higher_is_worse",
        "group": "outcome",
    },
    {
        "metric_id": "CHARGEBACK_RATE_90",
        "source_column": "chargebacks_90_rate",
        "direction": "higher_is_worse",
        "group": "outcome",
    },
    {
        "metric_id": "A_TO_Z_CLAIM_RATE_90",
        "source_column": "a_z_claims_90_rate",
        "direction": "higher_is_worse",
        "group": "outcome",
    },
    {
        "metric_id": "NEGATIVE_FEEDBACK_RATE_90",
        "source_column": "negativeFeedbacks_90_rate",
        "direction": "higher_is_worse",
        "group": "outcome",
    },

    # Operational
    {
        "metric_id": "LATE_SHIPMENT_RATE_30",
        "source_column": "lateShipment_30_rate",
        "direction": "higher_is_worse",
        "group": "operational",
    },
    {
        "metric_id": "PRE_FULFILL_CANCEL_RATE_30",
        "source_column": "preFulfillmentCancellation_30_rate",
        "direction": "higher_is_worse",
        "group": "operational",
    },
    {
        "metric_id": "AVG_RESPONSE_HOURS_30",
        "source_column": "averageResponseTimeInHours_30",
        "direction": "higher_is_worse",
        "group": "operational",
    },
    {
        "metric_id": "NO_RESPONSE_OVER_24H_30",
        "source_column": "noResponseForContactsOlderThan24Hours_30",
        "direction": "higher_is_worse",
        "group": "operational",
    },
    {
        "metric_id": "VALID_TRACKING_RATE_30",
        "source_column": "validTracking_rate_30",
        "direction": "lower_is_worse",
        "group": "operational",
    },
    {
        "metric_id": "ON_TIME_DELIVERY_RATE_30",
        "source_column": "onTimeDelivery_rate_30",
        "direction": "lower_is_worse",
        "group": "operational",
    },

    # Compliance
    {
        "metric_id": "PRODUCT_SAFETY_STATUS",
        "source_column": "productSafetyStatus_status",
        "direction": "status",
        "group": "compliance",
    },
    {
        "metric_id": "PRODUCT_AUTHENTICITY_STATUS",
        "source_column": "productAuthenticityStatus_status",
        "direction": "status",
        "group": "compliance",
    },
    {
        "metric_id": "POLICY_VIOLATION_STATUS",
        "source_column": "policyViolation_status",
        "direction": "status",
        "group": "compliance",
    },
    {
        "metric_id": "LISTING_POLICY_STATUS",
        "source_column": "listingPolicyStatus_status",
        "direction": "status",
        "group": "compliance",
    },
    {
        "metric_id": "INTELLECTUAL_PROPERTY_STATUS",
        "source_column": "intellectualProperty_status",
        "direction": "status",
        "group": "compliance",
    },
]

METRIC_ID_TO_GROUP = {m["metric_id"]: m["group"] for m in METRIC_CONFIG}

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


def pct_to_ratio(v: Any) -> Optional[float]:
    """
    BigQuery values appear to be percentage-style values.
    Examples:
      0.418  -> 0.00418
      99.74  -> 0.9974
    """
    x = safe_float(v)
    if x is None:
        return None
    return x / 100.0


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def backoff_sleep(attempt: int, base: float = 0.5, cap: float = 5.0) -> None:
    time.sleep(min(cap, base * (2 ** attempt)))


def normalize_key(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip().lower()
    return s if s else None


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


def fetch_latest_health_snapshot(report_date: date, limit: int = 5000) -> List[Dict[str, Any]]:
    metric_cols = [m["source_column"] for m in METRIC_CONFIG]
    metric_cols += ["orders_count_60", "orders_count_30", "orders_count_90", "path_golden"]

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


def fetch_payability_status_map() -> Dict[str, Dict[str, Any]]:
    q = f"""
    SELECT
      supplier_key,
      supplier_name,
      payability_status
    FROM {BQ_PAYABILITY_TABLE}
    """
    rows = bq.query(q).result()

    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        supplier_key = normalize_key(r["supplier_key"])
        if supplier_key is None:
            continue
        out[supplier_key] = {
            "supplier_name": r.get("supplier_name"),
            "payability_status": r.get("payability_status"),
        }
    return out


# ============================================================
# SUPABASE MAPPING
# ============================================================
def fetch_supplier_mapping() -> Dict[str, Dict[str, Any]]:
    """
    Fetch mp_sup_key -> supplier_key mapping from Supabase `suppliers`.
    """
    offset = 0
    page_size = 1000
    rows: List[Dict[str, Any]] = []

    while True:
        resp = (
            supabase.table(SUPABASE_MAPPING_TABLE)
            .select("mp_sup_key,supplier_key,supplier_name")
            .range(offset, offset + page_size - 1)
            .execute()
        )

        batch = getattr(resp, "data", None) or []
        rows.extend(batch)

        if len(batch) < page_size:
            break

        offset += page_size

    mapping: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        mp_sup_key = normalize_key(r.get("mp_sup_key"))
        if mp_sup_key is None:
            continue

        mapping[mp_sup_key] = {
            "supplier_key": r.get("supplier_key"),
            "supplier_name": r.get("supplier_name"),
        }

    return mapping


# ============================================================
# ENRICH + FILTER
# ============================================================
def enrich_health_rows_with_supplier_context(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    mapping = fetch_supplier_mapping()
    payability_map = fetch_payability_status_map()

    enriched: List[Dict[str, Any]] = []
    for row in rows:
        mp_sup_key_norm = normalize_key(row.get("mp_sup_key"))
        map_row = mapping.get(mp_sup_key_norm, {})

        supplier_key = map_row.get("supplier_key")
        supplier_key_norm = normalize_key(supplier_key)
        pay_row = payability_map.get(supplier_key_norm, {})

        row["supplier_key"] = supplier_key
        row["supplier_name"] = map_row.get("supplier_name") or pay_row.get("supplier_name")
        row["payability_status"] = pay_row.get("payability_status")

        enriched.append(row)

    return enriched


def filter_active_population(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Exclude Suspended and Pending accounts based on risk team feedback.
    """
    kept = []
    for row in rows:
        status = str(row.get("payability_status") or "").strip().lower()
        if status in {"suspended", "pending"}:
            continue
        kept.append(row)
    return kept


# ============================================================
# SCORING FUNCTIONS
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

    driver_contributions: Dict[str, float] = {
        "ORDER_DEFECT_RATE_60": 0.45 * 0.70 * outcome_subscores["ORDER_DEFECT_RATE_60"],
        "CHARGEBACK_RATE_90": 0.45 * 0.15 * outcome_subscores["CHARGEBACK_RATE_90"],
        "A_TO_Z_CLAIM_RATE_90": 0.45 * 0.10 * outcome_subscores["A_TO_Z_CLAIM_RATE_90"],
        "NEGATIVE_FEEDBACK_RATE_90": 0.45 * 0.05 * outcome_subscores["NEGATIVE_FEEDBACK_RATE_90"],
        "LATE_SHIPMENT_RATE_30": 0.30 * 0.25 * operational_subscores["LATE_SHIPMENT_RATE_30"],
        "PRE_FULFILL_CANCEL_RATE_30": 0.30 * 0.15 * operational_subscores["PRE_FULFILL_CANCEL_RATE_30"],
        "AVG_RESPONSE_HOURS_30": 0.30 * 0.15 * operational_subscores["AVG_RESPONSE_HOURS_30"],
        "NO_RESPONSE_OVER_24H_30": 0.30 * 0.15 * operational_subscores["NO_RESPONSE_OVER_24H_30"],
        "VALID_TRACKING_RATE_30": 0.30 * 0.15 * operational_subscores["VALID_TRACKING_RATE_30"],
        "ON_TIME_DELIVERY_RATE_30": 0.30 * 0.15 * operational_subscores["ON_TIME_DELIVERY_RATE_30"],
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

    def score_to_band(score: float) -> str:
        if score == 0:
            return "green"
        if score < 6:
            return "yellow"
        return "red"

    all_scores = {**outcome_subscores, **operational_subscores, **compliance_subscores}
    red_metric_count = sum(1 for v in all_scores.values() if score_to_band(v) == "red")
    yellow_metric_count = sum(1 for v in all_scores.values() if score_to_band(v) == "yellow")

    return {
        "report_date": iso(row.get("report_date")),
        "mp_sup_key": str(row["mp_sup_key"]),
        "supplier_key": row.get("supplier_key"),
        "supplier_name": row.get("supplier_name"),
        "payability_status": row.get("payability_status"),
        "snapshot_timestamp": iso(row.get("snapshot_date")),
        "pipeline_version": "risk_formula_v2_production",
        "risk_score": round(risk_score, 2),
        "risk_level": risk_level,
        "risk_reason": risk_reason,
        "top_risk_drivers": top_risk_drivers,
        "driver_1": driver_1,
        "driver_2": driver_2,
        "driver_3": driver_3,
        "red_metric_count": red_metric_count,
        "yellow_metric_count": yellow_metric_count,
        "created_at": utc_now_iso(),
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
# CONSOLIDATED FLAGGED TABLE
# ============================================================
def is_high_risk(row: Dict[str, Any]) -> bool:
    score = row.get("risk_score")
    if score is None:
        return False
    return score > 5


def build_consolidated_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "supplier_key": row.get("supplier_key"),
        "source": "health_report",
        "supplier_name": row.get("supplier_name"),
        "created_at": utc_now_iso(),
        "run_id": None,
        "metrics": {
            "top_risk_drivers": row.get("top_risk_drivers"),
            "driver_1": row.get("driver_1"),
            "driver_2": row.get("driver_2"),
            "driver_3": row.get("driver_3"),
            "red_metric_count": row.get("red_metric_count"),
            "yellow_metric_count": row.get("yellow_metric_count"),
            "pipeline_version": row.get("pipeline_version"),
        },
        "reasons": row.get("top_risk_drivers") or [],
        "overall_risk_score": row.get("risk_score"),
        "status": "pending_review",
    }


def write_flagged_to_consolidated(
    payload: List[Dict[str, Any]],
    chunk_size: int = 500,
    max_retries: int = 4,
) -> int:
    flagged_rows = [
        build_consolidated_row(row)
        for row in payload
        if is_high_risk(row) and row.get("supplier_key")
    ]

    if not flagged_rows:
        print("[INFO] No high-risk suppliers to write into consolidated table.")
        return 0

    total_written = 0
    chunks = math.ceil(len(flagged_rows) / chunk_size)

    for i in range(chunks):
        part = flagged_rows[i * chunk_size: (i + 1) * chunk_size]

        for attempt in range(max_retries + 1):
            try:
                (
                    supabase.table(CONSOLIDATED_TABLE)
                    .upsert(part, on_conflict="supplier_key,source")
                    .execute()
                )
                total_written += len(part)
                print(f"[OK] Upserted chunk {i + 1}/{chunks}: {len(part)} rows into {CONSOLIDATED_TABLE}")
                break
            except Exception as e:
                if attempt >= max_retries:
                    raise
                print(f"[WARN] Consolidated insert failed chunk {i + 1}/{chunks}, attempt {attempt + 1}: {e}")
                backoff_sleep(attempt)

    print(f"[INFO] High risk suppliers count: {len(flagged_rows)}")
    print(f"[OK] Inserted total {total_written} flagged suppliers into {CONSOLIDATED_TABLE}.")
    return total_written


# ============================================================
# SUPABASE OUTPUT
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
                (
                    supabase.table(SUPABASE_OUTPUT_TABLE)
                    .upsert(part, on_conflict="report_date,mp_sup_key")
                    .execute()
                )
                total_written += len(part)
                print(f"[OK] Upsert chunk {i + 1}/{chunks}: {len(part)} rows")
                break
            except Exception as e:
                if attempt >= max_retries:
                    raise
                print(f"[WARN] Upsert failed chunk {i + 1}/{chunks}, attempt {attempt + 1}: {e}")
                backoff_sleep(attempt)

    print(f"[OK] Upserted total {total_written} rows into {SUPABASE_OUTPUT_TABLE}.")
    return total_written


# ============================================================
# JSON EXPORT
# ============================================================
def build_unified_json(payload_row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "table_name": BQ_TABLE,
        "supplier_key": payload_row.get("supplier_key"),
        "mp_sup_key": payload_row.get("mp_sup_key"),
        "supplier_name": payload_row.get("supplier_name"),
        "payability_status": payload_row.get("payability_status"),
        "report_date": payload_row["report_date"],
        "overall_risk_score": payload_row["risk_score"],
        "risk_level": payload_row["risk_level"],
        "risk_reason": payload_row["risk_reason"],
        "top_risk_drivers": payload_row["top_risk_drivers"],
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

    health_rows = fetch_latest_health_snapshot(report_date=report_date, limit=limit)
    print(f"[INFO] Health rows fetched from BigQuery: {len(health_rows)}")

    if not health_rows:
        print("[INFO] No rows found. Skip.")
        return

    enriched_rows = enrich_health_rows_with_supplier_context(health_rows)

    print(f"[INFO] Rows after supplier mapping enrichment: {len(enriched_rows)}")

    supplier_key_count = sum(1 for r in enriched_rows if r.get("supplier_key"))
    payability_count = sum(1 for r in enriched_rows if r.get("payability_status"))

    print(f"[DEBUG] Rows with supplier_key: {supplier_key_count}")
    print(f"[DEBUG] Rows with payability_status: {payability_count}")

    for r in enriched_rows[:5]:
        print(
            "[DEBUG SAMPLE]",
            r.get("mp_sup_key"),
            r.get("supplier_key"),
            r.get("payability_status"),
        )

    filtered_rows = filter_active_population(enriched_rows)

    print(f"[INFO] Rows after payability filter (exclude suspended/pending): {len(filtered_rows)}")

    payload = build_payload(filtered_rows)
    print(f"[INFO] Payload rows prepared: {len(payload)}")

    if payload:
        scores = [float(p["risk_score"]) for p in payload]
        print(f"[INFO] Risk score range: min={min(scores):.2f}, max={max(scores):.2f}")

    if dry_run:
        print("[DRY-RUN] Skip Supabase write.")
    else:
        upsert_supabase(payload, chunk_size=chunk_size)
        write_flagged_to_consolidated(payload, chunk_size=chunk_size)

    if export_json:
        export_unified_json(payload, output_file="risk_output.json")

    print("[DONE]")


def main() -> None:
    parser = argparse.ArgumentParser(description="HealthData production risk pipeline")
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