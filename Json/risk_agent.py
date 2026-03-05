#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from statistics import median
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib import error as urllib_error
from urllib import request as urllib_request


SEVERE_NOTIFICATION_KEYWORDS = [
    "urgent",
    "at risk",
    "deactivated",
    "removed",
    "suspended",
    "action required",
    "trademark",
    "compliance",
    "restricted products",
    "policy warning",
]

DEFAULT_BQ_PROJECT = "bigqueryexport-183608"
DEFAULT_BQ_TABLE = f"{DEFAULT_BQ_PROJECT}.PayabilitySheets.marketplace_ext_data"
DEFAULT_BQ_DAYS = 60
DEFAULT_OUTPUT = Path("v1_output")
ENV_FILE_PATH = Path(__file__).with_name(".env.supabase")
SUPABASE_URL_PLACEHOLDER = "https://YOUR_PROJECT_REF.supabase.co"
SUPABASE_SERVICE_ROLE_KEY_PLACEHOLDER = "YOUR_SUPABASE_SERVICE_ROLE_KEY"
DEFAULT_SUPABASE_TABLE = "seller_risk_scores"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


load_env_file(ENV_FILE_PATH)

DEFAULT_SUPABASE_URL = os.getenv("SUPABASE_URL", SUPABASE_URL_PLACEHOLDER)
DEFAULT_SUPABASE_SERVICE_ROLE_KEY = os.getenv(
    "SUPABASE_SERVICE_ROLE_KEY", SUPABASE_SERVICE_ROLE_KEY_PLACEHOLDER
)


@dataclass
class Signal:
    metric_id: str
    value: float
    unit: str
    explanation: str
    risk_points: float


@dataclass
class RowRecord:
    row: Dict[str, Any]
    payload: Dict[str, Any]
    supplier_key: str
    report_date_text: str
    report_date_obj: date


def parse_percent(value: Any) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in {"n/a", "no data", "none", "null"}:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", s.replace(",", ""))
    if not m:
        return None
    return float(m.group(0))


def parse_money(value: Any) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = s.replace("$", "").replace(",", "")
    s = s.replace("(", "-").replace(")", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    return float(m.group(0))


def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def parse_feedback_bucket(value: Any) -> Tuple[Optional[float], Optional[float]]:
    """Returns (percent, count) from strings like '63 %(17)'."""
    if value is None:
        return None, None
    s = str(value).strip()
    if not s:
        return None, None
    nums = re.findall(r"-?\d+(?:\.\d+)?", s.replace(",", ""))
    if not nums:
        return None, None
    if len(nums) == 1:
        return float(nums[0]), None
    return float(nums[0]), float(nums[1])


def score_band(value: float, bands: List[Tuple[float, float]]) -> float:
    for threshold, score in bands:
        if value <= threshold:
            return score
    return bands[-1][1]


def extract_supplier_key(row: Dict[str, Any], payload: Dict[str, Any]) -> str:
    return (
        str(row.get("mp_sup_key") or "").strip()
        or str(row.get("supplier_key") or "").strip()
        or str(payload.get("Supplier Key") or "").strip()
        or "unknown"
    )


def parse_report_date(value: Any) -> Optional[date]:
    if value is None:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        return value.date()

    s = str(value).strip()
    if not s:
        return None

    # Common BigQuery and payload date formats
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d_%H-%M-%S_%z"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass

    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            return None

    return None


def extract_report_date(row: Dict[str, Any], payload: Dict[str, Any]) -> str:
    created = parse_report_date(row.get("created_date"))
    if created:
        return str(created)

    ts = parse_report_date(payload.get("Timestamp"))
    if ts:
        return str(ts)

    return str(date.today())


def load_rows_from_file(path: Path) -> List[Dict[str, Any]]:
    suffix = path.suffix.lower()

    if suffix == ".jsonl":
        rows: List[Dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        return rows

    if suffix == ".json":
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            return [obj]
        raise ValueError("Unsupported JSON root type")

    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))

    raise ValueError("Unsupported file type. Use .json, .jsonl, or .csv")


def load_rows_from_bigquery(
    table: str,
    project: Optional[str],
    days: Optional[int],
    limit: Optional[int],
    where: Optional[str],
) -> List[Dict[str, Any]]:
    try:
        from google.cloud import bigquery
    except Exception as exc:
        raise RuntimeError(
            "google-cloud-bigquery is required for --bq-table. Install with: pip install google-cloud-bigquery"
        ) from exc

    client = bigquery.Client(project=project)

    query = [
        "SELECT mp_sup_key, created_date, data",
        f"FROM `{table}`",
        "WHERE TRUE",
    ]
    params: List[Any] = []

    if days is not None:
        query.append("AND DATE(created_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)")
        params.append(bigquery.ScalarQueryParameter("days", "INT64", days))

    if where:
        query.append(f"AND ({where})")

    query.append("ORDER BY created_date DESC")
    if limit is not None:
        query.append(f"LIMIT {int(limit)}")

    job_config = bigquery.QueryJobConfig(query_parameters=params, use_legacy_sql=False)
    result = client.query("\n".join(query), job_config=job_config).result()

    rows: List[Dict[str, Any]] = []
    for row in result:
        rows.append(
            {
                "mp_sup_key": row.get("mp_sup_key"),
                "created_date": row.get("created_date"),
                "data": row.get("data"),
            }
        )
    return rows


def decode_payload(data_field: Any, fallback_row: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(data_field, dict):
        return data_field

    if isinstance(data_field, str) and data_field.strip():
        try:
            return json.loads(data_field)
        except json.JSONDecodeError:
            return {"Error": "invalid_json", "raw": data_field[:500]}

    if "Error" in fallback_row or "Supplier Key" in fallback_row:
        return fallback_row

    return {}


def get_perf_snapshot(payload: Dict[str, Any]) -> Dict[str, Optional[float]]:
    perf = payload.get("Account Performance Info", {})
    if not isinstance(perf, dict):
        return {"odr": None, "lsr": None, "cancel": None}

    return {
        "odr": parse_percent(perf.get("Order Defect Rate")),
        "lsr": parse_percent(perf.get("Late Shipment Rate")),
        "cancel": parse_percent(perf.get("Cancellation Rate")),
        "delivered_on_time": parse_percent(perf.get("Delivered on time")),
    }


def policy_weighted_sum(payload: Dict[str, Any]) -> float:
    policy = payload.get("policy_compliance", {})
    if not isinstance(policy, dict):
        return 0.0

    total = 0.0
    for key, raw in policy.items():
        value = parse_float(raw) or 0.0
        if value <= 0:
            continue
        kl = key.lower()
        weight = 1.0
        if "intellectual property" in kl or "authenticity" in kl:
            weight = 1.8
        elif "safety" in kl or "restricted" in kl:
            weight = 1.6
        elif "regulatory" in kl:
            weight = 1.5
        total += value * weight
    return total


def extract_statement_reserves(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    reserves: List[Dict[str, Any]] = []
    sources = []
    if isinstance(payload.get("Statements"), list):
        sources.append(payload.get("Statements"))
    if isinstance(payload.get("Statements_B2B"), list):
        sources.append(payload.get("Statements_B2B"))

    for statements in sources:
        for st in statements:
            if not isinstance(st, dict):
                continue
            details = st.get("details") if isinstance(st.get("details"), dict) else {}
            level_reserve = details.get("Account Level Reserve") if isinstance(details.get("Account Level Reserve"), dict) else {}
            reserve_raw = level_reserve.get("Reserve")
            reserve_value = parse_money(reserve_raw)
            if reserve_value is None:
                continue

            status = str(st.get("ProcessingStatus") or details.get("Status") or "").lower()
            reserves.append(
                {
                    "reserve_abs": abs(reserve_value),
                    "status": status,
                    "end_date": st.get("end_date"),
                    "statement_id": st.get("StatementID"),
                }
            )
    return reserves


def get_trend_snapshot(payload: Dict[str, Any]) -> Dict[str, Optional[float]]:
    perf = get_perf_snapshot(payload)
    reserves = extract_statement_reserves(payload)
    latest_reserve = reserves[0]["reserve_abs"] if reserves else None
    closed_reserves = [r["reserve_abs"] for r in reserves if "closed" in r.get("status", "")]
    typical_closed_reserve = median(closed_reserves) if closed_reserves else None

    return {
        "odr": perf.get("odr"),
        "lsr": perf.get("lsr"),
        "cancel": perf.get("cancel"),
        "delivered_on_time": perf.get("delivered_on_time"),
        "policy_weighted": policy_weighted_sum(payload),
        "latest_reserve": latest_reserve,
        "typical_closed_reserve": typical_closed_reserve,
    }


def analyze_payload(payload: Dict[str, Any]) -> List[Signal]:
    signals: List[Signal] = []

    err = payload.get("Error")
    if err:
        err_l = str(err).lower()
        base = 9.0
        if "password is incorrect" in err_l or "not authorized" in err_l:
            base = 9.5
        elif "internal error" in err_l:
            base = 7.0
        signals.append(
            Signal(
                metric_id="SYSTEM_ERROR",
                value=1,
                unit="flag",
                explanation=f"Payload contains error: {str(err)[:120]}",
                risk_points=base,
            )
        )

    feedback = payload.get("feedback", {})
    if isinstance(feedback, dict):
        summary = str(feedback.get("Summary") or "")
        star_match = re.search(r"(\d+(?:\.\d+)?)\s*stars", summary.lower())
        if star_match:
            stars = float(star_match.group(1))
            star_risk = 0.0
            if stars < 3.0:
                star_risk = 8.0
            elif stars < 3.5:
                star_risk = 6.0
            elif stars < 4.0:
                star_risk = 4.0
            elif stars < 4.5:
                star_risk = 2.0
            if star_risk > 0:
                signals.append(
                    Signal(
                        metric_id="FEEDBACK_STAR_RATING",
                        value=stars,
                        unit="stars",
                        explanation=f"Customer feedback summary is {stars} stars.",
                        risk_points=star_risk,
                    )
                )

        neg_30 = parse_percent(
            ((feedback.get("Negative") or {}).get("30 days") if isinstance(feedback.get("Negative"), dict) else None)
        )
        if neg_30 is not None:
            points = score_band(neg_30, [(5, 0.5), (10, 1.5), (20, 3.0), (40, 5.5), (100, 8.5)])
            signals.append(
                Signal(
                    metric_id="NEGATIVE_FEEDBACK_30D",
                    value=neg_30,
                    unit="%",
                    explanation="30-day negative feedback rate extracted from feedback section.",
                    risk_points=points,
                )
            )

        # Compare recent 30-day negative rate vs prior 60 days (derived from 90d-30d).
        if isinstance(feedback.get("Count"), dict) and isinstance(feedback.get("Negative"), dict):
            count_30 = parse_float((feedback.get("Count") or {}).get("30 days"))
            count_90 = parse_float((feedback.get("Count") or {}).get("90 days"))
            _, neg_count_30 = parse_feedback_bucket((feedback.get("Negative") or {}).get("30 days"))
            _, neg_count_90 = parse_feedback_bucket((feedback.get("Negative") or {}).get("90 days"))

            if (
                count_30 is not None
                and count_90 is not None
                and neg_count_30 is not None
                and neg_count_90 is not None
                and count_90 > count_30
            ):
                count_60 = count_90 - count_30
                neg_count_60 = max(0.0, neg_count_90 - neg_count_30)
                if count_30 > 0 and count_60 > 0:
                    neg_rate_30 = (neg_count_30 / count_30) * 100.0
                    neg_rate_60 = (neg_count_60 / count_60) * 100.0
                    delta = neg_rate_30 - neg_rate_60
                    if delta > 0:
                        risk_points = score_band(delta, [(2, 0.8), (5, 1.8), (10, 3.2), (20, 5.0), (100, 7.0)])
                        signals.append(
                            Signal(
                                metric_id="NEG_FEEDBACK_RATE_ACCEL_30V60",
                                value=round(delta, 3),
                                unit="pp",
                                explanation=(
                                    f"30d negative rate ({round(neg_rate_30, 2)}%) is above prior-60d "
                                    f"({round(neg_rate_60, 2)}%)."
                                ),
                                risk_points=risk_points,
                            )
                        )

    perf = payload.get("Account Performance Info", {})
    if isinstance(perf, dict):
        thresholds = [
            ("Order Defect Rate", "ODR", [(1, 0.5), (2, 2.5), (3, 5.5), (100, 8.5)]),
            ("Late Shipment Rate", "LSR", [(2, 0.5), (4, 2.5), (8, 5.5), (100, 8.0)]),
            ("Cancellation Rate", "CANCEL_RATE", [(1, 0.4), (2.5, 2.0), (5, 4.5), (100, 7.0)]),
            ("Valid Tracking Rate - All Categories", "VTR", [(90, 8.0), (95, 5.0), (98, 2.5), (100, 0.2)]),
            ("Delivered on time", "ON_TIME_DELIVERY", [(80, 7.0), (90, 4.0), (95, 2.0), (100, 0.3)]),
        ]

        for field, mid, bands in thresholds:
            v = parse_percent(perf.get(field))
            if v is None:
                continue
            points = score_band(v, bands)
            if points <= 0:
                continue
            signals.append(
                Signal(
                    metric_id=mid,
                    value=v,
                    unit="%",
                    explanation=f"{field} is {v}%.",
                    risk_points=points,
                )
            )

        neg_cnt = parse_float(perf.get("Negative Feedback Count"))
        if neg_cnt is not None and neg_cnt > 10:
            signals.append(
                Signal(
                    metric_id="NEG_FEEDBACK_COUNT",
                    value=neg_cnt,
                    unit="count",
                    explanation="Absolute negative feedback count is elevated.",
                    risk_points=min(6.0, 1.5 + neg_cnt / 15.0),
                )
            )

    policy_total = policy_weighted_sum(payload)
    if policy_total > 0:
            points = min(10.0, 2.0 + policy_total ** 0.5 / 2.0)
            signals.append(
                Signal(
                    metric_id="POLICY_VIOLATION_LOAD",
                    value=round(policy_total, 2),
                    unit="weighted_count",
                    explanation="Aggregated policy/compliance violations weighted by severity.",
                    risk_points=points,
                )
            )

    notifications = payload.get("Last Notification Titles")
    if isinstance(notifications, list) and notifications:
        severe = 0
        for title in notifications:
            tl = str(title).lower()
            if any(keyword in tl for keyword in SEVERE_NOTIFICATION_KEYWORDS):
                severe += 1

        if severe > 0:
            signals.append(
                Signal(
                    metric_id="SEVERE_NOTIFICATIONS",
                    value=severe,
                    unit="count",
                    explanation="Recent notifications include policy/deactivation severity keywords.",
                    risk_points=min(9.0, 1.5 + severe * 0.45),
                )
            )

    perms = payload.get("permissions", {})
    if isinstance(perms, dict) and perms.get("status") and str(perms.get("status")).lower() != "approved":
        signals.append(
            Signal(
                metric_id="PERMISSION_STATUS",
                value=1,
                unit="flag",
                explanation="Marketplace permissions are not approved.",
                risk_points=7.5,
            )
        )

    loans = payload.get("Closed Loans")
    if isinstance(loans, list) and loans:
        paid_off = 0
        for loan in loans:
            ext = loan.get("Extended") if isinstance(loan, dict) else {}
            if isinstance(ext, dict) and str(ext.get("status", "")).upper() == "PAID_OFF":
                paid_off += 1

        if paid_off == len(loans):
            signals.append(
                Signal(
                    metric_id="LOAN_STATUS_BUFFER",
                    value=paid_off,
                    unit="count",
                    explanation="All visible lending accounts are paid off (risk-reducing signal).",
                    risk_points=-1.8,
                )
            )

    statements_summary = payload.get("StatementsSummary", {})
    if isinstance(statements_summary, dict):
        funds = (
            ((statements_summary.get("Funds Available") or {}).get("All Accounts"))
            if isinstance(statements_summary.get("Funds Available"), dict)
            else None
        )
        total = (
            ((statements_summary.get("Total Balance") or {}).get("All Accounts"))
            if isinstance(statements_summary.get("Total Balance"), dict)
            else None
        )
        funds_v = parse_money(funds)
        total_v = parse_money(total)
        if total_v and total_v > 0 and funds_v is not None:
            ratio = funds_v / total_v
            if ratio < 0.2:
                signals.append(
                    Signal(
                        metric_id="LOW_AVAILABLE_FUNDS_RATIO",
                        value=round(ratio * 100, 2),
                        unit="%",
                        explanation="Only a small share of total balance is immediately available.",
                        risk_points=4.0,
                    )
                )

    reserves = extract_statement_reserves(payload)
    if reserves:
        latest_reserve = reserves[0]["reserve_abs"]
        if latest_reserve >= 5000:
            reserve_points = score_band(
                latest_reserve,
                [(10000, 1.0), (25000, 2.0), (50000, 3.5), (100000, 5.0), (10**12, 6.5)],
            )
            latest_status = reserves[0].get("status") or "unknown"
            signals.append(
                Signal(
                    metric_id="ACCOUNT_LEVEL_RESERVE_AMOUNT",
                    value=round(latest_reserve, 2),
                    unit="USD",
                    explanation=(
                        f"Latest account-level reserve is ${latest_reserve:,.2f} ({latest_status} statement). "
                        "Open settlements may understate final reserve impact until payout closes."
                    ),
                    risk_points=reserve_points,
                )
            )

        closed_reserves = [r["reserve_abs"] for r in reserves if "closed" in r.get("status", "")]
        if len(closed_reserves) >= 2:
            recent = closed_reserves[0]
            typical = median(closed_reserves[1:])
            if typical > 0 and recent >= typical * 1.5 and (recent - typical) >= 5000:
                signals.append(
                    Signal(
                        metric_id="ACCOUNT_RESERVE_RECENT_SPIKE",
                        value=round((recent / typical), 3),
                        unit="x",
                        explanation=(
                            f"Recent closed-statement reserve (${recent:,.2f}) is {round(recent/typical, 2)}x "
                            f"typical (${typical:,.2f})."
                        ),
                        risk_points=min(4.0, 1.5 + (recent / typical - 1.0)),
                    )
                )

    return signals


def compute_risk_score(signals: List[Signal]) -> float:
    raw_points = max(0.0, sum(signal.risk_points for signal in signals))
    return min(10.0, round(1.0 + raw_points / 6.0, 2))


def build_record(row: Dict[str, Any]) -> RowRecord:
    payload = decode_payload(row.get("data"), row)
    supplier_key = extract_supplier_key(row, payload)
    report_date_text = extract_report_date(row, payload)
    report_date_obj = parse_report_date(report_date_text) or date.today()
    return RowRecord(
        row=row,
        payload=payload,
        supplier_key=supplier_key,
        report_date_text=report_date_text,
        report_date_obj=report_date_obj,
    )


def build_trend_signal(current_score: float, previous_scores: List[float]) -> Optional[Signal]:
    if len(previous_scores) < 3:
        return None

    baseline = sum(previous_scores[-7:]) / min(7, len(previous_scores))
    delta = current_score - baseline
    if delta >= 2.0:
        return Signal(
            metric_id="RISK_SCORE_SPIKE_7D",
            value=round(delta, 3),
            unit="score_delta",
            explanation="Current daily risk is sharply above seller's trailing 7-day baseline.",
            risk_points=3.0,
        )
    if delta >= 1.0:
        return Signal(
            metric_id="RISK_SCORE_SPIKE_7D",
            value=round(delta, 3),
            unit="score_delta",
            explanation="Current daily risk is moderately above seller's trailing 7-day baseline.",
            risk_points=1.5,
        )
    if delta <= -2.0:
        return Signal(
            metric_id="RISK_SCORE_RECOVERY_7D",
            value=round(delta, 3),
            unit="score_delta",
            explanation="Current daily risk is materially below seller's trailing 7-day baseline.",
            risk_points=-1.0,
        )
    return None


def build_perf_deterioration_signal(
    current_snapshot: Dict[str, Optional[float]],
    previous_snapshot: Optional[Dict[str, Optional[float]]],
) -> Optional[Signal]:
    if previous_snapshot is None:
        return None

    deterioration_points = 0.0
    details: List[str] = []

    odr_now, odr_prev = current_snapshot.get("odr"), previous_snapshot.get("odr")
    if odr_now is not None and odr_prev is not None and (odr_now - odr_prev) >= 0.5:
        deterioration_points += 1.5
        details.append(f"ODR +{round(odr_now - odr_prev, 2)}pp")

    lsr_now, lsr_prev = current_snapshot.get("lsr"), previous_snapshot.get("lsr")
    if lsr_now is not None and lsr_prev is not None and (lsr_now - lsr_prev) >= 1.0:
        deterioration_points += 1.2
        details.append(f"LSR +{round(lsr_now - lsr_prev, 2)}pp")

    can_now, can_prev = current_snapshot.get("cancel"), previous_snapshot.get("cancel")
    if can_now is not None and can_prev is not None and (can_now - can_prev) >= 1.0:
        deterioration_points += 1.0
        details.append(f"Cancel +{round(can_now - can_prev, 2)}pp")

    dot_now, dot_prev = current_snapshot.get("delivered_on_time"), previous_snapshot.get("delivered_on_time")
    if dot_now is not None and dot_prev is not None and (dot_prev - dot_now) >= 1.0:
        deterioration_points += 1.0
        details.append(f"On-time -{round(dot_prev - dot_now, 2)}pp")

    if deterioration_points <= 0:
        return None

    return Signal(
        metric_id="PERFORMANCE_TREND_DETERIORATION",
        value=round(deterioration_points, 3),
        unit="risk_points",
        explanation="Day-over-day deterioration detected: " + ", ".join(details),
        risk_points=deterioration_points,
    )


def build_policy_trend_signal(
    current_snapshot: Dict[str, Optional[float]],
    previous_snapshot: Optional[Dict[str, Optional[float]]],
    prior_snapshots: List[Dict[str, Optional[float]]],
) -> Optional[Signal]:
    current = current_snapshot.get("policy_weighted")
    if current is None:
        return None

    baseline_values = [s.get("policy_weighted") for s in prior_snapshots[-7:] if s.get("policy_weighted") is not None]
    baseline = (sum(baseline_values) / len(baseline_values)) if baseline_values else None
    previous = previous_snapshot.get("policy_weighted") if previous_snapshot else None

    if previous is not None and current - previous >= 5:
        delta = current - previous
        return Signal(
            metric_id="POLICY_WARNINGS_INCREASE_DOD",
            value=round(delta, 3),
            unit="weighted_count_delta",
            explanation="Policy compliance warning load increased versus previous day.",
            risk_points=min(4.0, 1.2 + delta / 8.0),
        )

    if baseline is not None and current - baseline >= 8:
        delta = current - baseline
        return Signal(
            metric_id="POLICY_WARNINGS_INCREASE_7D",
            value=round(delta, 3),
            unit="weighted_count_delta",
            explanation="Policy compliance warning load is above trailing 7-day baseline.",
            risk_points=min(4.5, 1.5 + delta / 10.0),
        )

    return None


def build_reserve_trend_signal(
    current_snapshot: Dict[str, Optional[float]],
    previous_snapshot: Optional[Dict[str, Optional[float]]],
    prior_snapshots: List[Dict[str, Optional[float]]],
) -> Optional[Signal]:
    current = current_snapshot.get("latest_reserve")
    if current is None:
        return None

    baseline_values = [s.get("latest_reserve") for s in prior_snapshots[-7:] if s.get("latest_reserve") is not None]
    baseline = median(baseline_values) if baseline_values else None
    previous = previous_snapshot.get("latest_reserve") if previous_snapshot else None

    if previous is not None and current - previous >= 10000:
        delta = current - previous
        return Signal(
            metric_id="ACCOUNT_RESERVE_INCREASE_DOD",
            value=round(delta, 2),
            unit="USD_delta",
            explanation="Latest account-level reserve increased materially vs previous day.",
            risk_points=min(4.5, 1.5 + delta / 30000.0),
        )

    if baseline is not None and baseline > 0 and current >= baseline * 1.6 and (current - baseline) >= 10000:
        return Signal(
            metric_id="ACCOUNT_RESERVE_INCREASE_7D",
            value=round(current / baseline, 3),
            unit="x",
            explanation="Latest account-level reserve is elevated vs trailing baseline.",
            risk_points=min(4.5, 1.8 + (current / baseline - 1.0)),
        )

    return None


def evaluate_records(records: List[RowRecord], table_name: str, latest_per_seller: bool) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[RowRecord]] = {}
    for rec in records:
        grouped.setdefault(rec.supplier_key, []).append(rec)

    outputs: List[Dict[str, Any]] = []

    for supplier_key, seller_records in grouped.items():
        seller_records.sort(key=lambda rec: rec.report_date_obj)

        previous_scores: List[float] = []
        previous_snapshot: Optional[Dict[str, Optional[float]]] = None
        prior_snapshots: List[Dict[str, Optional[float]]] = []
        seller_outputs: List[Dict[str, Any]] = []

        for rec in seller_records:
            base_signals = analyze_payload(rec.payload)
            base_score = compute_risk_score(base_signals)

            trend_signal = build_trend_signal(base_score, previous_scores)
            current_snapshot = get_trend_snapshot(rec.payload)
            perf_signal = build_perf_deterioration_signal(current_snapshot, previous_snapshot)
            policy_trend_signal = build_policy_trend_signal(current_snapshot, previous_snapshot, prior_snapshots)
            reserve_trend_signal = build_reserve_trend_signal(current_snapshot, previous_snapshot, prior_snapshots)

            final_signals = list(base_signals)
            if trend_signal is not None:
                final_signals.append(trend_signal)
            if perf_signal is not None:
                final_signals.append(perf_signal)
            if policy_trend_signal is not None:
                final_signals.append(policy_trend_signal)
            if reserve_trend_signal is not None:
                final_signals.append(reserve_trend_signal)

            final_score = compute_risk_score(final_signals)
            seller_outputs.append(
                {
                    "table_name": table_name,
                    "supplier_key": supplier_key,
                    "report_date": rec.report_date_text,
                    "metrics": [
                        {
                            "metric_id": signal.metric_id,
                            "value": round(signal.value, 4),
                            "unit": signal.unit,
                            "explanation": signal.explanation,
                            "risk_points": round(signal.risk_points, 3),
                        }
                        for signal in sorted(final_signals, key=lambda item: item.risk_points, reverse=True)
                    ],
                    "overall_risk_score": final_score,
                }
            )

            previous_scores.append(final_score)
            previous_snapshot = current_snapshot
            prior_snapshots.append(current_snapshot)

        if latest_per_seller and seller_outputs:
            outputs.append(seller_outputs[-1])
        else:
            outputs.extend(seller_outputs)

    outputs.sort(key=lambda item: (item["supplier_key"], item["report_date"]))
    return outputs


def run(
    input_path: Optional[Path],
    bq_table: Optional[str],
    bq_project: Optional[str],
    bq_days: Optional[int],
    bq_where: Optional[str],
    output_path: Optional[Path],
    table_name: str,
    limit: Optional[int],
    latest_per_seller: bool,
) -> List[Dict[str, Any]]:
    # Notebook-friendly default behavior:
    # if no source is provided, automatically analyze all sellers from default BigQuery table.
    if not input_path and not bq_table:
        bq_table = DEFAULT_BQ_TABLE
        if bq_project is None:
            bq_project = DEFAULT_BQ_PROJECT
        if bq_days is None:
            bq_days = DEFAULT_BQ_DAYS
        if output_path is None:
            output_path = DEFAULT_OUTPUT

    if input_path and bq_table:
        raise ValueError("Provide only one data source: --input or --bq-table")

    if input_path:
        rows = load_rows_from_file(input_path)
    else:
        rows = load_rows_from_bigquery(
            table=bq_table or "",
            project=bq_project,
            days=bq_days,
            limit=limit,
            where=bq_where,
        )

    if limit is not None and input_path:
        rows = rows[:limit]

    records = [build_record(row) for row in rows]
    outputs = evaluate_records(records, table_name=table_name, latest_per_seller=latest_per_seller)

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(outputs, f, indent=2)

    return outputs


def push_to_supabase(
    outputs: List[Dict[str, Any]],
    supabase_url: str,
    supabase_service_role_key: str,
    supabase_table: str,
) -> None:
    base_url = (supabase_url or "").strip().rstrip("/")
    api_key = (supabase_service_role_key or "").strip()
    table = (supabase_table or "").strip()

    if not base_url or SUPABASE_URL_PLACEHOLDER in base_url:
        raise ValueError("Set a real Supabase project URL before using --to-supabase.")
    if not api_key or SUPABASE_SERVICE_ROLE_KEY_PLACEHOLDER in api_key:
        raise ValueError("Set a real Supabase service role key before using --to-supabase.")
    if not table:
        raise ValueError("Supabase table name cannot be empty.")

    endpoint = f"{base_url}/rest/v1/{table}"
    payload = json.dumps(outputs).encode("utf-8")
    req = urllib_request.Request(
        endpoint,
        data=payload,
        method="POST",
        headers={
            "apikey": api_key,
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
    )

    try:
        with urllib_request.urlopen(req) as response:
            if response.status not in (200, 201, 204):
                body = response.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"Supabase insert failed with status {response.status}: {body}")
    except urllib_error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Supabase insert failed with status {exc.code}: {body}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Supabase connection failed: {exc}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Risk analysis agent for daily JSON risk data")
    parser.add_argument("--input", type=Path, help="Input file (.json/.jsonl/.csv)")
    parser.add_argument(
        "--bq-table",
        help=(
            "BigQuery table in format project.dataset.table. "
            f"Default: {DEFAULT_BQ_TABLE}"
        ),
    )
    parser.add_argument(
        "--bq-project",
        help=f"Optional GCP project for BigQuery client. Default: {DEFAULT_BQ_PROJECT}",
    )
    parser.add_argument("--bq-days", type=int, help="Only load last N days from BigQuery using created_date")
    parser.add_argument("--bq-where", help="Additional SQL predicate appended to WHERE clause")

    parser.add_argument("--table-name", default="json_table", help="Source table name in output")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output file path for JSON results. Default: {DEFAULT_OUTPUT}",
    )
    parser.add_argument("--limit", type=int, help="Max rows to load/process")
    parser.add_argument(
        "--all-rows",
        action="store_true",
        help="Emit one result per row instead of latest per seller",
    )
    parser.add_argument(
        "--to-supabase",
        action="store_true",
        default=True,
        help="Insert results into Supabase table using REST API (default: enabled).",
    )
    parser.add_argument(
        "--no-supabase",
        dest="to_supabase",
        action="store_false",
        help="Disable Supabase upload for this run.",
    )
    parser.add_argument(
        "--supabase-url",
        default=os.getenv("SUPABASE_URL", DEFAULT_SUPABASE_URL),
        help=f"Supabase project URL (also loaded from {ENV_FILE_PATH.name}).",
    )
    parser.add_argument(
        "--supabase-service-role-key",
        default=os.getenv("SUPABASE_SERVICE_ROLE_KEY", DEFAULT_SUPABASE_SERVICE_ROLE_KEY),
        help=f"Supabase service role key (also loaded from {ENV_FILE_PATH.name}).",
    )
    parser.add_argument(
        "--supabase-table",
        default=DEFAULT_SUPABASE_TABLE,
        help=f"Supabase table name for inserts. Default: {DEFAULT_SUPABASE_TABLE}",
    )

    # Notebook kernels (Colab/Jupyter) inject extra args like: -f <kernel.json>.
    # Use parse_known_args so the script can run cleanly in notebooks.
    args, _unknown = parser.parse_known_args()

    outputs = run(
        input_path=args.input,
        bq_table=args.bq_table,
        bq_project=args.bq_project,
        bq_days=args.bq_days,
        bq_where=args.bq_where,
        output_path=args.output,
        table_name=args.table_name,
        limit=args.limit,
        latest_per_seller=not args.all_rows,
    )

    if args.to_supabase:
        push_to_supabase(
            outputs=outputs,
            supabase_url=args.supabase_url,
            supabase_service_role_key=args.supabase_service_role_key,
            supabase_table=args.supabase_table,
        )

    print(f"Saved {len(outputs)} records to {args.output}")


if __name__ == "__main__":
    main()
