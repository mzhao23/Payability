"""extractors/feature_extractor.py

Parses the `data` JSON column and the structured BQ columns into a flat
FeatureSet dict that is consumed by the rule-based scorer and the AI agent.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Optional

from utils.logger import get_logger

log = get_logger("feature_extractor")

# ── Supplier key cache ────────────────────────────────────────────────────────
import pathlib as _pathlib

_SUPPLIER_KEY_CACHE_FILE = _pathlib.Path(__file__).parent.parent / "input" / "supplier_key_cache.json"
_supplier_key_cache: dict[str, str] | None = None

def _get_supplier_key_cache() -> dict[str, str]:
    global _supplier_key_cache
    if _supplier_key_cache is None:
        if _SUPPLIER_KEY_CACHE_FILE.exists():
            import json as _json
            _supplier_key_cache = _json.loads(_SUPPLIER_KEY_CACHE_FILE.read_text())
            log.info("Loaded %d supplier_key mappings from cache.", len(_supplier_key_cache))
        else:
            log.warning("supplier_key_cache.json not found — run sync_suppliers.py first.")
            _supplier_key_cache = {}
    return _supplier_key_cache

# ── Error classification ───────────────────────────────────────────────────────
_ERROR_PATTERNS: dict[str, str] = {
    "login_error": r"(error in login|login process|something was wrong in login)",
    "not_authorized": r"(not authorized|email.*isn't associated|isn't associated with any)",
    "wrong_password": r"(password is incorrect|authorization error.*password)",
    "bank_page_error": r"(bank account page|data has not been displayed)",
    "internal_error": r"internal error",
    "advance_only": r"advanceOffers",           # data only contains advance offer data
    "onboarding_only": r"onboardingData",       # new/inactive seller
}


def classify_error(data_str: str) -> Optional[str]:
    """Return an error class string if the data column signals a known error, else None.
    If the JSON contains a top-level 'Error' field, returns 'scraper_error' immediately.
    """
    # Check for top-level Error field in JSON first
    try:
        d = json.loads(data_str)
        if isinstance(d, dict) and d.get("Error"):
            return "scraper_error"
    except (json.JSONDecodeError, TypeError):
        pass

    lower = data_str.lower()
    for label, pattern in _ERROR_PATTERNS.items():
        if re.search(pattern, lower):
            return label
    return None


# ── FeatureSet dataclass ───────────────────────────────────────────────────────

@dataclass
class FeatureSet:
    # identifiers
    supplier_key: str = ""
    supplier_name: str = ""
    seller_id: str = ""
    store_name: str = ""
    report_date: str = ""

    # data quality
    data_quality_flag: str = "ok"
    raw_error: Optional[str] = None

    # account
    account_status: str = ""
    two_step_verification: str = ""
    all_accounts_in_us: Optional[bool] = None

    # performance (from BQ columns)
    order_defect_rate: Optional[float] = None
    late_shipment_rate: Optional[float] = None
    cancellation_rate: Optional[float] = None
    valid_tracking_rate: Optional[float] = None
    delivered_on_time: Optional[float] = None
    late_responses: Optional[float] = None
    return_dissatisfaction_rate: Optional[float] = None
    customer_service_dissatisfaction_rate: Optional[float] = None

    # performance (from data JSON — richer)
    odr_from_json: Optional[str] = None
    late_shipment_from_json: Optional[str] = None
    cancellation_from_json: Optional[str] = None
    valid_tracking_from_json: Optional[str] = None
    delivered_on_time_from_json: Optional[str] = None

    # sales
    sales_30_days: Optional[float] = None
    sales_7_days: Optional[float] = None
    channel_sales_all: Optional[float] = None
    channel_sales_amazon: Optional[float] = None
    channel_sales_seller: Optional[float] = None

    # statement deposits (list of recent closed periods)
    recent_deposits: list[dict] = field(default_factory=list)   # [{period, amount}]
    statements_detail: list[dict] = field(default_factory=list) # [{period, end_date, deposit, reserve, infobox}] most recent first
    total_balance: Optional[float] = None
    funds_available: Optional[float] = None

    # complaints
    cust_complaints_authenticity: Optional[float] = None
    cust_complaints_safety: Optional[float] = None
    cust_complaints_ip: Optional[float] = None
    cust_complaints_policy: Optional[float] = None

    # policy compliance (from data JSON)
    policy_regulatory: Optional[int] = None
    policy_other: Optional[int] = None
    policy_listing: Optional[int] = None
    policy_food_safety: Optional[int] = None
    policy_restricted: Optional[int] = None
    policy_condition: Optional[int] = None
    policy_authenticity: Optional[int] = None
    policy_ip_received: Optional[int] = None
    policy_ip_suspected: Optional[int] = None
    policy_reviews: Optional[int] = None

    # feedback
    feedback_rating_summary: str = ""
    feedback_positive_30d: Optional[float] = None   # percentage 0-100
    feedback_negative_30d: Optional[float] = None
    feedback_neutral_30d: Optional[float] = None
    feedback_count_30d: Optional[int] = None

    # feedback trend (30d vs 60d window: days 31-90)
    feedback_negative_30d_pct: Optional[float] = None    # same as feedback_negative_30d, alias
    feedback_negative_prior60d_pct: Optional[float] = None  # pct in days 31-90 (60d window)
    feedback_negative_trend_delta: Optional[float] = None   # 30d_pct minus prior_60d_pct (pp)
    feedback_count_90d: Optional[int] = None

    # performance over time (from Performance Over Time JSON)
    perf_over_time_recent_defect_pct: Optional[float] = None   # most recent full week
    perf_over_time_prior_defect_pct: Optional[float] = None    # prior week
    perf_over_time_defect_trend_delta: Optional[float] = None  # recent - prior (pp)

    # statement payout health
    deferred_transactions_amount: Optional[float] = None  # from StatementsSummary
    deferred_transactions_pct: Optional[float] = None     # deferred / total balance
    account_level_reserve_amount: float = 0.0             # max reserve seen across statements
    # Regular Statements account level reserve history
    stmt_reserve_periods: list[float] = field(default_factory=list)
    stmt_reserve_consecutive_negative: int = 0
    stmt_reserve_max_negative: float = 0.0
    stmt_reserve_is_worsening: bool = False
    # Reserve ratio analysis
    stmt_reserve_latest_ratio: Optional[float] = None   # reserve/gross_revenue for most recent closed stmt
    stmt_reserve_avg_ratio: Optional[float] = None      # avg ratio across valid closed stmts (last 90d)
    stmt_reserve_change_pct: Optional[float] = None     # (latest - avg) / avg * 100
    unavailable_balance_amount: float = 0.0               # max unavailable seen across statements
    failed_disbursement_count: int = 0                    # count of cancelled/failed transfers in statements (90d)
    failed_disbursement_most_recent: bool = False         # True if most recent closed statement is a failed disbursement
    negative_deposit_latest: bool = False                 # True if most recent closed statement has negative Deposit Total
    negative_deposit_consecutive: int = 0                 # count of consecutive closed statements with negative Deposit Total

    # policy compliance trend (cross-period)
    curr_policy_total: Optional[int] = None       # sum of all policy_compliance fields
    prev_policy_total: Optional[int] = None       # from previous BQ record
    policy_total_delta: Optional[int] = None      # curr - prev (positive = got worse)

    # B2B account level reserve history (from Statements_B2B)
    b2b_reserve_periods: list[float] = field(default_factory=list)  # negative = held by Amazon
    b2b_reserve_consecutive_negative: int = 0     # how many consecutive recent periods negative
    b2b_reserve_max_negative: float = 0.0         # largest negative amount seen (absolute value)
    b2b_reserve_is_worsening: bool = False        # True if amounts getting more negative over time

    # loans
    active_loans_count: int = 0
    closed_loans_count: int = 0
    outstanding_loan_amount: float = 0.0
    past_due_amount: float = 0.0
    has_loan_history: bool = False

    # notifications
    notification_titles: list[str] = field(default_factory=list)
    notification_count: int = 0
    high_risk_notification_count: int = 0   # computed during extraction
    inv_credit_card_notification: bool = False  # credit card notification on report date or day before
    acc_deactivation_notification: bool = False  # account deactivation risk notification on report date or day before

    # inventory
    inv_report_value: Optional[float] = None
    inv_report_amazon_fulfilled_value: Optional[float] = None

    # Fulfillment type (from Performance Over Time)
    seller_fulfilled_odr: Optional[float] = None  # ODR from Seller Fulfilled rows only; None if no SF data

    # short/long term order metrics
    cancellation_orders_short_term: Optional[float] = None
    order_defect_orders_short_term: Optional[float] = None
    late_shipment_orders_short_term: Optional[float] = None
    chargeback_claims_orders_short_term: Optional[float] = None
    negative_feedback_orders_short_term: Optional[float] = None
    a_to_z_orders_short_term: Optional[float] = None


# FAILED_DISB_WINDOW_DAYS moved to json_risk_agent_config Supabase table (key: failed_disb_window_days)

# ── Risk notification keywords ─────────────────────────────────────────────────

_HIGH_RISK_NOTIF_KEYWORDS = [
    "deactivat", "suspend", "remov", "urgent", "at risk", "violation",
    "restricted product", "trademark", "intellectual property", "authenticity",
    "defective", "safety", "high cancellation", "late shipment", "account health",
    "temporarily removed", "action required", "policy warning",
]


def _count_risky_notifications(titles: list[str]) -> int:
    count = 0
    for title in titles:
        lower = title.lower()
        if any(kw in lower for kw in _HIGH_RISK_NOTIF_KEYWORDS):
            count += 1
    return count


# ── Numeric helpers ────────────────────────────────────────────────────────────

def _pct_str_to_float(s: str) -> Optional[float]:
    """Convert '3 %(1)' → 3.0, '0.07%' → 0.07, 'no data' → None."""
    if not s or s.strip().lower() in ("no data", "n/a", ""):
        return None
    # pattern like "44 %(4)"  — the leading number is the percentage
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*%", s)
    if m:
        return float(m.group(1))
    # plain number
    m2 = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)", s)
    if m2:
        return float(m2.group(1))
    return None


def _money_to_float(s: str) -> Optional[float]:
    """Convert '$147,940.31' → 147940.31, '-$921.25' → -921.25."""
    if not s:
        return None
    s = s.strip()
    negative = s.startswith("-")
    cleaned = re.sub(r"[^\d.]", "", s.replace(",", ""))
    try:
        val = float(cleaned)
        return -val if negative else val
    except ValueError:
        return None


def _safe_int(val: Any) -> Optional[int]:
    try:
        return int(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


# ── Main extractor ─────────────────────────────────────────────────────────────

def extract_features(row: dict) -> FeatureSet:
    """
    Given a raw BigQuery row dict, return a populated FeatureSet.
    The heavy lifting is done on the `data` JSON column; structured BQ
    columns are used as a fallback / supplement.
    """
    fs = FeatureSet()

    # ── 1. Timestamps / dates ──────────────────────────────────────────────────
    fs.report_date = str(row.get("created_date") or "")

    # ── 2. Try to parse the data column ───────────────────────────────────────
    raw_data: str = row.get("data") or ""
    error_class = classify_error(raw_data)

    if error_class:
        fs.data_quality_flag = error_class
        # Try to at least get supplier info from the JSON
        try:
            d = json.loads(raw_data)
            mp_sup_key = row.get("mp_sup_key", "")
            cache = _get_supplier_key_cache()
            fs.supplier_key = cache.get(mp_sup_key, d.get("Supplier Key", mp_sup_key))
            fs.supplier_name = d.get("Supplier Name", "")
            fs.raw_error = d.get("Error", raw_data[:200])
        except json.JSONDecodeError:
            _mp = row.get("mp_sup_key", "")
            fs.supplier_key = _get_supplier_key_cache().get(_mp, _mp)
            fs.raw_error = raw_data[:200]
        return fs  # nothing more to do

    # ── 3. Parse the JSON ──────────────────────────────────────────────────────
    try:
        d: dict = json.loads(raw_data)
    except (json.JSONDecodeError, TypeError):
        log.warning("Could not parse data column for mp_sup_key=%s", row.get("mp_sup_key"))
        _mp = row.get("mp_sup_key", "")
        fs.supplier_key = _get_supplier_key_cache().get(_mp, _mp)
        fs.data_quality_flag = "json_parse_error"
        fs.raw_error = raw_data[:200] if raw_data else "empty"
        return fs

    # ── 4. Identifiers ────────────────────────────────────────────────────────
    mp_sup_key = row.get("mp_sup_key", "")
    cache = _get_supplier_key_cache()
    fs.supplier_key = cache.get(mp_sup_key, d.get("Supplier Key", mp_sup_key))
    fs.supplier_name = d.get("Supplier Name", "") or d.get("Legal Business Name", "")
    fs.seller_id = d.get("Seller ID", "")
    fs.store_name = d.get("Store Name", "")

    # ── 5. Account status ─────────────────────────────────────────────────────
    fs.account_status = d.get("Account Status", row.get("account_status", ""))
    fs.two_step_verification = d.get("Two step verification", "")
    fs.all_accounts_in_us = d.get("All accounts in US")

    # ── 6. Performance (structured BQ columns first, JSON supplement) ─────────
    def _bq_float(col: str) -> Optional[float]:
        v = row.get(col)
        return float(v) if v is not None else None

    fs.order_defect_rate = _bq_float("order_defect_rate")
    fs.late_shipment_rate = _bq_float("late_shipment_rate")
    fs.cancellation_rate = _bq_float("cancellation_rate")
    fs.valid_tracking_rate = _bq_float("valid_tracking_rate_all_cat")
    fs.delivered_on_time = _bq_float("delivered_on_time")
    fs.late_responses = _bq_float("late_responses")
    fs.return_dissatisfaction_rate = _bq_float("return_dissatisfaction_rate")
    fs.customer_service_dissatisfaction_rate = _bq_float("customer_service_dissatisfaction_rate_beta")

    # Richer strings from Account Performance Info
    perf = d.get("Account Performance Info", {})
    if perf:
        fs.odr_from_json = perf.get("Order Defect Rate")
        fs.late_shipment_from_json = perf.get("Late Shipment Rate")
        fs.cancellation_from_json = perf.get("Cancellation Rate")
        fs.valid_tracking_from_json = perf.get("Valid Tracking Rate - All Categories")
        fs.delivered_on_time_from_json = perf.get("Delivered on time")

        # Override BQ numerics if not present
        if fs.order_defect_rate is None:
            fs.order_defect_rate = _pct_str_to_float(fs.odr_from_json or "")
        if fs.late_shipment_rate is None:
            fs.late_shipment_rate = _pct_str_to_float(fs.late_shipment_from_json or "")
        if fs.cancellation_rate is None:
            fs.cancellation_rate = _pct_str_to_float(fs.cancellation_from_json or "")

    # ── 7. Sales ──────────────────────────────────────────────────────────────
    fs.sales_30_days = _bq_float("sales_30_days")
    fs.sales_7_days = _bq_float("sales_7_days")
    fs.channel_sales_all = _bq_float("channel_sales_all")
    fs.channel_sales_amazon = _bq_float("channel_sales_amazon")
    fs.channel_sales_seller = _bq_float("channel_sales_seller")

    # ── 8. Statements summary ─────────────────────────────────────────────────
    stmt_summary = d.get("StatementsSummary", {})
    if stmt_summary:
        tb = stmt_summary.get("Total Balance", {})
        fa = stmt_summary.get("Funds Available", {})
        fs.total_balance = _money_to_float(tb.get("All Accounts", ""))
        fs.funds_available = _money_to_float(fa.get("All Accounts", ""))

    # Recent deposits from closed statements
    for stmt in d.get("Statements", []):
        if stmt.get("ProcessingStatus") == "Closed":
            dep = _money_to_float(stmt.get("Deposit Total", ""))
            if dep is not None:
                fs.recent_deposits.append({
                    "period": stmt.get("Settlement Period", ""),
                    "amount": dep,
                })

    # ── 9. Complaints (BQ columns) ────────────────────────────────────────────
    fs.cust_complaints_authenticity = _bq_float("cust_complaints_prod_authenticity")
    fs.cust_complaints_safety = _bq_float("cust_complaints_prod_safety")
    fs.cust_complaints_ip = _bq_float("cust_complaints_intelectual_prop")
    fs.cust_complaints_policy = _bq_float("cust_complaints_policy_violation")

    # ── 10. Policy compliance (data JSON) ─────────────────────────────────────
    pc = d.get("policy_compliance", {})
    if pc:
        fs.policy_regulatory = _safe_int(pc.get("Regulatory Compliance"))
        fs.policy_other = _safe_int(pc.get("Other Policy Violations"))
        fs.policy_listing = _safe_int(pc.get("Listing Policy Violations"))
        fs.policy_food_safety = _safe_int(pc.get("Food and Product Safety Issues"))
        fs.policy_restricted = _safe_int(pc.get("Restricted Product Policy Violations"))
        fs.policy_condition = _safe_int(pc.get("Product Condition Customer Complaints"))
        fs.policy_authenticity = _safe_int(pc.get("Product Authenticity Customer Complaints"))
        fs.policy_ip_received = _safe_int(pc.get("Received Intellectual Property Complaints"))
        fs.policy_ip_suspected = _safe_int(pc.get("Suspected Intellectual Property Violations"))
        fs.policy_reviews = _safe_int(pc.get("Customer Product Reviews Policy Violations"))

    # ── 10b. Policy compliance total + cross-period delta ────────────────────
    pc = d.get("policy_compliance", {})
    if pc:
        _pc_fields = [
            "Other Policy Violations", "Listing Policy Violations",
            "Food and Product Safety Issues", "Restricted Product Policy Violations",
            "Product Condition Customer Complaints", "Product Authenticity Customer Complaints",
            "Received Intellectual Property Complaints", "Customer Product Reviews Policy Violations",
            "Suspected Intellectual Property Violations", "Regulatory Compliance",
        ]
        total = 0
        for f_name in _pc_fields:
            v = _safe_int(pc.get(f_name))
            if v:
                total += v
        fs.curr_policy_total = total

    # prev_policy_total comes directly from BQ row (injected by bq_loader)
    prev_total = row.get("prev_policy_total")
    if prev_total is not None:
        fs.prev_policy_total = int(prev_total)
        if fs.curr_policy_total is not None:
            fs.policy_total_delta = fs.curr_policy_total - fs.prev_policy_total

    # ── 10c. B2B Account Level Reserve history ────────────────────────────────
    b2b_statements = d.get("Statements_B2B", [])
    reserve_values: list[float] = []
    for stmt in b2b_statements:
        det = stmt.get("details") or {}
        if not det:
            continue
        reserve_str = det.get("Account Level Reserve", {}).get("Reserve", "") or ""
        if not reserve_str:
            continue
        # Parse: "$0.00", "-$355.05", "-$17,715.28"
        cleaned = reserve_str.replace("$", "").replace(",", "").strip()
        try:
            val = float(cleaned)
            reserve_values.append(val)
        except ValueError:
            continue

    if reserve_values:
        fs.b2b_reserve_periods = reserve_values
        # Consecutive negative periods from most recent (index 0 = most recent)
        consec = 0
        for v in reserve_values:
            if v < 0:
                consec += 1
            else:
                break
        fs.b2b_reserve_consecutive_negative = consec
        fs.b2b_reserve_max_negative = abs(min(reserve_values))  # largest magnitude
        # Worsening: most recent is more negative than second most recent
        if len(reserve_values) >= 2 and reserve_values[0] < 0 and reserve_values[1] < 0:
            fs.b2b_reserve_is_worsening = reserve_values[0] < reserve_values[1]

    # ── 11. Feedback ──────────────────────────────────────────────────────────
    fb = d.get("feedback", {})
    if fb:
        fs.feedback_rating_summary = fb.get("Summary", "")
        pos = fb.get("Positive", {}).get("30 days", "")
        neg = fb.get("Negative", {}).get("30 days", "")
        neu = fb.get("Neutral", {}).get("30 days", "")
        fs.feedback_positive_30d = _pct_str_to_float(pos)
        fs.feedback_negative_30d = _pct_str_to_float(neg)
        fs.feedback_neutral_30d = _pct_str_to_float(neu)
        cnt = fb.get("Count", {}).get("30 days")
        fs.feedback_count_30d = _safe_int(cnt)

    # ── 11b. Feedback trend (30d vs 60d window: days 31–90) ──────────────────
    # prior_60d = (90d_raw - 30d_raw) / (90d_count - 30d_count)
    # trend_delta = 30d_pct - prior_60d_pct  (positive = getting worse)
    if fb:
        neg_30d_str = fb.get("Negative", {}).get("30 days", "")
        neg_90d_str = fb.get("Negative", {}).get("90 days", "")
        cnt_30 = fs.feedback_count_30d or 0
        cnt_90 = _safe_int(fb.get("Count", {}).get("90 days")) or 0

        neg_30_pct = _pct_str_to_float(neg_30d_str)
        neg_90_pct = _pct_str_to_float(neg_90d_str)

        if neg_30_pct is not None and neg_90_pct is not None and cnt_90 > cnt_30 > 0:
            cnt_prior_60 = cnt_90 - cnt_30
            neg_30_raw = int(neg_30d_str.split("%(")[1].rstrip(")")) if "%(" in neg_30d_str else 0
            neg_90_raw = int(neg_90d_str.split("%(")[1].rstrip(")")) if "%(" in neg_90d_str else 0
            neg_prior_60_raw = neg_90_raw - neg_30_raw
            prior_60d_pct = (neg_prior_60_raw / cnt_prior_60 * 100) if cnt_prior_60 > 0 else 0.0
            fs.feedback_negative_prior60d_pct = prior_60d_pct
            fs.feedback_negative_trend_delta = (neg_30_pct or 0) - prior_60d_pct
        fs.feedback_count_90d = cnt_90

    # ── 11c. Performance Over Time trend ──────────────────────────────────────
    pot = d.get("Performance Over Time", {})
    af_rows = pot.get("Amazon Fulfilled", [])
    sf_rows = pot.get("Seller Fulfilled", [])

    # Seller-fulfilled ODR: use most recent SF row that has actual order data (orders > 0)
    sf_with_orders = [
        r for r in sf_rows
        if (_safe_int(r.get("Total Orders", "0") or "0") or 0) > 0
        and r.get("Total Orders With Defects") not in (None, "", "N/A")
    ]
    if sf_with_orders:
        fs.seller_fulfilled_odr = _pct_str_to_float(sf_with_orders[0].get("Total Orders With Defects", "") or "")

    # rows are ordered newest-first; skip same-day snapshots (low order count)
    full_weeks = [
        r for r in af_rows
        if _safe_int(r.get("Total Orders", "0") or "0") is not None
        and (_safe_int(r.get("Total Orders", "0") or "0") or 0) >= 10
    ]
    if len(full_weeks) >= 2:
        def _defect_pct(row: dict) -> Optional[float]:
            return _pct_str_to_float(row.get("Total Orders With Defects", "") or "")
        fs.perf_over_time_recent_defect_pct = _defect_pct(full_weeks[0])
        fs.perf_over_time_prior_defect_pct  = _defect_pct(full_weeks[1])
        if fs.perf_over_time_recent_defect_pct is not None and fs.perf_over_time_prior_defect_pct is not None:
            fs.perf_over_time_defect_trend_delta = (
                fs.perf_over_time_recent_defect_pct - fs.perf_over_time_prior_defect_pct
            )
    elif len(full_weeks) == 1:
        fs.perf_over_time_recent_defect_pct = _pct_str_to_float(
            full_weeks[0].get("Total Orders With Defects", "") or ""
        )

    # ── 11d. Statement payout health ─────────────────────────────────────────
    stmt_summary = d.get("StatementsSummary", {})
    if stmt_summary:
        tb = stmt_summary.get("Total Balance", {})
        deferred_str = tb.get("Deferred Transactions", "") or ""
        total_str    = tb.get("All Accounts", "") or ""
        deferred_amt = _money_to_float(deferred_str)
        total_amt    = _money_to_float(total_str)
        if deferred_amt is not None:
            fs.deferred_transactions_amount = deferred_amt
        if deferred_amt and total_amt and total_amt > 0:
            fs.deferred_transactions_pct = deferred_amt / total_amt * 100

    from datetime import datetime, timedelta, timezone
    from config.agent_config import cfg_int as _cfg_int
    _now = datetime.now(timezone.utc)
    _disb_cutoff = _now - timedelta(days=_cfg_int("failed_disb_window_days"))

    stmt_reserve_values: list[float] = []
    statements_list = d.get("Statements", [])
    for stmt in statements_list:
        det = stmt.get("details", {}) or {}

        # Account Level Reserve
        reserve_str = det.get("Account Level Reserve", {}).get("Reserve", "$0") or "$0"
        reserve_amt = _money_to_float(reserve_str) or 0.0
        # track max absolute reserve held (negative = Amazon holding funds)
        if abs(reserve_amt) > fs.account_level_reserve_amount:
            fs.account_level_reserve_amount = abs(reserve_amt)
        stmt_reserve_values.append(reserve_amt)

        # Unavailable balance — only from the most recent statement (first in list)
        if stmt == statements_list[0]:
            unavail_str = det.get("Closing Balance", {}).get("Unavailable balance", "$0") or "$0"
            fs.unavailable_balance_amount = _money_to_float(unavail_str) or 0.0

        # Failed/cancelled disbursement in InfoBox — only within time window
        end_date_str = stmt.get("end_date", "") or ""
        try:
            # Handle both "2026-03-13" and "2026-3-13" formats
            parts = [int(x) for x in end_date_str.split("-")]
            from datetime import date as _date
            stmt_end = datetime(parts[0], parts[1], parts[2], tzinfo=timezone.utc)
            within_window = stmt_end >= _disb_cutoff
        except (ValueError, TypeError, IndexError):
            within_window = True  # if date unparseable, include it

        infobox = det.get("InfoBox", "") or ""
        is_failed = any(kw in infobox.lower() for kw in ["canceled your transfer", "cancelled your transfer", "failed disbursement"])

        if within_window and is_failed:
            fs.failed_disbursement_count += 1

        # Check if most recent closed statement is a failed disbursement
        stmt_status = det.get("Status", "") or stmt.get("ProcessingStatus", "") or ""
        if stmt_status.lower() == "closed":
            if not fs.failed_disbursement_most_recent:
                fs.failed_disbursement_most_recent = is_failed

            # Track negative Deposit Total on closed statements (streak from most recent)
            deposit_amt = _money_to_float(stmt.get("Deposit Total", "") or "") or 0.0
            _streak_broken = getattr(fs, "_neg_deposit_streak_broken", False)
            if not _streak_broken:
                if deposit_amt < 0:
                    fs.negative_deposit_consecutive += 1
                    if fs.negative_deposit_consecutive == 1:
                        fs.negative_deposit_latest = True
                else:
                    fs._neg_deposit_streak_broken = True  # type: ignore

        # Build statements_detail for LLM context (most recent 8)
        if len(fs.statements_detail) < 8:
            fs.statements_detail.append({
                "period": stmt.get("Settlement Period", ""),
                "end_date": end_date_str,
                "deposit_usd": _money_to_float(stmt.get("Deposit Total", "")) or 0.0,
                "reserve_usd": reserve_amt,
                "status": det.get("Status", ""),
                "infobox": infobox[:120] if infobox else "",
            })

    if stmt_reserve_values:
        fs.stmt_reserve_periods = stmt_reserve_values
        consec = 0
        for v in stmt_reserve_values:
            if v < 0:
                consec += 1
            else:
                break
        fs.stmt_reserve_consecutive_negative = consec
        fs.stmt_reserve_max_negative = abs(min(stmt_reserve_values))
        if len(stmt_reserve_values) >= 2 and stmt_reserve_values[0] < 0 and stmt_reserve_values[1] < 0:
            fs.stmt_reserve_is_worsening = stmt_reserve_values[0] < stmt_reserve_values[1]

    # ── Reserve ratio analysis ────────────────────────────────────────────────
    # Only use closed statements within 90 days with gross revenue >= threshold
    from config.agent_config import cfg as _cfg
    _RESERVE_RATIO_MIN_REVENUE = _cfg("reserve_ratio_min_revenue_usd", 200.0)
    _RESERVE_RATIO_WINDOW_DAYS = 90
    _ratio_cutoff = _now - timedelta(days=_RESERVE_RATIO_WINDOW_DAYS)

    valid_ratios: list[float] = []
    latest_ratio: Optional[float] = None

    for stmt in statements_list:
        det = stmt.get("details", {}) or {}
        status = (det.get("Status", "") or stmt.get("ProcessingStatus", "") or "").lower()
        if status != "closed":
            continue

        # Parse end_date
        end_date_str = stmt.get("end_date", "") or ""
        try:
            parts = [int(x) for x in end_date_str.split("-")]
            stmt_end = datetime(parts[0], parts[1], parts[2], tzinfo=timezone.utc)
        except (ValueError, TypeError, IndexError):
            continue

        if stmt_end < _ratio_cutoff:
            continue

        # Gross Revenue = Product Charges + Shipping - Refunded Sales
        sales = det.get("Sales", {}) or {}
        refunds = det.get("Refunds", {}) or {}
        product_charges  = _money_to_float(sales.get("Product charges", "") or "") or 0.0
        shipping         = _money_to_float(sales.get("Shipping", "") or "") or 0.0
        refunded_sales   = abs(_money_to_float(refunds.get("Refunded sales", "") or "") or 0.0)
        gross_revenue    = product_charges + shipping - refunded_sales

        if gross_revenue < _RESERVE_RATIO_MIN_REVENUE:
            continue

        reserve_str = det.get("Account Level Reserve", {}).get("Reserve", "") or ""
        reserve_amt = _money_to_float(reserve_str) or 0.0
        if reserve_amt >= 0:
            continue  # only care about negative reserve

        ratio = abs(reserve_amt) / gross_revenue
        valid_ratios.append(ratio)
        if latest_ratio is None:
            latest_ratio = ratio  # first valid closed stmt is the most recent

    if valid_ratios and latest_ratio is not None:
        fs.stmt_reserve_latest_ratio = latest_ratio
        fs.stmt_reserve_avg_ratio = sum(valid_ratios) / len(valid_ratios)
        if fs.stmt_reserve_avg_ratio > 0:
            fs.stmt_reserve_change_pct = (
                (latest_ratio - fs.stmt_reserve_avg_ratio) / fs.stmt_reserve_avg_ratio * 100
            )

    # ── 12. Loans ─────────────────────────────────────────────────────────────
    # Active loans
    active_loans = d.get("Loans", {})
    # Loans may be a dict keyed by loan-id or a list
    if isinstance(active_loans, dict):
        for loan_id, loan_data in active_loans.items():
            if isinstance(loan_data, dict):
                fs.active_loans_count += 1
                outstanding = loan_data.get("outstandingLoanAmount", 0) or 0
                past_due = loan_data.get("pastDueAmount", 0) or 0
                fs.outstanding_loan_amount += float(outstanding)
                fs.past_due_amount += float(past_due)
    elif isinstance(active_loans, list):
        for loan in active_loans:
            if isinstance(loan, dict):
                fs.active_loans_count += 1
                fs.outstanding_loan_amount += float(loan.get("outstandingLoanAmount", 0) or 0)
                fs.past_due_amount += float(loan.get("pastDueAmount", 0) or 0)

    # Closed loans
    closed_loans = d.get("Closed Loans", [])
    fs.closed_loans_count = len(closed_loans) if isinstance(closed_loans, list) else 0
    fs.has_loan_history = (fs.active_loans_count + fs.closed_loans_count) > 0

    # External loans
    ext_loans = d.get("External Loans", [])
    if isinstance(ext_loans, list):
        for loan in ext_loans:
            if isinstance(loan, dict):
                fs.active_loans_count += 1
                fs.outstanding_loan_amount += float(loan.get("outstandingLoanAmount", 0) or 0)
                fs.past_due_amount += float(loan.get("pastDueAmount", 0) or 0)

    # ── 13. Notifications ─────────────────────────────────────────────────────
    notif_titles = d.get("Last Notification Titles", [])
    if isinstance(notif_titles, list):
        fs.notification_titles = notif_titles
        fs.notification_count = len(notif_titles)
        fs.high_risk_notification_count = _count_risky_notifications(notif_titles)

        # Check for credit card notification on report date or previous day
        try:
            from datetime import date as _date_cls, timedelta as _td
            _report_dt = _date_cls.fromisoformat(fs.report_date) if fs.report_date else None
            if _report_dt:
                _window = {_report_dt, _report_dt - _td(days=1)}
                for title in notif_titles:
                    title_lower = title.lower()
                    # Parse date from title: "March 15, 2026: ..."
                    try:
                        _date_part = title.split(":")[0].strip()
                        _notif_dt = _date_cls.fromisoformat(
                            datetime.strptime(_date_part, "%B %d, %Y").strftime("%Y-%m-%d")
                        )
                    except (ValueError, IndexError):
                        continue
                    if _notif_dt not in _window:
                        continue
                    if "credit card" in title_lower:
                        fs.inv_credit_card_notification = True
                    if (
                        "at risk of deactivation" in title_lower
                        or "account is at risk" in title_lower
                        or "selling account" in title_lower and "deactivat" in title_lower
                    ):
                        fs.acc_deactivation_notification = True
        except Exception:
            pass

    # ── 14. Inventory ─────────────────────────────────────────────────────────
    fs.inv_report_value = _bq_float("inv_report_value")
    fs.inv_report_amazon_fulfilled_value = _bq_float("inv_report_amazon_fulfilled_value")

    # ── 15. Short-term order metrics ─────────────────────────────────────────
    fs.cancellation_orders_short_term = _bq_float("cancellation_orders_short_term")
    fs.order_defect_orders_short_term = _bq_float("order_defect_orders_short_term")
    fs.late_shipment_orders_short_term = _bq_float("late_shipment_orders_short_term")
    fs.chargeback_claims_orders_short_term = _bq_float("chargeback_claims_orders_short_term")
    fs.negative_feedback_orders_short_term = _bq_float("negative_feedback_orders_short_term")
    fs.a_to_z_orders_short_term = _bq_float("a_to_z_guarantee_claims_orders_short_term")

    return fs