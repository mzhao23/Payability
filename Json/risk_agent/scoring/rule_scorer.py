"""scoring/rule_scorer.py

Rule-based pre-scoring layer.

Hard rules: each sets a score floor AND adds a flag. These represent
clear, directional risk signals — trend-based or threshold-breaching events.

Soft rules: additive penalty (+1 or +2) for weaker signals.
- If any hard rule fires: soft penalty capped at +2
- If no hard rule fires: soft penalty capped at +6 (max score 7)

This means a score of 10 can only be reached by LOAN_PAST_DUE (floor 9)
combined with other hard/soft signals, or by extreme hard rule stacking.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from extractors.feature_extractor import FeatureSet

# ── Thresholds ─────────────────────────────────────────────────────────────────

# Hard rule thresholds
POLICY_DELTA_HARD        = 5       # compliance total increase to trigger hard rule
NEG_FEEDBACK_TREND_HARD  = 10.0    # pp increase in 30d vs prior 30d
NEG_FEEDBACK_MIN_SAMPLE  = 10      # minimum orders for feedback rules
PERF_DEGRADATION_HARD    = 0.5     # pp WoW defect rate increase
B2B_RESERVE_CONSEC_HARD  = 2       # consecutive negative reserve periods
B2B_RESERVE_AMOUNT_HARD  = 5000.0  # single-period negative reserve magnitude

# Soft rule thresholds
ODR_THRESHOLD            = 1.0     # %
ODR_ELEVATED             = 0.5     # %
LATE_SHIPMENT_THRESHOLD  = 4.0     # %
LATE_SHIPMENT_ELEVATED   = 2.0     # %
CANCELLATION_THRESHOLD   = 2.5     # %
CANCELLATION_ELEVATED    = 1.5     # %
VALID_TRACKING_MIN       = 95.0    # %
DELIVERED_ON_TIME_MIN    = 85.0    # %
NEGATIVE_FEEDBACK_HIGH   = 10.0    # %
NEGATIVE_FEEDBACK_ELEV   = 5.0     # %
POLICY_DELTA_SOFT        = 2       # compliance total increase for soft signal
DEFERRED_SOFT_PCT        = 50.0    # %
DEFERRED_SOFT_AMT        = 5000.0  # USD
NOTIFICATIONS_HARD       = 10      # count for hard floor
NOTIFICATIONS_SOFT_HI    = 5       # count for soft +2
NOTIFICATIONS_SOFT_LO    = 2       # count for soft +1

# Scoring formula caps
SOFT_ONLY_MAX            = 6.0     # max score when no hard rules fire
SOFT_WITH_HARD_MAX       = 6.0     # max soft_penalty considered when hard rules fire
SCORE_MAX                = 10.0    # absolute ceiling
HARD_FLOOR_DIVISOR       = 6.0     # divisor for additional hard rule floors
SOFT_HARD_DIVISOR        = 6.0     # divisor for soft penalty when hard rules fire


@dataclass
class PreScoreResult:
    preliminary_score: float = 1.0
    triggered_rules: list[str] = field(default_factory=list)
    hard_floors: list[int] = field(default_factory=list)  # all fired hard rule floors
    # but may be scored based on error type alone.


_DATA_QUALITY_SCORES: dict[str, int] = {
    "not_authorized":  8,
    "login_error":     7,
    "wrong_password":  7,
    "bank_page_error": 5,
    "internal_error":  4,
    "json_parse_error": 4,
    "advance_only":    2,
    "onboarding_only": 2,
}


def score(fs: FeatureSet) -> PreScoreResult:
    result = PreScoreResult()
    rules: list[str] = result.triggered_rules

    # ── Data quality short-circuit ────────────────────────────────────────────
    if fs.data_quality_flag != "ok":
        score_val = _DATA_QUALITY_SCORES.get(fs.data_quality_flag, 3)
        result.preliminary_score = score_val
        rules.append(f"DATA_QUALITY: flag={fs.data_quality_flag} (score={score_val})")
        return result

    penalty = 0

    def hard(floor: int, msg: str) -> None:
        result.hard_floors.append(floor)
        rules.append(msg)

    def soft(pts: int, msg: str) -> None:
        nonlocal penalty
        penalty += pts
        rules.append(msg)

    # ══════════════════════════════════════════════════════════════════════════
    # HARD RULES — directional trend signals and critical threshold breaches
    # ══════════════════════════════════════════════════════════════════════════

    # ── 1. Account status ─────────────────────────────────────────────────────
    if fs.account_status and fs.account_status.upper() not in ("OK", "ACTIVE", ""):
        hard(8, f"ACCOUNT_STATUS: '{fs.account_status}' (not OK/Active, threshold OK)")

    # ── 2. Loan past due ──────────────────────────────────────────────────────
    if fs.past_due_amount > 0:
        hard(9, f"LOAN_PAST_DUE: past due amount = ${fs.past_due_amount:,.2f} (threshold >$0)")

    # ── 3. Negative feedback trend (30d vs prior 30d) ─────────────────────────
    _neg_sample = fs.feedback_count_30d or 0
    if (
        fs.feedback_negative_trend_delta is not None
        and _neg_sample >= NEG_FEEDBACK_MIN_SAMPLE
        and fs.feedback_negative_trend_delta >= NEG_FEEDBACK_TREND_HARD
    ):
        neg_30 = fs.feedback_negative_30d or 0
        prior  = neg_30 - fs.feedback_negative_trend_delta
        hard(7, (
            f"NEG_FEEDBACK_TREND: 30d neg rate {neg_30:.1f}% vs 60d window {prior:.1f}% "
            f"(+{fs.feedback_negative_trend_delta:.1f}pp, threshold +{NEG_FEEDBACK_TREND_HARD}pp, n={_neg_sample})"
        ))

    # ── 4. Performance Over Time degradation ──────────────────────────────────
    if fs.perf_over_time_defect_trend_delta is not None:
        delta  = fs.perf_over_time_defect_trend_delta
        recent = fs.perf_over_time_recent_defect_pct or 0
        if delta >= PERF_DEGRADATION_HARD:
            hard(7, (
                f"PERF_DEGRADATION: defect rate {recent:.2f}% "
                f"(+{delta:.2f}pp WoW, threshold +{PERF_DEGRADATION_HARD}pp)"
            ))

    # ── 5. Policy compliance increase ─────────────────────────────────────────
    if fs.policy_total_delta is not None:
        if fs.policy_total_delta >= POLICY_DELTA_HARD:
            hard(7, (
                f"POLICY_COMPLIANCE_INCREASE: total violations up +{fs.policy_total_delta} "
                f"vs prior record (curr={fs.curr_policy_total}, "
                f"prev={fs.prev_policy_total}, threshold +{POLICY_DELTA_HARD})"
            ))

    # ── 6. B2B Account Level Reserve ─────────────────────────────────────────
    if fs.b2b_reserve_consecutive_negative >= B2B_RESERVE_CONSEC_HARD:
        hard(7, (
            f"ACCOUNT_LEVEL_RESERVE: negative reserve for "
            f"{fs.b2b_reserve_consecutive_negative} consecutive periods "
            f"(max ${fs.b2b_reserve_max_negative:,.0f}, threshold {B2B_RESERVE_CONSEC_HARD} periods)"
        ))
    elif fs.b2b_reserve_max_negative >= B2B_RESERVE_AMOUNT_HARD:
        hard(7, (
            f"ACCOUNT_LEVEL_RESERVE: single-period reserve "
            f"${fs.b2b_reserve_max_negative:,.0f} "
            f"(threshold ${B2B_RESERVE_AMOUNT_HARD:,.0f})"
        ))

    # ── 7. Failed / cancelled disbursement ───────────────────────────────────
    if fs.failed_disbursement_count >= 2:
        hard(7, f"FAILED_DISBURSEMENT: {fs.failed_disbursement_count} cancelled transfers (threshold 2)")
    elif fs.failed_disbursement_count == 1:
        hard(6, f"FAILED_DISBURSEMENT: 1 cancelled/failed transfer detected")

    # ══════════════════════════════════════════════════════════════════════════
    # SOFT RULES — weak signals, additive penalty only
    # ══════════════════════════════════════════════════════════════════════════

    # ── Performance metrics ───────────────────────────────────────────────────
    # ODR: only evaluated using seller-fulfilled data from Performance Over Time.
    # If no SF data available (None), rule is skipped — no fallback to global ODR.
    _odr = fs.seller_fulfilled_odr

    if _odr is not None:
        if _odr > ODR_THRESHOLD:
            hard(8, f"ORDER_DEFECT_RATE: {_odr:.2f}% > {ODR_THRESHOLD}% (Amazon red line, seller-fulfilled)")
        elif _odr > ODR_ELEVATED:
            soft(1, f"ORDER_DEFECT_RATE: {_odr:.2f}% (elevated, seller-fulfilled)")

    if fs.late_shipment_rate is not None:
        if fs.late_shipment_rate > LATE_SHIPMENT_THRESHOLD:
            hard(8, f"LATE_SHIPMENT_RATE: {fs.late_shipment_rate:.2f}% > {LATE_SHIPMENT_THRESHOLD}% (Amazon red line)")
        elif fs.late_shipment_rate > LATE_SHIPMENT_ELEVATED:
            soft(1, f"LATE_SHIPMENT_RATE: {fs.late_shipment_rate:.2f}% (elevated)")

    if fs.cancellation_rate is not None:
        if fs.cancellation_rate > CANCELLATION_THRESHOLD:
            soft(2, f"CANCELLATION_RATE: {fs.cancellation_rate:.2f}% > {CANCELLATION_THRESHOLD}%")
        elif fs.cancellation_rate > CANCELLATION_ELEVATED:
            soft(1, f"CANCELLATION_RATE: {fs.cancellation_rate:.2f}% (elevated)")

    if fs.valid_tracking_rate is not None and fs.valid_tracking_rate < VALID_TRACKING_MIN:
        soft(2, f"VALID_TRACKING_RATE: {fs.valid_tracking_rate:.2f}% < {VALID_TRACKING_MIN}%")

    if fs.delivered_on_time is not None and fs.delivered_on_time < DELIVERED_ON_TIME_MIN:
        soft(1, f"DELIVERED_ON_TIME: {fs.delivered_on_time:.1f}% < {DELIVERED_ON_TIME_MIN}%")

    if fs.two_step_verification and fs.two_step_verification.lower() not in ("active", ""):
        soft(1, f"TWO_STEP_VERIFICATION: status='{fs.two_step_verification}'")

    # ── Feedback ──────────────────────────────────────────────────────────────
    if fs.feedback_negative_30d is not None and _neg_sample >= NEG_FEEDBACK_MIN_SAMPLE:
        if fs.feedback_negative_30d > NEGATIVE_FEEDBACK_HIGH:
            soft(2, f"NEGATIVE_FEEDBACK_30D: {fs.feedback_negative_30d:.1f}% (n={_neg_sample})")
        elif fs.feedback_negative_30d > NEGATIVE_FEEDBACK_ELEV:
            soft(1, f"NEGATIVE_FEEDBACK_30D: {fs.feedback_negative_30d:.1f}% (elevated, n={_neg_sample})")

    # ── Loans ─────────────────────────────────────────────────────────────────
    if fs.outstanding_loan_amount > 0:
        soft(1, f"LOAN_OUTSTANDING: balance = ${fs.outstanding_loan_amount:,.0f}")

    # ── Policy compliance (absolute levels as weak signals) ───────────────────
    if fs.curr_policy_total is not None and fs.curr_policy_total > 0:
        if fs.curr_policy_total >= 20:
            soft(2, f"POLICY_TOTAL_HIGH: {fs.curr_policy_total} total violations")
        elif fs.curr_policy_total >= 5:
            soft(1, f"POLICY_TOTAL_ELEVATED: {fs.curr_policy_total} total violations")

    if fs.policy_total_delta is not None and POLICY_DELTA_SOFT <= fs.policy_total_delta < POLICY_DELTA_HARD:
        soft(1, f"POLICY_COMPLIANCE_INCREASE: +{fs.policy_total_delta} vs prior record (soft)")

    # ── Notifications ─────────────────────────────────────────────────────────
    if fs.high_risk_notification_count >= NOTIFICATIONS_HARD:
        soft(2, f"HIGH_RISK_NOTIFICATIONS: {fs.high_risk_notification_count} (high)")
    elif fs.high_risk_notification_count >= NOTIFICATIONS_SOFT_HI:
        soft(2, f"HIGH_RISK_NOTIFICATIONS: {fs.high_risk_notification_count}")
    elif fs.high_risk_notification_count >= NOTIFICATIONS_SOFT_LO:
        soft(1, f"HIGH_RISK_NOTIFICATIONS: {fs.high_risk_notification_count}")

    # ── Payout / deferred ─────────────────────────────────────────────────────
    if (
        fs.deferred_transactions_pct is not None
        and (fs.deferred_transactions_amount or 0) > DEFERRED_SOFT_AMT
        and fs.deferred_transactions_pct >= DEFERRED_SOFT_PCT
    ):
        soft(1, f"DEFERRED_TRANSACTIONS: {fs.deferred_transactions_pct:.0f}% of balance deferred")

    if fs.b2b_reserve_consecutive_negative == 1:
        soft(1, f"ACCOUNT_LEVEL_RESERVE: 1 period with negative reserve (${fs.b2b_reserve_max_negative:,.0f})")
    if fs.b2b_reserve_is_worsening and fs.b2b_reserve_consecutive_negative < B2B_RESERVE_CONSEC_HARD:
        soft(1, f"ACCOUNT_LEVEL_RESERVE: reserve worsening across periods")

    if fs.failed_disbursement_count == 0 and fs.unavailable_balance_amount >= 1000:
        soft(1, f"UNAVAILABLE_BALANCE: ${fs.unavailable_balance_amount:,.0f} in recent statement")

    # ── Performance Over Time (no WoW data — single period elevated) ──────────
    if fs.perf_over_time_defect_trend_delta is None and fs.perf_over_time_recent_defect_pct is not None:
        recent = fs.perf_over_time_recent_defect_pct
        if recent >= 1.0:
            soft(2, f"PERF_OVER_TIME_HIGH: defect rate {recent:.2f}%")
        elif recent >= 0.5:
            soft(1, f"PERF_OVER_TIME_ELEVATED: defect rate {recent:.2f}%")

    # ── Customer complaints ────────────────────────────────────────────────────
    for label, val in [
        ("AUTHENTICITY", fs.cust_complaints_authenticity),
        ("SAFETY",       fs.cust_complaints_safety),
        ("IP",           fs.cust_complaints_ip),
        ("POLICY",       fs.cust_complaints_policy),
    ]:
        if val is not None and val > 0:
            soft(1, f"CUSTOMER_COMPLAINT_{label}: {val}")

    # ══════════════════════════════════════════════════════════════════════════
    # Final score
    # ══════════════════════════════════════════════════════════════════════════
    if result.hard_floors:
        # Hard path: max_floor + sum(other_floors)/6 + min(soft, SOFT_WITH_HARD_MAX)/6
        sorted_floors = sorted(result.hard_floors, reverse=True)
        max_floor   = sorted_floors[0]
        other_sum   = sum(sorted_floors[1:])
        final = min(SCORE_MAX, max_floor + other_sum / HARD_FLOOR_DIVISOR + min(penalty, SOFT_WITH_HARD_MAX) / SOFT_HARD_DIVISOR)
    else:
        # Soft only: capped at SOFT_ONLY_MAX — never exceeds the lowest hard rule floor
        final = min(SOFT_ONLY_MAX, 1 + penalty)

    result.preliminary_score = final

    if not rules:
        rules.append("No significant risk indicators detected by rule engine.")

    return result