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
from config.agent_config import cfg, cfg_int


@dataclass
class PreScoreResult:
    preliminary_score: float = 1.0
    triggered_rules: list[str] = field(default_factory=list)
    hard_floors: list[int] = field(default_factory=list)  # all fired hard rule floors
    # but may be scored based on error type alone.


# Data quality scores loaded dynamically from json_risk_agent_config via cfg()


def score(fs: FeatureSet) -> PreScoreResult:
    result = PreScoreResult()
    rules: list[str] = result.triggered_rules

    # ── Data quality short-circuit ────────────────────────────────────────────
    if fs.data_quality_flag != "ok":
        score_val = cfg_int(f"dq_score_{fs.data_quality_flag}", cfg_int("dq_score_default"))
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
        hard(cfg_int("floor_account_status"), f"ACCOUNT_STATUS: '{fs.account_status}' (not OK/Active, threshold OK)")

    # ── 2. Loan past due ──────────────────────────────────────────────────────
    if fs.past_due_amount > 0:
        hard(cfg_int("floor_loan_past_due"), f"LOAN_PAST_DUE: past due amount = ${fs.past_due_amount:,.2f} (threshold >$0)")

    # ── 3. Negative feedback trend (30d vs prior 30d) ─────────────────────────
    _neg_sample = fs.feedback_count_30d or 0
    if (
        fs.feedback_negative_trend_delta is not None
        and _neg_sample >= cfg_int("neg_feedback_min_sample")
        and fs.feedback_negative_trend_delta >= cfg("neg_feedback_trend_hard_pp")
    ):
        neg_30 = fs.feedback_negative_30d or 0
        prior  = neg_30 - fs.feedback_negative_trend_delta
        hard(cfg_int("floor_neg_feedback_trend"), (
            f"NEG_FEEDBACK_TREND: 30d neg rate {neg_30:.1f}% vs 60d window {prior:.1f}% "
            f"(+{fs.feedback_negative_trend_delta:.1f}pp, threshold +{cfg('neg_feedback_trend_hard_pp')}pp, n={_neg_sample})"
        ))

    # ── 4. Policy compliance increase ─────────────────────────────────────────
    # TEMPORARILY DISABLED — policy violations not yet distinguished by health impact
    # Re-enable when account_health_impact flag is available in data
    # if fs.policy_total_delta is not None:
    #     if fs.policy_total_delta >= cfg_int("policy_delta_hard"):
    #         hard(cfg_int("floor_policy_compliance"), (
    #             f"POLICY_COMPLIANCE_INCREASE: total violations up +{fs.policy_total_delta} "
    #             f"vs prior record (curr={fs.curr_policy_total}, "
    #             f"prev={fs.prev_policy_total}, threshold +{cfg_int('policy_delta_hard')})"
    #         ))

    # ── 6. Account Level Reserve (ratio-based) ───────────────────────────────
    if fs.stmt_reserve_change_pct is not None:
        change = fs.stmt_reserve_change_pct
        if change >= cfg("reserve_ratio_change_hard_pct"):
            hard(cfg_int("floor_reserve_consecutive"), (
                f"ACCOUNT_LEVEL_RESERVE: reserve/revenue ratio increased {change:.0f}% "
                f"(latest={fs.stmt_reserve_latest_ratio:.2f}x, "
                f"avg={fs.stmt_reserve_avg_ratio:.2f}x, "
                f"threshold +{cfg('reserve_ratio_change_hard_pct'):.0f}%)"
            ))

    # ── 7. Account deactivation risk notification ────────────────────────────
    if fs.acc_deactivation_notification:
        hard(cfg_int("floor_acc_deactivation"), (
            "ACC_DEACTIVATION: account at risk of deactivation notification on or before report date"
        ))

    # ── 7b. Credit card notification (invoice/payment issue) ─────────────────
    if fs.inv_credit_card_notification:
        hard(cfg_int("floor_inv_credit_card"), (
            "INV_CREDIT_CARD: credit card update required notification on or before report date"
        ))

    # ── 8. Failed / cancelled disbursement ───────────────────────────────────
    # Most recent closed statement is a failed disbursement → active risk
    if fs.failed_disbursement_most_recent:
        hard(cfg_int("floor_failed_disbursement"), "FAILED_DISBURSEMENT: most recent closed statement is a failed disbursement")
    # Historical failed disbursements within 90 days (but since recovered) → soft signal
    elif fs.failed_disbursement_count >= 1:
        soft(1, f"FAILED_DISBURSEMENT: {fs.failed_disbursement_count} historical failed transfer(s) in past 90 days (since recovered)")

    # ══════════════════════════════════════════════════════════════════════════
    # SOFT RULES — weak signals, additive penalty only
    # ══════════════════════════════════════════════════════════════════════════

    # ── Performance metrics ───────────────────────────────────────────────────
    # ODR: only evaluated using seller-fulfilled data from Performance Over Time.
    # If no SF data available (None), rule is skipped — no fallback to global ODR.
    _odr = fs.seller_fulfilled_odr

    if _odr is not None:
        if _odr > cfg("odr_threshold_pct"):
            hard(cfg_int("floor_order_defect_rate"), f"ORDER_DEFECT_RATE: {_odr:.2f}% > {cfg('odr_threshold_pct')}% (Amazon red line, seller-fulfilled)")

    # LATE_SHIPMENT_RATE rule removed — no total order count available,
    # so a single late shipment can produce artificially high rates for low-volume sellers

    if fs.cancellation_rate is not None:
        if fs.cancellation_rate > cfg("cancellation_threshold_pct"):
            soft(2, f"CANCELLATION_RATE: {fs.cancellation_rate:.2f}% > {cfg('cancellation_threshold_pct')}%")
        elif fs.cancellation_rate > cfg("cancellation_elevated_pct"):
            soft(1, f"CANCELLATION_RATE: {fs.cancellation_rate:.2f}% (elevated)")

    if fs.valid_tracking_rate is not None and fs.valid_tracking_rate < cfg("valid_tracking_min_pct"):
        soft(2, f"VALID_TRACKING_RATE: {fs.valid_tracking_rate:.2f}% < {cfg('valid_tracking_min_pct')}%")

    if fs.delivered_on_time is not None and fs.delivered_on_time < cfg("delivered_on_time_min_pct"):
        soft(1, f"DELIVERED_ON_TIME: {fs.delivered_on_time:.1f}% < {cfg('delivered_on_time_min_pct')}%")

    if fs.two_step_verification and fs.two_step_verification.lower() not in ("active", ""):
        soft(1, f"TWO_STEP_VERIFICATION: status='{fs.two_step_verification}'")

    # ── Feedback ──────────────────────────────────────────────────────────────
    if fs.feedback_negative_30d is not None and _neg_sample >= cfg_int("neg_feedback_min_sample"):
        if fs.feedback_negative_30d > cfg("negative_feedback_high_pct"):
            soft(2, f"NEGATIVE_FEEDBACK_30D: {fs.feedback_negative_30d:.1f}% (n={_neg_sample})")
        elif fs.feedback_negative_30d > cfg("negative_feedback_elev_pct"):
            soft(1, f"NEGATIVE_FEEDBACK_30D: {fs.feedback_negative_30d:.1f}% (elevated, n={_neg_sample})")

    # ── Loans ─────────────────────────────────────────────────────────────────
    if fs.outstanding_loan_amount > 0:
        soft(1, f"LOAN_OUTSTANDING: balance = ${fs.outstanding_loan_amount:,.0f}")

    # ── Policy compliance (absolute levels as weak signals) ───────────────────
    # TEMPORARILY DISABLED — policy violations not yet distinguished by health impact
    # Re-enable when account_health_impact flag is available in data
    # if fs.curr_policy_total is not None and fs.curr_policy_total > 0:
    #     if fs.curr_policy_total >= 20:
    #         soft(2, f"POLICY_TOTAL_HIGH: {fs.curr_policy_total} total violations")
    #     elif fs.curr_policy_total >= 5:
    #         soft(1, f"POLICY_TOTAL_ELEVATED: {fs.curr_policy_total} total violations")
    #
    # if fs.policy_total_delta is not None and cfg_int("policy_delta_soft") <= fs.policy_total_delta < cfg_int("policy_delta_hard"):
    #     soft(1, f"POLICY_COMPLIANCE_INCREASE: +{fs.policy_total_delta} vs prior record (soft)")

    # ── Notifications ─────────────────────────────────────────────────────────
    if fs.high_risk_notification_count >= cfg_int("notifications_hard_count"):
        soft(2, f"HIGH_RISK_NOTIFICATIONS: {fs.high_risk_notification_count} (high)")
    elif fs.high_risk_notification_count >= cfg_int("notifications_soft_hi_count"):
        soft(2, f"HIGH_RISK_NOTIFICATIONS: {fs.high_risk_notification_count}")
    elif fs.high_risk_notification_count >= cfg_int("notifications_soft_lo_count"):
        soft(1, f"HIGH_RISK_NOTIFICATIONS: {fs.high_risk_notification_count}")

    if fs.stmt_reserve_change_pct is not None:
        change = fs.stmt_reserve_change_pct
        if cfg("reserve_ratio_change_soft_pct") < change < cfg("reserve_ratio_change_hard_pct"):
            soft(1, (
                f"ACCOUNT_LEVEL_RESERVE: reserve/revenue ratio elevated {change:.0f}% above avg "
                f"(latest={fs.stmt_reserve_latest_ratio:.2f}x, avg={fs.stmt_reserve_avg_ratio:.2f}x)"
            ))

    if not fs.failed_disbursement_most_recent and fs.unavailable_balance_amount >= cfg("unavailable_balance_soft_usd"):
        soft(1, f"UNAVAILABLE_BALANCE: ${fs.unavailable_balance_amount:,.0f} in recent statement")



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
        # Hard path: max_floor + sum(other_floors)/6 + min(soft, cfg("soft_with_hard_max"))/6
        sorted_floors = sorted(result.hard_floors, reverse=True)
        max_floor   = sorted_floors[0]
        other_sum   = sum(sorted_floors[1:])
        final = min(cfg("score_max"), max_floor + other_sum / cfg("hard_floor_divisor") + min(penalty, cfg("soft_with_hard_max")) / cfg("soft_hard_divisor"))
    else:
        # Soft only: capped at soft_only_max — never exceeds the lowest hard rule floor
        final = min(cfg("soft_only_max"), 1 + penalty)

    result.preliminary_score = final

    if not rules:
        rules.append("No significant risk indicators detected by rule engine.")

    return result