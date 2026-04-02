"""agent/claude_agent.py

Sends the FeatureSet + pre-score context to an LLM and parses the structured
JSON risk report back.

Provider is controlled by the LLM_PROVIDER env var:
  LLM_PROVIDER=claude   → Anthropic Claude (default)
  LLM_PROVIDER=gemini   → Google Gemini (Google AI Studio)
"""

from __future__ import annotations

import json
import re
from typing import Any

import anthropic
from google import genai
from google.genai import errors as genai_errors
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config import settings
from config.agent_config import cfg_int as _cfg_int

from config.models import Metric, RiskReport
from extractors.feature_extractor import FeatureSet
from scoring.rule_scorer import PreScoreResult
from utils.logger import get_logger

log = get_logger("claude_agent")

# ── System prompt ──────────────────────────────────────────────────────────────
_SYSTEM_PROMPT = """
You are an expert marketplace risk analyst specialising in Amazon third-party sellers.
Your job is to assess the financial and operational risk of a supplier based on their
Amazon seller data and output a structured JSON risk report.

RISK SCORE DEFINITION (1-10):
1-2  Very Low   — healthy metrics, no compliance issues, consistent sales
3-4  Low        — minor issues, within Amazon thresholds, manageable
5-6  Moderate   — approaching thresholds, some violations, monitoring needed
7-8  High       — breaching Amazon policy thresholds, active violations, loan concerns
9-10 Critical   — account at risk of suspension, past-due loans, severe violations

OUTPUT FORMAT — respond ONLY with a valid JSON object, no markdown, no preamble:
{
  "table_name": "<string>",
  "supplier_key": "<string>",
  "supplier_name": "<string>",
  "report_date": "<YYYY-MM-DD>",
  "metrics": [
    {"metric_id": "<string>", "value": <number|string|null>, "unit": "<string|null>"}
  ],
  "trigger_reason": "<concise 2-4 sentence English explanation of the key risk drivers>",
  "overall_risk_score": <float 1.0-10.0, e.g. 7.83>
}

REQUIRED metrics to include (use null if data is unavailable):
  order_defect_rate, late_shipment_rate, cancellation_rate,
  valid_tracking_rate, delivered_on_time,
  feedback_negative_30d, feedback_negative_60d_window, feedback_negative_trend_delta,
  feedback_count_30d,
  outstanding_loan_amount, past_due_amount,
  policy_compliance_total,
  sales_30_days, total_balance, funds_available,
  stmt_reserve_latest_ratio, stmt_reserve_avg_ratio, stmt_reserve_change_pct,
  failed_disbursement_count,
  high_risk_notification_count, account_status

You may add additional metrics if they are significant for the risk assessment.
Do NOT include the raw_error or data_quality_flag in the metrics array.

IMPORTANT JUDGEMENT GUIDELINES:
0. **Hard rule floor transparency**: The `rule_engine_preliminary_score` reflects hard
   rule floors that fired. If your `overall_risk_score` is lower than the preliminary
   score, you MUST explicitly explain in `trigger_reason` why the hard rule signals do
   not constitute material risk in this case (e.g. the issue is historical and resolved,
   the account is otherwise healthy, etc.). Unexplained downward adjustments from hard
   rule floors are not acceptable.

1. Rule engine signals are alerts, not conclusions. Use them as starting points, not as
   mechanical score inputs. Weigh them against the full picture.

2. Prioritise current account health over historical events:
   - If ODR, LSR, feedback, and account_status are all healthy, this is a strong positive
     signal that should meaningfully offset historical flags.
   - A seller with excellent current metrics should not score above 6 based solely on
     historical issues unless there is evidence of ongoing risk.

3. Interpreting failed_disbursements:
   - Check statements_detail for context. If the failed disbursement was followed by
     normal successful transfers in subsequent periods, treat it as a resolved historical
     event, not an active risk.
   - Only treat as active risk if the most recent statement(s) show a failed transfer.

4. Interpreting reserve:
   - Reserve being negative means Amazon is holding funds — this is common and not inherently risky.
   - Focus on stmt_reserve_change_pct: how much the reserve/revenue ratio has changed vs the 90-day average.
   - A stable or shrinking ratio is normal. A ratio spike (>50%) signals Amazon is holding
     a disproportionate share of recent revenue, which is a risk signal.
   - A large but stable reserve on a high-volume account is normal operational behaviour.

5. Late shipment rate (late_shipment_rate_pct):
   - This rule only fires when fbm_orders_60 >= 20, ensuring low-volume FBM sellers are
     not penalized by a single late shipment. If triggered, treat it as a meaningful signal.

6. Notifications — date awareness:
   - `recent_notification_titles` includes the most recent notifications with their dates.
   - ALWAYS check the date of each notification relative to `report_date`. Notifications
     older than 3 days before `report_date` are historical context only — do NOT treat
     them as active current risk unless they are corroborated by other current signals.
   - A notification about disbursement deactivation from several days ago does not mean
     disbursements are still deactivated today. If the account status is OK and recent
     statements show successful transfers, the issue has likely been resolved.

7. Policy compliance:
   - Policy violation counts are provided for context ONLY — do NOT use them to drive
     the risk score up. The rule engine has disabled policy compliance scoring because
     violations cannot yet be distinguished by whether they impact account health.
   - Food safety violations are especially common for food/grocery sellers and rarely
     indicate real risk. Ignore them unless there is direct evidence of account suspension.
   - Deferred transactions are normal under Amazon's DD+7 policy (payments held until
     7 days after delivery). Do NOT treat deferred balance as a risk signal.
""".strip()

# ── Provider clients (initialised lazily based on LLM_PROVIDER) ──────────────
_anthropic_client: anthropic.Anthropic | None = None
_gemini_model: genai.Client | None = None

def _get_anthropic_client() -> anthropic.Anthropic:
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
    return _anthropic_client

def _get_gemini_client() -> genai.Client:
    global _gemini_model
    if _gemini_model is None:
        _gemini_model = genai.Client(api_key=settings.GEMINI_API_KEY)
    return _gemini_model


def _build_user_message(
    fs: FeatureSet,
    pre: PreScoreResult,
    table_name: str,
) -> str:
    """Serialise the FeatureSet and pre-score into a prompt message."""

    # Build a concise JSON payload — avoid dumping the entire raw dataclass
    payload: dict[str, Any] = {
        "table_name": table_name,
        "supplier_key": fs.supplier_key,
        "supplier_name": fs.supplier_name or fs.store_name,
        "report_date": fs.report_date,
        "data_quality_flag": fs.data_quality_flag,
        "raw_error": fs.raw_error,
        # Account
        "account_status": fs.account_status,
        "two_step_verification": fs.two_step_verification,
        "all_accounts_in_us": fs.all_accounts_in_us,
        # Performance
        "order_defect_rate_pct": fs.seller_fulfilled_odr,  # None for FBA-only sellers
        "late_shipment_rate_pct": fs.late_shipment_rate,
        "cancellation_rate_pct": fs.cancellation_rate,
        "valid_tracking_rate_pct": fs.valid_tracking_rate,
        "delivered_on_time_pct": fs.delivered_on_time,
        "return_dissatisfaction_rate_pct": fs.return_dissatisfaction_rate,
        # Sales
        "sales_30_days_usd": fs.sales_30_days,
        "sales_7_days_usd": fs.sales_7_days,
        "channel_sales_all_usd": fs.channel_sales_all,
        # Statements / payout health
        "total_balance_usd": fs.total_balance,
        "funds_available_usd": fs.funds_available,
        "recent_statement_deposits": fs.recent_deposits[:6],
        "stmt_reserve_consecutive_negative": fs.stmt_reserve_consecutive_negative,
        "stmt_reserve_max_negative_usd": fs.stmt_reserve_max_negative,
        "stmt_reserve_is_worsening": fs.stmt_reserve_is_worsening,
        "stmt_reserve_latest_ratio": fs.stmt_reserve_latest_ratio,
        "stmt_reserve_avg_ratio": fs.stmt_reserve_avg_ratio,
        "stmt_reserve_change_pct": fs.stmt_reserve_change_pct,
        "failed_disbursement_count": fs.failed_disbursement_count,
        "failed_disbursement_most_recent": fs.failed_disbursement_most_recent,
        "negative_deposit_latest":        fs.negative_deposit_latest,
        "negative_deposit_consecutive":   fs.negative_deposit_consecutive,
        "unavailable_balance_usd": fs.unavailable_balance_amount,
        "statements_detail": fs.statements_detail,         # per-statement: period, reserve, deposit, infobox
        "stmt_reserve_periods_usd": fs.stmt_reserve_periods,  # reserve amounts most-recent-first
        # Feedback
        "feedback_summary": fs.feedback_rating_summary,
        "feedback_positive_30d_pct": fs.feedback_positive_30d,
        "feedback_negative_30d_pct": fs.feedback_negative_30d,
        "feedback_negative_60d_window_pct": fs.feedback_negative_prior60d_pct,
        "feedback_negative_trend_delta_pp": fs.feedback_negative_trend_delta,
        "feedback_count_30d": fs.feedback_count_30d,
        "feedback_count_90d": fs.feedback_count_90d,
        # Loans
        "active_loans_count": fs.active_loans_count,
        "closed_loans_count": fs.closed_loans_count,
        "outstanding_loan_amount_usd": fs.outstanding_loan_amount,
        "past_due_amount_usd": fs.past_due_amount,
        # Policy compliance
        "policy_compliance_total": fs.curr_policy_total,
        "policy_compliance_prev_total": fs.prev_policy_total,
        "policy_compliance_delta": fs.policy_total_delta,
        "policy_breakdown": {
            "other": fs.policy_other,
            "listing": fs.policy_listing,
            "food_safety": fs.policy_food_safety,
            "restricted": fs.policy_restricted,
            "authenticity": fs.policy_authenticity,
            "ip_received": fs.policy_ip_received,
            "ip_suspected": fs.policy_ip_suspected,
        },
        # Customer complaints
        "cust_complaints_authenticity": fs.cust_complaints_authenticity,
        "cust_complaints_safety": fs.cust_complaints_safety,
        "cust_complaints_ip": fs.cust_complaints_ip,
        "cust_complaints_policy": fs.cust_complaints_policy,
        # Notifications
        "high_risk_notification_count": fs.high_risk_notification_count,
        "total_notification_count": fs.notification_count,
        "recent_notification_titles": fs.notification_titles[:5],  # capped to reduce tokens
        # Inventory
        "inv_report_value_usd": fs.inv_report_value,
        "inv_report_amazon_fulfilled_usd": fs.inv_report_amazon_fulfilled_value,
        # Pre-score from rule engine
        "rule_engine_preliminary_score": pre.preliminary_score,
        "rule_engine_triggered_rules": pre.triggered_rules,
    }

    # Compact JSON (no indentation) to reduce token usage
    return (
        "Analyse the following Amazon seller data and return the risk report JSON.\n\n"
        f"SELLER DATA:\n{json.dumps(payload, separators=(',', ':'), default=str)}"
    )


def _parse_llm_response(text: str) -> dict:
    """Extract the JSON object from the LLM response text."""
    # Strip markdown code fences if present
    text = re.sub(r"```(?:json)?", "", text).strip().rstrip("`").strip()

    # Find the outermost { … }
    start = text.find("{")
    end = text.rfind("}") + 1
    if start == -1 or end == 0:
        raise ValueError(f"No JSON object found in LLM response: {text[:300]}")
    return json.loads(text[start:end])


# ── LLM call — provider selected via LLM_PROVIDER env var ───────────────────

@retry(
    retry=retry_if_exception_type((
        anthropic.RateLimitError, anthropic.APIStatusError,
        genai_errors.APIError,
    )),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    stop=stop_after_attempt(4),
)
def _call_llm(user_message: str) -> str:
    """Call the configured LLM provider and return the raw text response."""
    provider = settings.LLM_PROVIDER.lower()
    if provider == "gemini":
        response = _get_gemini_client().models.generate_content(
            model=settings.GEMINI_MODEL,
            contents=user_message,
            config=genai.types.GenerateContentConfig(
                system_instruction=_SYSTEM_PROMPT,
                max_output_tokens=4000,
                thinking_config=genai.types.ThinkingConfig(thinking_budget=0),  # disable thinking to save tokens
            ),
        )
        return response.text
    else:
        # Default: Claude
        response = _get_anthropic_client().messages.create(
            model=settings.ANTHROPIC_MODEL,
            max_tokens=2000,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        return response.content[0].text


# ── Flags that mean there is nothing useful for the LLM to analyse -----------
_SKIP_LLM_FLAGS = {
    "login_error",
    "not_authorized",
    "wrong_password",
    "bank_page_error",
    # "internal_error" removed — errors now go through LLM with score floor 8
    "json_parse_error",
    "advance_only",
    "onboarding_only",
}


# ── Public interface ----------------------------------------------------------

def analyse(
    fs: FeatureSet,
    pre: PreScoreResult,
    table_name: str,
) -> RiskReport:
    """
    Run the full AI analysis and return a validated RiskReport.

    Rows with data-quality errors are scored by the rule engine only --
    no LLM call is made, saving cost and latency.
    Falls back to a rule-only report if the LLM call fails after retries.
    """
    # Skip LLM for error / no-data rows
    if fs.data_quality_flag in _SKIP_LLM_FLAGS:
        log.info(
            "Skipping LLM for supplier_key=%s (data_quality_flag=%s)",
            fs.supplier_key, fs.data_quality_flag,
        )
        return _fallback_report(
            fs, pre, table_name,
            error=f"Skipped: data_quality_flag={fs.data_quality_flag}. Raw: {fs.raw_error or ''}",
        )

    # Skip LLM if score is below threshold AND no hard rules were triggered
    # Hard rules push score to 6+ on their own, so score>=5 also catches hard rule rows.
    # Threshold: preliminary_score >= 5 OR any hard rule triggered.
    _LLM_SCORE_THRESHOLD = _cfg_int("llm_score_threshold")
    _NO_RISK_MSG = "No significant risk indicators detected by rule engine."

    should_use_llm = pre.preliminary_score >= _LLM_SCORE_THRESHOLD

    if not should_use_llm:
        log.info(
            "Skipping LLM for supplier_key=%s — score=%d < %d and no hard rules (rules: %s)",
            fs.supplier_key, pre.preliminary_score, _LLM_SCORE_THRESHOLD,
            "; ".join(pre.triggered_rules[:2]) or "none",
        )
        return _fallback_report(
            fs, pre, table_name,
            error=f"Skipped: score={pre.preliminary_score} below threshold={_LLM_SCORE_THRESHOLD} and no hard rules triggered.",
        )

    user_msg = _build_user_message(fs, pre, table_name)

    parsed = None
    last_exc = None
    for attempt in range(1, 3):  # up to 2 attempts
        try:
            raw_text = _call_llm(user_msg)
            log.debug("Raw LLM response (attempt %d): %s", attempt, raw_text[:500])
            parsed = _parse_llm_response(raw_text)
            break
        except json.JSONDecodeError as exc:
            last_exc = exc
            log.warning(
                "LLM returned invalid JSON for supplier_key=%s (attempt %d/2): %s — retrying.",
                fs.supplier_key, attempt, exc,
            )
        except Exception as exc:
            last_exc = exc
            log.error(
                "LLM call failed for supplier_key=%s: %s — falling back to rule-only report.",
                fs.supplier_key, exc,
            )
            return _fallback_report(fs, pre, table_name, error=str(exc))

    if parsed is None:
        log.error(
            "LLM returned invalid JSON for supplier_key=%s after 2 attempts — falling back.",
            fs.supplier_key,
        )
        return _fallback_report(fs, pre, table_name, error=str(last_exc))

    # ── Validate and coerce ────────────────────────────────────────────────────
    try:
        # Filter LLM-returned metrics to only triggered ones
        _triggered_ids = {
            mid
            for rule in pre.triggered_rules
            for key, ids in _RULE_METRIC_MAP.items()
            if key in rule.upper()
            for mid in ids
        } | _ALWAYS_INCLUDE_METRICS
        metrics = [
            Metric(
                metric_id=str(m.get("metric_id", "")),
                value=m.get("value"),
                unit=m.get("unit"),
            )
            for m in parsed.get("metrics", [])
            if str(m.get("metric_id", "")) in _triggered_ids
        ]
        # If LLM returned nothing relevant, fallback to rule-derived metrics
        if not metrics:
            metrics = _metrics_for_triggered_rules(fs, pre.triggered_rules)

        score = float(parsed.get("overall_risk_score", pre.preliminary_score))
        score = max(1.0, min(10.0, score))

        report = RiskReport(
            table_name=table_name,
            supplier_key=fs.supplier_key,
            supplier_name=fs.supplier_name or fs.store_name or parsed.get("supplier_name", ""),
            report_date=fs.report_date or parsed.get("report_date", ""),
            metrics=metrics,
            trigger_reason=parsed.get("trigger_reason", ""),
            overall_risk_score=score,
            data_quality_flag=fs.data_quality_flag,
            raw_error=fs.raw_error,
        )
        return report

    except Exception as exc:
        log.error("Failed to parse LLM JSON response for %s: %s", fs.supplier_key, exc)
        return _fallback_report(fs, pre, table_name, error=str(exc))


def _build_trigger_reason(fs: FeatureSet, pre: PreScoreResult, error: str = "") -> str:
    """
    Generate a human-readable trigger reason without calling the LLM.
    Used for skipped rows (low score, error flags, or LLM fallback).
    """
    score = pre.preliminary_score

    # Data quality errors
    if fs.data_quality_flag != "ok":
        labels = {
            "login_error":      "Login failed — credentials may be invalid or expired.",
            "not_authorized":   "Account not authorized — email not associated with any Amazon account.",
            "wrong_password":   "Authentication failed — incorrect password.",
            "bank_page_error":  "Bank account page could not be loaded — data may be incomplete.",
            "internal_error":   "Internal scraper error — data collection was incomplete.",
            "json_parse_error": "Data column could not be parsed — JSON is malformed.",
            "advance_only":     "Only advance offer data available — seller may be inactive or very new.",
            "onboarding_only":  "Only onboarding data available — seller has not started selling yet.",
        }
        base = labels.get(fs.data_quality_flag, f"Data quality issue: {fs.data_quality_flag}.")
        if error and "Skipped" not in error:
            base += f" Detail: {error[:150]}"
        return base

    # No risk flags at all
    _NO_RISK_MSG = "No significant risk indicators detected by rule engine."
    if pre.triggered_rules == [_NO_RISK_MSG]:
        return (
            "No risk indicators detected. All performance metrics are within acceptable "
            "thresholds and no policy violations or compliance issues were identified."
        )

    # Low score with some soft rules — build a readable summary
    rules = [r for r in pre.triggered_rules if r != _NO_RISK_MSG]

    # Group rules into categories for a cleaner sentence
    categories = {
        "performance":  [],
        "feedback":     [],
        "policy":       [],
        "loan":         [],
        "notification": [],
        "account":      [],
    }
    for r in rules:
        rl = r.lower()
        if any(k in rl for k in ["order_defect", "late_shipment", "cancellation", "tracking", "delivered", "perf_"]):
            categories["performance"].append(r.split(":")[0])
        elif "feedback" in rl:
            categories["feedback"].append(r.split(":")[0])
        elif any(k in rl for k in ["policy", "complaint", "ip_", "authenticity", "food"]):
            categories["policy"].append(r.split(":")[0])
        elif "loan" in rl or "past_due" in rl:
            categories["loan"].append(r.split(":")[0])
        elif "notification" in rl:
            categories["notification"].append(r.split(":")[0])
        elif any(k in rl for k in ["reserve", "disbursement", "deferred", "unavailable"]):
            categories["account"].append(r.split(":")[0])
        else:
            categories["account"].append(r.split(":")[0])

    parts = []
    if categories["performance"]:
        parts.append(f"elevated performance metrics ({', '.join(set(categories['performance']))})")
    if categories["feedback"]:
        parts.append("elevated negative customer feedback")
    if categories["policy"]:
        parts.append(f"policy or compliance issues ({', '.join(set(categories['policy']))})")
    if categories["loan"]:
        parts.append("outstanding loan balance")
    if categories["notification"]:
        parts.append(f"{fs.high_risk_notification_count} high-risk account notifications")
    if categories["account"]:
        parts.append("account status concerns")

    if parts:
        issues = ", ".join(parts[:-1]) + (" and " + parts[-1] if len(parts) > 1 else parts[0])
        return (
            f"Rule-based assessment (score {score}/10). "
            f"Minor risk signals detected: {issues}. "
            f"No individual metric breaches Amazon policy thresholds at this time."
        )

    # Fallback to raw rule list if categorisation produces nothing
    return (
        f"Rule-based assessment (score {score}/10). "
        f"Triggered rules: {'; '.join(rules[:5])}."
    )


def _fallback_report(
    fs: FeatureSet,
    pre: PreScoreResult,
    table_name: str,
    error: str = "",
) -> RiskReport:
    """Rule-only report — used when LLM is skipped or unavailable."""
    metrics = _metrics_for_triggered_rules(fs, pre.triggered_rules)
    return RiskReport(
        table_name=table_name,
        supplier_key=fs.supplier_key,
        supplier_name=fs.supplier_name or fs.store_name,
        report_date=fs.report_date,
        metrics=metrics,
        trigger_reason=_build_trigger_reason(fs, pre, error),
        overall_risk_score=pre.preliminary_score,
        data_quality_flag=fs.data_quality_flag,
        raw_error=fs.raw_error,
    )


# Maps rule name keywords → metric_ids to include when that rule fires
_RULE_METRIC_MAP: dict[str, list[str]] = {
    "ACCOUNT_STATUS":           ["account_status"],
    "LOAN_PAST_DUE":            ["past_due_amount", "outstanding_loan_amount"],
    "ORDER_DEFECT_RATE":        ["order_defect_rate"],
    "LATE_SHIPMENT_RATE":       ["late_shipment_rate", "fbm_orders_60"],
    "NEG_FEEDBACK_TREND":       ["feedback_negative_30d", "feedback_negative_60d_window", "feedback_negative_trend_delta", "feedback_count_30d"],
    "POLICY_COMPLIANCE":        ["policy_compliance_total", "policy_compliance_delta"],
    "ACCOUNT_LEVEL_RESERVE":    ["stmt_reserve_latest_ratio", "stmt_reserve_avg_ratio", "stmt_reserve_change_pct"],
    "FAILED_DISBURSEMENT":      ["failed_disbursement_count"],
    "NEGATIVE_DEPOSIT":         ["negative_deposit_consecutive"],
    "HIGH_RISK_NOTIFICATION":   ["high_risk_notification_count"],
    "CANCELLATION":             ["cancellation_rate"],
    "VALID_TRACKING":           ["valid_tracking_rate"],
    "DELIVERED_ON_TIME":        ["delivered_on_time"],
    "NEGATIVE_FEEDBACK":        ["feedback_negative_30d", "feedback_count_30d"],
    "LOAN_OUTSTANDING":         ["outstanding_loan_amount"],
    "UNAVAILABLE_BALANCE":      ["unavailable_balance"],
    "DATA_QUALITY":             ["account_status"],
}

# Always include these regardless of triggered rules
_ALWAYS_INCLUDE_METRICS = {"account_status", "overall_feedback_rating"}


def _metrics_for_triggered_rules(fs: FeatureSet, triggered_rules: list[str]) -> list[Metric]:
    """Return only metrics relevant to the triggered rules."""
    # Collect metric_ids from triggered rules
    metric_ids: set[str] = set(_ALWAYS_INCLUDE_METRICS)
    for rule in triggered_rules:
        rule_upper = rule.upper()
        for key, ids in _RULE_METRIC_MAP.items():
            if key in rule_upper:
                metric_ids.update(ids)

    # Full metric pool to pick from
    all_metrics: dict[str, Metric] = {
        "late_shipment_rate":              Metric(metric_id="late_shipment_rate",              value=fs.late_shipment_rate,                unit="%"),
        "fbm_orders_60":                   Metric(metric_id="fbm_orders_60",                   value=fs.fbm_orders_60 or None,             unit="orders"),
        "order_defect_rate":               Metric(metric_id="order_defect_rate",               value=fs.seller_fulfilled_odr,              unit="%"),
        "late_shipment_rate":              Metric(metric_id="late_shipment_rate",              value=fs.late_shipment_rate,                unit="%"),
        "cancellation_rate":               Metric(metric_id="cancellation_rate",               value=fs.cancellation_rate,                 unit="%"),
        "valid_tracking_rate":             Metric(metric_id="valid_tracking_rate",             value=fs.valid_tracking_rate,               unit="%"),
        "delivered_on_time":               Metric(metric_id="delivered_on_time",               value=fs.delivered_on_time,                 unit="%"),
        "feedback_negative_30d":           Metric(metric_id="feedback_negative_30d",           value=fs.feedback_negative_30d,             unit="%"),
        "feedback_negative_60d_window":    Metric(metric_id="feedback_negative_60d_window",    value=fs.feedback_negative_prior60d_pct,    unit="%"),
        "feedback_negative_trend_delta":   Metric(metric_id="feedback_negative_trend_delta",   value=fs.feedback_negative_trend_delta,     unit="pp"),
        "feedback_count_30d":              Metric(metric_id="feedback_count_30d",              value=fs.feedback_count_30d,                unit=None),
        "overall_feedback_rating":         Metric(metric_id="overall_feedback_rating",         value=fs.feedback_rating_summary or None,   unit=None),
        "outstanding_loan_amount":         Metric(metric_id="outstanding_loan_amount",         value=fs.outstanding_loan_amount,           unit="USD"),
        "past_due_amount":                 Metric(metric_id="past_due_amount",                 value=fs.past_due_amount,                   unit="USD"),
        "policy_compliance_total":         Metric(metric_id="policy_compliance_total",         value=fs.curr_policy_total,                 unit=None),
        "policy_compliance_delta":         Metric(metric_id="policy_compliance_delta",         value=fs.policy_total_delta,                unit=None),
        "stmt_reserve_consecutive_negative": Metric(metric_id="stmt_reserve_consecutive_negative", value=fs.stmt_reserve_consecutive_negative, unit="periods"),
        "stmt_reserve_max_negative":       Metric(metric_id="stmt_reserve_max_negative",       value=fs.stmt_reserve_max_negative or None, unit="USD"),
        "stmt_reserve_latest_ratio":       Metric(metric_id="stmt_reserve_latest_ratio",       value=fs.stmt_reserve_latest_ratio,         unit="ratio"),
        "stmt_reserve_avg_ratio":          Metric(metric_id="stmt_reserve_avg_ratio",          value=fs.stmt_reserve_avg_ratio,            unit="ratio"),
        "stmt_reserve_change_pct":         Metric(metric_id="stmt_reserve_change_pct",         value=fs.stmt_reserve_change_pct,           unit="%"),
        "failed_disbursement_count":       Metric(metric_id="failed_disbursement_count",       value=fs.failed_disbursement_count or None, unit=None),
        "negative_deposit_latest":         Metric(metric_id="negative_deposit_latest",         value=fs.negative_deposit_latest if fs.negative_deposit_latest else None,           unit=None),
        "negative_deposit_consecutive":    Metric(metric_id="negative_deposit_consecutive",    value=fs.negative_deposit_consecutive if fs.negative_deposit_consecutive > 0 else None, unit="periods"),
        "high_risk_notification_count":    Metric(metric_id="high_risk_notification_count",    value=fs.high_risk_notification_count,      unit=None),
        "account_status":                  Metric(metric_id="account_status",                  value=fs.account_status,                    unit=None),
        "unavailable_balance":             Metric(metric_id="unavailable_balance",             value=fs.unavailable_balance_amount or None, unit="USD"),
    }

    return [m for mid, m in all_metrics.items() if mid in metric_ids and m.value is not None]


def _build_fallback_metrics(fs: FeatureSet) -> list[Metric]:
    return [
        Metric(metric_id="order_defect_rate",                value=fs.seller_fulfilled_odr,                 unit="%"),  # None for FBA-only sellers
        Metric(metric_id="late_shipment_rate",               value=fs.late_shipment_rate,                   unit="%"),
        Metric(metric_id="cancellation_rate",                value=fs.cancellation_rate,                    unit="%"),
        Metric(metric_id="valid_tracking_rate",              value=fs.valid_tracking_rate,                  unit="%"),
        Metric(metric_id="delivered_on_time",                value=fs.delivered_on_time,                    unit="%"),
        Metric(metric_id="feedback_negative_30d",            value=fs.feedback_negative_30d,                unit="%"),
        Metric(metric_id="feedback_negative_60d_window",     value=fs.feedback_negative_prior60d_pct,       unit="%"),
        Metric(metric_id="feedback_negative_trend_delta",    value=fs.feedback_negative_trend_delta,        unit="pp"),
        Metric(metric_id="feedback_count_30d",               value=fs.feedback_count_30d,                   unit=None),
        Metric(metric_id="outstanding_loan_amount",          value=fs.outstanding_loan_amount,              unit="USD"),
        Metric(metric_id="past_due_amount",                  value=fs.past_due_amount,                      unit="USD"),
        Metric(metric_id="policy_compliance_total",          value=fs.curr_policy_total,                    unit=None),
        Metric(metric_id="policy_compliance_delta",          value=fs.policy_total_delta,                   unit=None),
        Metric(metric_id="stmt_reserve_latest_ratio",        value=fs.stmt_reserve_latest_ratio,             unit="ratio"),
        Metric(metric_id="stmt_reserve_avg_ratio",            value=fs.stmt_reserve_avg_ratio,                unit="ratio"),
        Metric(metric_id="stmt_reserve_change_pct",           value=fs.stmt_reserve_change_pct,               unit="%"),
        Metric(metric_id="failed_disbursement_count",        value=fs.failed_disbursement_count or None,    unit=None),
        Metric(metric_id="high_risk_notification_count",     value=fs.high_risk_notification_count,         unit=None),
        Metric(metric_id="account_status",                   value=fs.account_status,                       unit=None),
    ]