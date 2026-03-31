"""config/agent_config.py

Loads risk agent tuning parameters from the `json_risk_agent_config` Supabase table.
Falls back to hardcoded defaults if the table is unavailable.

Usage:
    from config.agent_config import cfg
    threshold = cfg("odr_threshold_pct")
"""

from __future__ import annotations

from typing import Optional
from supabase import create_client, Client
from config import settings
from utils.logger import get_logger

log = get_logger("agent_config")

# ── Hardcoded defaults (used if Supabase is unavailable) ──────────────────────
_DEFAULTS: dict[str, float] = {
    # Hard rule floors
    "floor_account_status":           8,
    "floor_loan_past_due":            9,
    "floor_order_defect_rate":        8,
    "floor_late_shipment_rate":       8,
    "floor_neg_feedback_trend":       7,
    "floor_policy_compliance":        7,
    "floor_reserve_consecutive":      7,
    "floor_reserve_amount":           7,
    "floor_failed_disbursement":      7,
    # Hard rule thresholds
    "odr_threshold_pct":              1.0,
    "late_shipment_threshold_pct":    4.0,
    "neg_feedback_trend_hard_pp":     10.0,
    "neg_feedback_min_sample":        10,
    "policy_delta_hard":              5,
    "stmt_reserve_consec_hard":       2,
    "stmt_reserve_amount_hard_usd":   5000.0,
    "reserve_ratio_change_hard_pct":  50.0,  # change_pct >= 50% → hard rule floor 7
    "reserve_ratio_change_soft_pct":  10.0,  # 10% < change_pct < 50% → soft +1
    "reserve_ratio_min_revenue_usd":  200.0, # skip statements with gross revenue < this
    # Soft rule thresholds
    "cancellation_threshold_pct":     2.5,
    "cancellation_elevated_pct":      1.5,
    "valid_tracking_min_pct":         95.0,
    "delivered_on_time_min_pct":      85.0,
    "negative_feedback_high_pct":     50.0,  # > 50% → soft +2
    "negative_feedback_elev_pct":     30.0,  # 30–50% → soft +1
    "policy_delta_soft":              2,
    "deferred_soft_pct":              50.0,
    "deferred_soft_amt_usd":          5000.0,
    "notifications_hard_count":       10,
    "notifications_soft_hi_count":    5,
    "notifications_soft_lo_count":    2,
    "unavailable_balance_soft_usd":   1000.0,
    # Scoring formula
    "soft_only_max":                  6.0,
    "soft_with_hard_max":             6.0,
    "score_max":                      10.0,
    "hard_floor_divisor":             6.0,
    "soft_hard_divisor":              6.0,
    # Data quality short-circuit scores
    "dq_score_not_authorized":        8,
    "dq_score_login_error":           7,
    "dq_score_wrong_password":        7,
    "dq_score_scraper_error":         8,  # JSON has top-level Error field — score 8
    "dq_score_bank_page_error":       8,  # raised from 5 — bank page errors indicate access issues
    "dq_score_internal_error":        8,  # no longer used in short-circuit — internal errors now go through LLM with floor 8
    "dq_score_json_parse_error":      4,
    "dq_score_advance_only":          2,
    "dq_score_onboarding_only":       2,
    "dq_score_default":               3,
    # Pipeline / operational
    "failed_disb_window_days":        90,
    "llm_score_threshold":            5,
}

_config: dict[str, float] | None = None


def _load_from_supabase() -> dict[str, float]:
    """Fetch all rows from json_risk_agent_config and return as a dict."""
    client: Client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
    result = client.table("json_risk_agent_config").select("key, value").execute()
    loaded = {row["key"]: float(row["value"]) for row in result.data}
    log.info("Loaded %d config params from json_risk_agent_config.", len(loaded))
    return loaded


def load_config(force_reload: bool = False) -> None:
    """Load config from Supabase into memory. Called once at pipeline startup."""
    global _config
    if _config is not None and not force_reload:
        return
    try:
        remote = _load_from_supabase()
        # Merge: start with defaults, overlay with remote values
        _config = {**_DEFAULTS, **remote}
        missing = [k for k in _DEFAULTS if k not in remote]
        if missing:
            log.warning(
                "%d config key(s) not found in Supabase, using defaults: %s",
                len(missing), ", ".join(missing),
            )
    except Exception as exc:
        log.warning(
            "Could not load config from Supabase (%s) — using hardcoded defaults.", exc
        )
        _config = dict(_DEFAULTS)


def cfg(key: str, default: Optional[float] = None) -> float:
    """Get a config value by key. Loads from defaults if not yet initialised."""
    if _config is None:
        load_config()
    val = _config.get(key)  # type: ignore[union-attr]
    if val is None:
        if default is not None:
            return default
        raise KeyError(f"Unknown config key: '{key}' — add it to json_risk_agent_config or _DEFAULTS")
    return val


def cfg_int(key: str, default: Optional[int] = None) -> int:
    """Get a config value as int."""
    return int(cfg(key, float(default) if default is not None else None))