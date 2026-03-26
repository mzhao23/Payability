import os
from dotenv import load_dotenv

load_dotenv(override=True)

# ── BigQuery ────────────────────────────────────────────────
BQ_PROJECT = "bigqueryexport-183608"
BQ_TABLE = "bigqueryexport-183608.dynamodb.app_production_tracking_hub_trackingLabels0BF56DC6_19QOKS1UQM9IN"

# ── Supabase ────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

# ── LLM / Gemini ────────────────────────────────────────────
GATEWAY_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
GATEWAY_MODEL = "gemini-2.5-flash"
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]

# ── Metric Parameters (adjustable) ─────────────────────────
PARAMS = {
    # Metric 1
    "ship_sla_days": 5,       # Buffer days before flagging as untracked
    "window_days": 7,          # Rolling window for untracked rate

    # Metric 2
    "zscore_threshold": 2.0,   # Price anomaly z-score threshold

    # Metric 3
    "stuck_days": 5,           # Days since init before flagging as stuck (FedEx only)

    # Shared
    "baseline_days": 30,       # Historical baseline period
}

# ── Risk Scoring Thresholds ──────────────────────────────────
# Reference only — authoritative values are in prompts/llm_risk_scorer.md
RISK_THRESHOLDS = {
    # Metric 1 — untracked_rate (rate × volume matrix, see prompt)
    "untracked_min_orders": 10,     # Below this, carrier signal is weak
    "untracked_rate_low": 0.1,      # rate < 0.1 → no score
    "untracked_rate_mid": 0.3,      # rate 0.1–0.3 → low tier
    "untracked_rate_high": 0.6,     # rate 0.3–0.6 → mid tier; >0.6 → high tier

    # Metric 2 — price_escalation z-score tiers
    "zscore_min": 2.0,              # Below this → no score
    "zscore_mid": 3.0,              # 2.0–3.0 → +1
    "zscore_high": 4.5,             # 3.0–4.5 → +2; >4.5 → +3
    "max_zscore_signal": 5.0,       # max_zscore meaningful threshold
    "min_orders_price": 5,          # min orders for price signal

    # Metric 3A — FedEx pickup lag
    "pickup_lag_diff_high": 2.0,    # >2 days vs baseline → +1 (supporting only)
}