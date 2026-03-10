import os
from dotenv import load_dotenv

load_dotenv()

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
    "ship_sla_days": 3,       # Buffer days before flagging as untracked
    "window_days": 7,          # Rolling window for untracked rate

    # Metric 2
    "zscore_threshold": 2.0,   # Price anomaly z-score threshold

    # Metric 3
    "stuck_days": 5,           # Days since init before flagging as stuck (FedEx only)

    # Shared
    "baseline_days": 30,       # Historical baseline period
}

# ── Risk Scoring Thresholds (confirm with risk team) ────────
RISK_THRESHOLDS = {
    # Metric 1
    "untracked_rate_high": 0.3,     # 30% untracked = high risk
    "untracked_diff_high": 0.2,     # 20% spike vs baseline = high risk

    # Metric 2
    "zscore_high": 2.0,             # Avg price z-score threshold
    "max_zscore_high": 5.0,         # Max price z-score threshold

    # Metric 3A
    "pickup_lag_diff_high": 2.0,    # 2+ days spike vs baseline = high risk

    # Metric 3B
    # FedEx baseline = 0, any stuck order = HIGH risk (handled in SQL)
}