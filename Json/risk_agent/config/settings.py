"""config/settings.py — central settings loaded from environment / .env file."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root (one level up from this file)
load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Required environment variable '{key}' is not set.")
    return val


# ── Anthropic ──────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY: str = _require("ANTHROPIC_API_KEY")
ANTHROPIC_MODEL: str = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")

# ── BigQuery ───────────────────────────────────────────────────────────────────
BQ_SERVICE_ACCOUNT_PATH: str | None = os.getenv("BQ_SERVICE_ACCOUNT_PATH")
BQ_SERVICE_ACCOUNT_JSON: str | None = os.getenv("BQ_SERVICE_ACCOUNT_JSON")
BQ_PROJECT_ID: str = _require("BQ_PROJECT_ID")
BQ_DATASET: str = _require("BQ_DATASET")
BQ_TABLE: str = _require("BQ_TABLE")
BQ_LOOKBACK_HOURS: int = int(os.getenv("BQ_LOOKBACK_HOURS", "24"))

# ── Supabase ───────────────────────────────────────────────────────────────────
SUPABASE_URL: str = _require("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY: str = _require("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_RISK_TABLE: str = os.getenv("SUPABASE_RISK_TABLE", "supplier_risk_reports")

# ── Agent ──────────────────────────────────────────────────────────────────────
MAX_ROWS: int = int(os.getenv("MAX_ROWS", "0"))
DRY_RUN: bool = os.getenv("DRY_RUN", "false").lower() == "true"
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
# Number of parallel workers for LLM + Supabase calls (5 is a safe default)
PIPELINE_WORKERS: int = int(os.getenv("PIPELINE_WORKERS", "3"))