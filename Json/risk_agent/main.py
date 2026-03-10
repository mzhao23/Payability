"""main.py — end-to-end pipeline entry point with concurrent LLM calls.

Usage:
    python main.py

The pipeline:
1. Fetch ALL rows from BigQuery into memory
2. Process each row concurrently using a ThreadPoolExecutor:
   a. Extract features from the `data` JSON column + structured BQ columns
   b. Run rule-based pre-scorer
   c. Send to Claude AI for final risk analysis (skipped for error/clean rows)
   d. Write RiskReport to Supabase
3. Log a summary with timing and cost estimate

Concurrency is controlled by PIPELINE_WORKERS in .env (default: 5).
Raising it above 10 risks hitting Anthropic rate limits.
"""

from __future__ import annotations

import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from config import settings
from extractors.bq_loader import fetch_rows
from extractors.feature_extractor import extract_features
from scoring.rule_scorer import score as rule_score
from agent.claude_agent import analyse
from output.supabase_writer import upsert_report
from config.models import RiskReport
from utils.logger import get_logger

log = get_logger("pipeline")

TABLE_NAME = f"{settings.BQ_PROJECT_ID}.{settings.BQ_DATASET}.{settings.BQ_TABLE}"

_lock = threading.Lock()


def _process_row(row: dict) -> tuple[str, str, RiskReport | None]:
    """
    Process a single BQ row end-to-end.
    Returns (supplier_key, status, report_or_None)
    status is one of: "ok", "error"
    """
    supplier_key = row.get("mp_sup_key", "UNKNOWN")
    try:
        # Step 1: Feature extraction
        fs = extract_features(row)

        # Step 2: Rule-based pre-scoring
        pre = rule_score(fs)

        # Step 3: AI analysis (may be skipped internally based on flags)
        report = analyse(fs, pre, TABLE_NAME)
        report.mp_sup_key = row.get("mp_sup_key")

        # Step 4: Write to Supabase
        upsert_report(report)

        # Mirror the same logic used in claude_agent.py
        _LLM_SCORE_THRESHOLD = 5
        _NO_RISK_MSG = "No significant risk indicators detected by rule engine."
        # Hard rules always contain "(threshold N)" or "past due" or "DATA_QUALITY"
        has_hard = any(
            "threshold" in r or "past due" in r.lower() or "DATA_QUALITY" in r
            for r in pre.triggered_rules
        )
        used_llm = (
            fs.data_quality_flag not in {
                "login_error","not_authorized","wrong_password",
                "bank_page_error","internal_error","json_parse_error",
                "advance_only","onboarding_only",
            }
            and (has_hard or pre.preliminary_score >= _LLM_SCORE_THRESHOLD)
        )
        log.info(
            "[%s] %s — score=%d/10 | flag=%s | llm=%s",
            supplier_key[:8],
            (report.supplier_name or "?")[:30],
            report.overall_risk_score,
            fs.data_quality_flag,
            "yes" if used_llm else "no",
        )
        return supplier_key, "ok", report

    except Exception as exc:
        log.error("Error processing supplier_key=%s: %s", supplier_key, exc, exc_info=True)
        return supplier_key, "error", None


def run_pipeline() -> None:
    start = time.time()
    log.info("=" * 60)
    log.info(
        "Risk Analysis Pipeline started at %s",
        datetime.now(timezone.utc).isoformat(),
    )
    log.info(
        "Table: %s | Lookback: %dh | Workers: %d | DryRun: %s",
        TABLE_NAME,
        settings.BQ_LOOKBACK_HOURS,
        settings.PIPELINE_WORKERS,
        settings.DRY_RUN,
    )
    log.info("=" * 60)

    # Fetch all rows upfront — BQ streaming is not thread-safe
    log.info("Fetching rows from BigQuery ...")
    rows = list(fetch_rows())
    log.info("Fetched %d rows. Starting concurrent processing ...", len(rows))

    processed = 0
    errors = 0
    scores: list[int] = []

    with ThreadPoolExecutor(max_workers=settings.PIPELINE_WORKERS) as executor:
        futures = {executor.submit(_process_row, row): row for row in rows}

        for future in as_completed(futures):
            supplier_key, status, report = future.result()
            if status == "ok":
                processed += 1
                if report:
                    scores.append(report.overall_risk_score)
            else:
                errors += 1

    elapsed = time.time() - start

    log.info("=" * 60)
    log.info(
        "Pipeline finished in %.1fs (%.1f min) — processed=%d, errors=%d",
        elapsed, elapsed / 60, processed, errors,
    )
    if scores:
        avg_score = sum(scores) / len(scores)
        high_risk = sum(1 for s in scores if s >= 7)
        log.info(
            "Score summary — avg=%.1f | high-risk(>=7): %d/%d (%.0f%%)",
            avg_score, high_risk, len(scores), high_risk / len(scores) * 100,
        )
    log.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()