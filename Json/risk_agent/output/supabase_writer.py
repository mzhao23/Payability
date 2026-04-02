"""output/supabase_writer.py

Writes RiskReport objects to Supabase via the REST API (supabase-py client).

Upsert strategy: supplier_key + report_date is treated as the logical unique
key. If a report already exists for that (key, date) pair, it is overwritten.
Make sure you create a UNIQUE constraint on those two columns in Supabase:

    ALTER TABLE supplier_risk_reports
    ADD CONSTRAINT uq_supplier_report_date UNIQUE (supplier_key, report_date);
"""

from __future__ import annotations

import time
from supabase import create_client, Client
from postgrest.exceptions import APIError
import httpx

from config import settings
from config.models import RiskReport
from utils.logger import get_logger

_RETRY_STATUS_CODES = {502, 503, 504}
_MAX_RETRIES = 3
_RETRY_DELAY = 5  # seconds

log = get_logger("supabase_writer")

_supabase: Client | None = None


def _get_client() -> Client:
    global _supabase
    if _supabase is None:
        _supabase = create_client(
            settings.SUPABASE_URL,
            settings.SUPABASE_SERVICE_ROLE_KEY,
        )
    return _supabase


def upsert_report(report: RiskReport) -> None:
    """
    Upsert a single RiskReport into the Supabase risk table.
    Raises on error so the caller can decide how to handle failures.
    """
    if settings.DRY_RUN:
        log.info(
            "[DRY_RUN] Would upsert report for supplier_key=%s score=%d",
            report.supplier_key,
            report.overall_risk_score,
        )
        return

    client = _get_client()
    data = report.to_supabase_dict()

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            response = (
                client.table(settings.SUPABASE_RISK_TABLE)
                .upsert(data, on_conflict="mp_sup_key,report_date")
                .execute()
            )
            if hasattr(response, "data") and response.data:
                log.debug(
                    "Upserted report for supplier_key=%s (score=%s)",
                    report.supplier_key,
                    report.overall_risk_score,
                )
            else:
                log.warning(
                    "Unexpected Supabase response for supplier_key=%s: %s",
                    report.supplier_key,
                    response,
                )
            return
        except (APIError, httpx.ReadError, httpx.ConnectError, httpx.TimeoutException) as exc:
            is_api_error = isinstance(exc, APIError)
            code = int(exc.code) if is_api_error and str(exc.code).isdigit() else 0
            retryable = (not is_api_error) or (code in _RETRY_STATUS_CODES)
            if retryable and attempt < _MAX_RETRIES:
                log.warning(
                    "Supabase network error for supplier_key=%s (attempt %d/%d) — retrying in %ds: %s",
                    report.supplier_key, attempt, _MAX_RETRIES, _RETRY_DELAY, exc,
                )
                time.sleep(_RETRY_DELAY)
                _supabase = None  # reset client to get fresh connection
            else:
                raise


def upsert_reports_bulk(reports: list[RiskReport]) -> tuple[int, int]:
    """
    Bulk upsert a list of reports.
    Returns (success_count, failure_count).
    """
    success = 0
    failure = 0
    for report in reports:
        try:
            upsert_report(report)
            success += 1
        except Exception as exc:
            log.error(
                "Failed to upsert report for supplier_key=%s: %s",
                report.supplier_key,
                exc,
            )
            failure += 1
    return success, failure