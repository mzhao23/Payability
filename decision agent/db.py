from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any, Callable

from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()


AGENT_CONFIG = {
    "json_report": {
        "table": "json_risk_report",
        "supplier_key_fields": ["supplier_key"],
        "score_fields": ["overall_risk_score", "risk_score", "score", "final_score"],
        "reason_fields": ["trigger_reason", "reason", "reasons", "summary_reason"],
        "threshold": lambda s: s is not None and s >= 8,
        "threshold_op": "gte",
        "threshold_value": 8,
    },
    "health_report": {
        "table": "health_daily_risk",
        "supplier_key_fields": ["supplier_key"],
        "date_fields": ["report_date"],
        "score_fields": ["risk_score"],
        "reason_fields": ["risk_reason"],
        # Rows with null/empty supplier_key are former customers; never use them.
        "require_supplier_key_present": True,
        "threshold": lambda s: s is not None and s > 6,
        "threshold_op": "gt",
        "threshold_value": 6,
    },
    "daily_summary_report": {
        "table": "daily_summary_report_flagged_suppliers",
        "supplier_key_fields": ["supplier_key"],
        "date_fields": ["created_at"],
        "score_fields": ["overall_risk_score", "risk_score", "score", "final_score"],
        "reason_fields": ["reasons", "trigger_reason", "reason", "summary_reason"],
        "threshold": lambda s: s is not None and s >= 3,
        "threshold_op": "gte",
        "threshold_value": 3,
    },
    "ship_tracking": {
        "table": "ship_risk_scores",
        "supplier_key_fields": ["supplier_key"],
        "score_fields": ["overall_risk_score", "risk_score", "score", "final_score"],
        "reason_fields": ["trigger_reason", "reason", "reasons", "summary_reason"],
        "threshold": lambda s: s is not None and s >= 6,
        "threshold_op": "gte",
        "threshold_value": 6,
    },
}

DATE_FIELD_CANDIDATES = ["report_date", "date", "run_date", "created_at"]

# Fill default fallback fields for agents that are not fixed-schema.
for _cfg in AGENT_CONFIG.values():
    if not _cfg.get("supplier_key_fields"):
        _cfg["supplier_key_fields"] = ["supplier_key"]
    if not _cfg.get("date_fields"):
        _cfg["date_fields"] = DATE_FIELD_CANDIDATES


def _get_supabase_client() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError(
            "Missing SUPABASE_URL or SUPABASE_SERVICE_KEY/SUPABASE_SERVICE_ROLE_KEY in env."
        )
    return create_client(url, key)


def _safe_score(row: dict[str, Any] | None) -> float | None:
    if not row:
        return None
    raw = row.get("overall_risk_score")
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _safe_score_from_field(row: dict[str, Any] | None, field: str) -> float | None:
    if not row:
        return None
    raw = row.get(field)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _normalize_date_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if "T" in text:
        return text.split("T", 1)[0]
    return text


def _is_missing_column_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "42703" in msg or "does not exist" in msg


def _apply_single_day_filter(query: Any, field: str, report_date: date) -> Any:
    day = report_date.isoformat()
    if field == "created_at":
        start = f"{day}T00:00:00Z"
        end = f"{day}T23:59:59Z"
        return query.gte(field, start).lte(field, end)
    return query.eq(field, day)


def _apply_history_filter(query: Any, field: str, report_date: date) -> Any:
    start_day = (report_date - timedelta(days=7)).isoformat()
    end_day = (report_date - timedelta(days=1)).isoformat()
    if field == "created_at":
        start = f"{start_day}T00:00:00Z"
        end = f"{end_day}T23:59:59Z"
        return query.gte(field, start).lte(field, end)
    return query.gte(field, start_day).lte(field, end_day)


def _apply_supplier_key_present_filter(query: Any, cfg: dict[str, Any]) -> Any:
    if not cfg.get("require_supplier_key_present"):
        return query
    return query.not_.is_("supplier_key", "null").neq("supplier_key", "")


def _select_today_rows_with_fallback_fields(
    client: Client, table: str, supplier_key: str, report_date: date, cfg: dict[str, Any]
) -> tuple[list[dict[str, Any]], str, str]:
    last_exc: Exception | None = None
    for score_field in cfg["score_fields"]:
        for reason_field in cfg["reason_fields"]:
            for key_field in cfg["supplier_key_fields"]:
                for date_field in cfg["date_fields"]:
                    try:
                        fields = f"{score_field},{reason_field},{key_field}"
                        base = _apply_supplier_key_present_filter(
                            client.table(table).select(fields).eq(key_field, supplier_key),
                            cfg,
                        )
                        rows = (
                            _apply_single_day_filter(base, date_field, report_date)
                            .limit(1)
                            .execute()
                            .data
                            or []
                        )
                        return rows, score_field, reason_field
                    except Exception as exc:  # noqa: BLE001
                        if _is_missing_column_error(exc):
                            last_exc = exc
                            continue
                        raise
    raise RuntimeError(
        f"Could not find compatible score/reason/date fields in table {table}. "
        f"Tried date={cfg['date_fields']}, key={cfg['supplier_key_fields']}, "
        f"score={cfg['score_fields']}, reason={cfg['reason_fields']}"
    ) from last_exc


def _select_history_rows_with_fallback_fields(
    client: Client, table: str, supplier_key: str, report_date: date, cfg: dict[str, Any]
) -> tuple[list[dict[str, Any]], str, str]:
    last_exc: Exception | None = None
    for score_field in cfg["score_fields"]:
        for reason_field in cfg["reason_fields"]:
            for key_field in cfg["supplier_key_fields"]:
                for date_field in cfg["date_fields"]:
                    try:
                        fields = f"{date_field},{score_field},{reason_field}"
                        base = _apply_supplier_key_present_filter(
                            client.table(table).select(fields).eq(key_field, supplier_key),
                            cfg,
                        )
                        query = _apply_history_filter(base, date_field, report_date).order(
                            date_field, desc=False
                        )
                        if cfg["threshold_op"] == "gt":
                            query = query.gt(score_field, cfg["threshold_value"])
                        else:
                            query = query.gte(score_field, cfg["threshold_value"])
                        rows = query.execute().data or []
                        return rows, date_field, score_field, reason_field
                    except Exception as exc:  # noqa: BLE001
                        if _is_missing_column_error(exc):
                            last_exc = exc
                            continue
                        raise
    raise RuntimeError(
        f"Could not find compatible score/reason/date fields in table {table}. "
        f"Tried date={cfg['date_fields']}, key={cfg['supplier_key_fields']}, "
        f"score={cfg['score_fields']}, reason={cfg['reason_fields']}"
    ) from last_exc


def get_flagged_suppliers_today(report_date: date) -> list[dict[str, str]]:
    client = _get_supabase_client()
    rows: list[dict[str, Any]] = []
    last_exc: Exception | None = None
    for field in DATE_FIELD_CANDIDATES:
        try:
            rows = (
                _apply_single_day_filter(
                    client.table("consolidated_flagged_supplier_list").select(
                        "supplier_key,supplier_name,source"
                    ),
                    field,
                    report_date,
                )
                .execute()
                .data
                or []
            )
            last_exc = None
            break
        except Exception as exc:  # noqa: BLE001
            if _is_missing_column_error(exc):
                last_exc = exc
                continue
            raise
    if last_exc is not None:
        raise RuntimeError(
            "Could not find a valid date field in consolidated_flagged_supplier_list. "
            f"Tried: {DATE_FIELD_CANDIDATES}"
        ) from last_exc

    dedup: dict[str, dict[str, str]] = {}
    for row in rows:
        supplier_key = row.get("supplier_key")
        if not supplier_key:
            continue
        existing = dedup.get(supplier_key)
        if existing is None:
            dedup[supplier_key] = {
                "supplier_key": supplier_key,
                "supplier_name": row.get("supplier_name") or "",
            }
        elif not existing.get("supplier_name") and row.get("supplier_name"):
            existing["supplier_name"] = row["supplier_name"]
    return list(dedup.values())


def get_today_scores(supplier_key: str, report_date: date) -> dict[str, dict[str, Any]]:
    client = _get_supabase_client()
    result: dict[str, dict[str, Any]] = {}

    for agent_key, cfg in AGENT_CONFIG.items():
        rows, score_field, reason_field = _select_today_rows_with_fallback_fields(
            client=client,
            table=cfg["table"],
            supplier_key=supplier_key,
            report_date=report_date,
            cfg=cfg,
        )

        row = rows[0] if rows else None
        score = _safe_score_from_field(row, score_field)
        reason = row.get(reason_field) if row else None
        if reason is not None:
            reason = str(reason)
        threshold_fn: Callable[[float | None], bool] = cfg["threshold"]
        result[agent_key] = {
            "score": score,
            "flagged": threshold_fn(score),
            "reason": reason,
        }

    return result


def get_history_7d(supplier_key: str, report_date: date) -> dict[str, list[dict[str, Any]]]:
    client = _get_supabase_client()
    history: dict[str, list[dict[str, Any]]] = {}

    for agent_key, cfg in AGENT_CONFIG.items():
        rows, selected_date_field, score_field, reason_field = _select_history_rows_with_fallback_fields(
            client=client,
            table=cfg["table"],
            supplier_key=supplier_key,
            report_date=report_date,
            cfg=cfg,
        )

        normalized: list[dict[str, Any]] = []
        for row in rows:
            normalized.append(
                {
                    "date": _normalize_date_value(row.get(selected_date_field)),
                    "score": _safe_score_from_field(row, score_field),
                    "flagged": True,
                    "reason": row.get(reason_field),
                }
            )
        history[agent_key] = normalized

    return history


def upsert_decision_report(
    supplier_key: str,
    supplier_name: str,
    report_date: date,
    final_score: int,
    reason: str,
    today_scores: dict[str, Any],
    history_7d: dict[str, Any],
    resonance_count: int,
) -> None:
    client = _get_supabase_client()
    payload = {
        "supplier_key": supplier_key,
        "supplier_name": supplier_name,
        "report_date": report_date.isoformat(),
        "final_score": final_score,
        "agent_scores": today_scores,
        "history_summary": history_7d,
        "resonance_count": resonance_count,
        "reason": reason,
    }
    (
        client.table("decision_agent_daily_report")
        .upsert(payload, on_conflict="supplier_key,report_date")
        .execute()
    )
