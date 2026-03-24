from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Mapping, MutableMapping, Tuple

from health_risk.config import Settings

_SYSTEM = """\
You are a marketplace health analyst. The seller has been flagged by our rule-based \
scoring engine. The numeric risk score is final — do NOT change or question it.

Your job: compare every metric below against its Amazon health threshold, identify \
which ones are UNHEALTHY, and explain why.

━━━ METRIC THRESHOLDS (values are in ratio form, e.g. 0.01 = 1%) ━━━

OUTCOME METRICS:
  Order Defect Rate (60d)     — Healthy: <0.5%  | Low: 0.5–1%  | Moderate: 1–2%  | High: 2–3%  | Critical: >3%
  Chargeback Rate (90d)       — Healthy: <0.1%  | Low: 0.1–0.2% | Moderate: 0.2–0.5% | Critical: >0.5%
  A-to-Z Claim Rate (90d)    — Healthy: <0.1%  | Low: 0.1–0.3% | Moderate: 0.3–0.8% | Critical: >0.8%
  Negative Feedback Rate (90d)— Healthy: <0.5%  | Low: 0.5–1%  | Moderate: 1–3%  | Critical: >3%

OPERATIONAL METRICS:
  Late Shipment Rate (30d)    — Healthy: <2%   | Low: 2–4%  | Moderate: 4–8%  | Critical: >8%
  Pre-Fulfillment Cancel (30d)— Healthy: <1%   | Low: 1–3%  | Moderate: 3–5%  | Critical: >5%
  Avg Response Hours (30d)    — Healthy: <12h  | Low: 12–24h | Moderate: 24–48h | Critical: >48h
  No Response >24h Count (30d)— Healthy: 0     | Low: 1–4    | Moderate: 5–19  | Critical: ≥20
  Valid Tracking Rate (30d)   — Healthy: ≥97%  | Low: 94–97% | Moderate: 90–94% | Critical: <90%
  On-Time Delivery Rate (30d) — Healthy: ≥95%  | Low: 90–95% | Moderate: 85–90% | Critical: <85%

COMPLIANCE STATUS FIELDS (text values):
  Product Safety / Product Authenticity / Policy Violation / Listing Policy / Intellectual Property
  — "Good" = Healthy | "Fair"/"Warning"/"Watch" = Moderate | anything else (e.g. "Bad","At Risk") = Critical

━━━ OUTPUT FORMAT ━━━

1. List ONLY the unhealthy metrics (skip any that are Healthy or null/missing).
   For each one write ONE line:
     • <Metric Name>: <actual value> — <band: Low/Moderate/High/Critical> — <1-sentence business impact>

2. After the list, write a short overall summary (2–3 sentences) of the seller's \
   health situation and the combined risk picture.

Do NOT mention metrics that are Healthy. Do NOT invent values for null/missing fields — just skip them.
Use plain business English, no markdown headings."""

_RAW_NUMERIC_KEYS = (
    "orderWithDefects_60_rate",
    "chargebacks_90_rate",
    "a_z_claims_90_rate",
    "negativeFeedbacks_90_rate",
    "lateShipment_30_rate",
    "preFulfillmentCancellation_30_rate",
    "averageResponseTimeInHours_30",
    "noResponseForContactsOlderThan24Hours_30",
    "validTracking_rate_30",
    "onTimeDelivery_rate_30",
    "orders_count_60",
)

_RAW_STATUS_KEYS = (
    "productSafetyStatus_status",
    "productAuthenticityStatus_status",
    "policyViolation_status",
    "listingPolicyStatus_status",
    "intellectualProperty_status",
)


def _health_snapshot(raw: Mapping[str, Any] | None) -> Dict[str, Any]:
    if not raw:
        return {}
    snap: Dict[str, Any] = {}
    for k in _RAW_NUMERIC_KEYS:
        if k in raw:
            snap[k] = raw[k]
    for k in _RAW_STATUS_KEYS:
        if k in raw:
            snap[k] = raw[k]
    return snap


def _build_user_message(scored: Mapping[str, Any], raw: Mapping[str, Any] | None) -> str:
    payload = {
        "supplier": {
            "mp_sup_key": scored.get("mp_sup_key"),
            "supplier_key": scored.get("supplier_key"),
            "supplier_name": scored.get("supplier_name"),
            "payability_status": scored.get("payability_status"),
            "report_date": scored.get("report_date"),
        },
        "rule_engine_output": {
            "risk_score": scored.get("risk_score"),
            "risk_level": scored.get("risk_level"),
            "risk_reason": scored.get("risk_reason"),
            "top_risk_drivers": scored.get("top_risk_drivers"),
            "driver_1": scored.get("driver_1"),
            "driver_2": scored.get("driver_2"),
            "driver_3": scored.get("driver_3"),
            "red_metric_count": scored.get("red_metric_count"),
            "yellow_metric_count": scored.get("yellow_metric_count"),
            "pipeline_version": scored.get("pipeline_version"),
        },
        "health_metrics_snapshot": _health_snapshot(raw),
    }
    return (
        "Write the high-risk explanation narrative for this seller.\n\n"
        f"DATA:\n{json.dumps(payload, separators=(',', ':'), default=str)}"
    )


def _call_openai(settings: Settings, user_message: str) -> str:
    from openai import OpenAI

    client = OpenAI(api_key=settings.openai_api_key)
    resp = client.chat.completions.create(
        model=settings.openai_model,
        max_tokens=1200,
        messages=[
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": user_message},
        ],
    )
    text = resp.choices[0].message.content
    if not text:
        raise RuntimeError("OpenAI returned empty content")
    return text.strip()


def enrich_high_risk_narratives(
    payload: List[MutableMapping[str, Any]],
    settings: Settings,
    raw_index: Mapping[Tuple[str | None, str], Mapping[str, Any]],
    *,
    force_disable: bool = False,
) -> int:
    """
    For rows with risk_score above threshold, set `high_risk_narrative_llm` via OpenAI.
    Mutates payload dicts in place. Returns number of successful LLM calls.
    """
    for row in payload:
        row.setdefault("high_risk_narrative_llm", None)
        row.setdefault("high_risk_narrative_error", None)

    if (
        force_disable
        or not settings.llm_high_risk_narrative_enabled
        or not settings.openai_api_key
    ):
        return 0

    threshold = settings.high_risk_narrative_threshold
    tasks: List[Tuple[MutableMapping[str, Any], str]] = []
    for row in payload:
        try:
            score = float(row.get("risk_score", 0))
        except (TypeError, ValueError):
            continue
        if score <= threshold:
            continue
        key = (row.get("report_date"), str(row.get("mp_sup_key", "")))
        raw = raw_index.get(key)
        user_msg = _build_user_message(row, raw)
        tasks.append((row, user_msg))

    if not tasks:
        return 0

    n_ok = 0
    workers = min(settings.llm_narrative_max_workers, len(tasks))

    def job(
        item: Tuple[MutableMapping[str, Any], str],
    ) -> Tuple[MutableMapping[str, Any], bool, str]:
        row, user_msg = item
        try:
            text = _call_openai(settings, user_msg)
            return row, True, text
        except Exception as exc:
            return row, False, str(exc)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(job, t) for t in tasks]
        for fut in as_completed(futures):
            row, ok, text = fut.result()
            if ok:
                row["high_risk_narrative_llm"] = text
                row["high_risk_narrative_error"] = None
                n_ok += 1
            else:
                row["high_risk_narrative_llm"] = None
                row["high_risk_narrative_error"] = text

    return n_ok


def strip_llm_narrative_for_supabase(row: Mapping[str, Any]) -> Dict[str, Any]:
    """Drop LLM-only keys for health_daily_risk upsert when the table has no such columns."""
    return {k: v for k, v in row.items() if k not in ("high_risk_narrative_llm", "high_risk_narrative_error")}
