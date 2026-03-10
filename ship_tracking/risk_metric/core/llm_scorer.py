import json
import logging
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

from openai import OpenAI

from config.settings import GEMINI_API_KEY, GATEWAY_MODEL, GATEWAY_URL
from core.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

SYSTEM_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "../prompts/llm_risk_scorer.md")
MAX_WORKERS = 30

# Carriers that have untracked_rate data in metric 1
TRACKED_CARRIERS = {"FEDEX", "FEDEX_UNACTIVATED", "UPS", "USPS"}


def _load_system_prompt() -> str:
    with open(SYSTEM_PROMPT_PATH, "r") as f:
        return f.read()


def _fetch_recent_metrics(sb: SupabaseClient) -> dict[str, list[dict]]:
    """Fetch last 7 days of supplier_daily_metrics from Supabase, grouped by supplier_key."""
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    response = (
        sb.client.table("supplier_daily_metrics")
        .select("*")
        .gte("run_date", cutoff)
        .order("run_date", desc=False)
        .execute()
    )
    rows = response.data or []

    grouped = defaultdict(list)
    for row in rows:
        grouped[row["supplier_key"]].append(row)
    return dict(grouped)


def _build_supplier_context(supplier_key: str, rows: list[dict]) -> dict:
    """Build the metrics context dict for a supplier from their 7-day rows."""
    evaluation_date = date.today().isoformat()
    metrics = {}

    # ── Price escalation (carrier = 'ALL') ───────────────────
    price_rows = sorted(
        [r for r in rows if r.get("carrier") == "ALL" and r.get("m2_zscore") is not None],
        key=lambda r: r["run_date"],
    )
    if price_rows:
        latest = price_rows[-1]
        price_data = {}
        if latest.get("m2_zscore") is not None:
            price_data["latest_zscore"] = latest["m2_zscore"]
        if latest.get("m2_max_zscore") is not None:
            price_data["latest_max_zscore"] = latest["m2_max_zscore"]
        trend = [r["m2_zscore"] for r in price_rows if r.get("m2_zscore") is not None]
        if len(trend) > 1:
            price_data["trend_7d"] = trend
        if price_data:
            metrics["price_escalation"] = price_data

    # ── Untracked rate (per carrier) ─────────────────────────
    untracked = {}
    for carrier in TRACKED_CARRIERS:
        carrier_rows = sorted(
            [r for r in rows if r.get("carrier") == carrier and r.get("m1_untracked_rate") is not None],
            key=lambda r: r["run_date"],
        )
        if not carrier_rows:
            continue
        latest = carrier_rows[-1]
        carrier_data = {"latest_rate": latest["m1_untracked_rate"]}
        if latest.get("m1_diff") is not None:
            carrier_data["latest_diff"] = latest["m1_diff"]
        if latest.get("m1_total_orders") is not None:
            carrier_data["total_orders"] = latest["m1_total_orders"]
        trend = [r["m1_untracked_rate"] for r in carrier_rows if r.get("m1_untracked_rate") is not None]
        if len(trend) > 1:
            carrier_data["trend_7d"] = trend
        untracked[carrier] = carrier_data
    if untracked:
        metrics["untracked_rate"] = untracked

    # ── FedEx pickup lag (carrier = 'ALL') ───────────────────
    lag_rows = sorted(
        [r for r in rows if r.get("carrier") == "ALL" and r.get("m3a_avg_pickup_lag") is not None],
        key=lambda r: r["run_date"],
    )
    if lag_rows:
        latest = lag_rows[-1]
        lag_data = {}
        if latest.get("m3a_avg_pickup_lag") is not None:
            lag_data["latest_avg_lag"] = latest["m3a_avg_pickup_lag"]
        if latest.get("m3a_diff") is not None:
            lag_data["latest_diff"] = latest["m3a_diff"]
        trend = [r["m3a_avg_pickup_lag"] for r in lag_rows if r.get("m3a_avg_pickup_lag") is not None]
        if len(trend) > 1:
            lag_data["trend_7d"] = trend
        if lag_data:
            metrics["fedex_pickup_lag"] = lag_data

    return {
        "supplier_key": supplier_key,
        "evaluation_date": evaluation_date,
        "metrics": metrics,
    }


def _build_output_row(supplier_key: str, rows: list[dict], llm_result: dict) -> dict:
    """Build the supplier_risk_scores row from latest metric values + LLM output."""
    all_rows = sorted(
        [r for r in rows if r.get("carrier") == "ALL"],
        key=lambda r: r["run_date"],
    )
    latest_all = all_rows[-1] if all_rows else {}

    latest_by_carrier = {}
    for carrier in TRACKED_CARRIERS:
        c_rows = sorted(
            [r for r in rows if r.get("carrier") == carrier and r.get("m1_untracked_rate") is not None],
            key=lambda r: r["run_date"],
        )
        if c_rows:
            latest_by_carrier[carrier] = c_rows[-1]

    metrics = []

    def _add(metric_id, value, unit):
        if value is not None:
            metrics.append({"metric_id": metric_id, "value": value, "unit": unit})

    _add("m2_zscore", latest_all.get("m2_zscore"), "standard_deviations")
    _add("m2_max_zscore", latest_all.get("m2_max_zscore"), "standard_deviations")

    for carrier, row in latest_by_carrier.items():
        _add(f"m1_untracked_rate_{carrier.lower()}", row.get("m1_untracked_rate"), "rate")

    _add("m3a_avg_pickup_lag", latest_all.get("m3a_avg_pickup_lag"), "days")
    _add("m3a_diff", latest_all.get("m3a_diff"), "days")

    return {
        "table_name": "supplier_risk_scores",
        "supplier_key": supplier_key,
        "supplier_name": None,
        "report_date": date.today().isoformat(),
        "metrics": metrics,
        "trigger_reason": llm_result.get("trigger_reason"),
        "overall_risk_score": llm_result.get("overall_risk_score"),
    }


def _call_llm(client: OpenAI, system_prompt: str, supplier_context: dict) -> dict | None:
    """Call the LLM with retry on JSON parse failure. Returns parsed dict or None."""
    user_prompt = (
        "Evaluate the risk for this supplier based on the following 7-day metrics:\n"
        + json.dumps(supplier_context, indent=2)
    )

    for attempt in range(2):
        try:
            response = client.chat.completions.create(
                model=GATEWAY_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0,
            )
            content = response.choices[0].message.content.strip()
            # Strip markdown code blocks if present (e.g. ```json ... ```)
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
                content = content.strip()
            return json.loads(content)
        except json.JSONDecodeError:
            if attempt == 0:
                logger.warning(f"  JSON parse failed for {supplier_context['supplier_key']}, retrying...")
                continue
            logger.warning(f"  JSON parse failed twice for {supplier_context['supplier_key']}, skipping")
            return None
        except Exception as e:
            logger.error(f"  LLM call failed for {supplier_context['supplier_key']}: {e}")
            return None

    return None


def _score_supplier(
    supplier_key: str,
    rows: list[dict],
    client: OpenAI,
    system_prompt: str,
) -> dict | None:
    context = _build_supplier_context(supplier_key, rows)
    llm_result = _call_llm(client, system_prompt, context)
    if llm_result is None:
        return None
    return _build_output_row(supplier_key, rows, llm_result)


def run(sb: SupabaseClient) -> list[dict]:
    """
    Read supplier_daily_metrics, score each supplier with the LLM in parallel,
    and return list of rows ready for supplier_risk_scores upsert.
    """
    logger.info("[LLM Scorer] Fetching supplier_daily_metrics (last 7 days)...")
    grouped = _fetch_recent_metrics(sb)
    total = len(grouped)
    logger.info(f"[LLM Scorer] Scoring {total} suppliers with {MAX_WORKERS} workers...")

    system_prompt = _load_system_prompt()
    client = OpenAI(api_key=GEMINI_API_KEY, base_url=GATEWAY_URL)

    results = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_score_supplier, supplier_key, rows, client, system_prompt): supplier_key
            for supplier_key, rows in grouped.items()
        }

        for future in as_completed(futures):
            completed += 1
            result = future.result()
            if result is not None:
                results.append(result)
            if completed % 50 == 0:
                logger.info(f"[LLM Scorer] Scored {completed}/{total} suppliers...")

    logger.info(f"[LLM Scorer] Completed. {len(results)}/{total} suppliers scored.")
    return results
