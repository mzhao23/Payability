import json
import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo


def _today_et():
    return datetime.now(ZoneInfo("America/New_York")).date()

from openai import OpenAI

from config.settings import GEMINI_API_KEY, GATEWAY_MODEL, GATEWAY_URL

logger = logging.getLogger(__name__)

SYSTEM_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "../prompts/llm_risk_scorer.md")
MAX_WORKERS = 30

# Carriers that have untracked_rate data in metric 1
# FEDEX_UNACTIVATED excluded for now — under engineering investigation
TRACKED_CARRIERS = {"FEDEX", "UPS", "USPS"}

# Minimum orders required for any carrier's untracked_rate to be a meaningful signal.
# Below this threshold the rate is too volatile to act on.
MIN_ORDERS_FOR_SIGNAL = 10

# Minimum orders required for price escalation zscore to be meaningful.
MIN_ORDERS_FOR_PRICE = 5


def _compute_untracked_score(rows: list[dict]) -> float:
    """Compute untracked score: sqrt(sum of squares) of per-carrier scores, capped at 5.
    Per-carrier score = rate × exp(n/50)/exp(3) × 10. Carriers with < 10 orders excluded.
    """
    carrier_scores = []
    for carrier in TRACKED_CARRIERS:
        latest = next(
            (r for r in sorted(
                [r for r in rows if r.get("carrier") == carrier and r.get("m1_untracked_rate") is not None],
                key=lambda r: str(r.get("last_purchase_date") or r.get("run_date") or ""),
                reverse=True,
            )),
            None,
        )
        if latest is None:
            continue
        volume = latest.get("m1_total_orders") or 0
        if volume < MIN_ORDERS_FOR_SIGNAL:
            continue
        rate = latest["m1_untracked_rate"]
        confidence = min(0.5, math.exp(volume / 50) / math.exp(3))
        carrier_scores.append(rate * confidence * 30)

    if not carrier_scores:
        return 0.0
    total = math.sqrt(sum(s ** 2 for s in carrier_scores))
    return round(min(8.0, total), 3)


def _load_system_prompt() -> str:
    with open(SYSTEM_PROMPT_PATH, "r") as f:
        return f.read()



def _build_supplier_context(supplier_key: str, rows: list[dict], carrier_baseline: dict) -> dict:
    """Build the metrics context dict for a supplier from their latest Supabase row."""
    evaluation_date = _today_et().isoformat()
    metrics = {}

    # ── Price escalation (carrier = 'ALL') ───────────────────
    price_rows = sorted(
        [r for r in rows if r.get("carrier") == "ALL" and r.get("m2_zscore") is not None],
        key=lambda r: str(r.get("last_purchase_date") or r.get("run_date") or ""),
    )
    if price_rows:
        latest = price_rows[-1]
        price_data = {}

        if latest.get("m2_total_orders") is not None:
            price_data["order_count_today"] = latest["m2_total_orders"]
        if latest.get("m2_zscore") is not None:
            price_data["latest_zscore"] = latest["m2_zscore"]
        if latest.get("m2_max_zscore") is not None:
            price_data["latest_max_zscore"] = latest["m2_max_zscore"]
        if latest.get("m2_avg_of_avg") is not None:
            price_data["rolling_avg_30d_value"] = latest["m2_avg_of_avg"]

        if price_data:
            metrics["price_escalation"] = price_data

    # ── Untracked rate (per carrier) ─────────────────────────
    untracked = {}
    for carrier in TRACKED_CARRIERS:
        carrier_rows = sorted(
            [r for r in rows if r.get("carrier") == carrier and r.get("m1_untracked_rate") is not None],
            key=lambda r: str(r.get("last_purchase_date") or r.get("run_date") or ""),
        )
        if not carrier_rows:
            continue

        latest = carrier_rows[-1]
        if (latest.get("m1_total_orders") or 0) < MIN_ORDERS_FOR_SIGNAL:
            continue

        carrier_data = {
            "latest_rate": latest["m1_untracked_rate"],
        }

        if latest.get("m1_rolling_avg_30d") is not None:
            carrier_data["rolling_avg_30d_rate"] = latest["m1_rolling_avg_30d"]
        if latest.get("m1_diff") is not None:
            carrier_data["diff_vs_baseline"] = latest["m1_diff"]
        if latest.get("m1_total_orders") is not None:
            carrier_data["order_volume_today"] = latest["m1_total_orders"]
        if latest.get("m1_order_volume_7d") is not None:
            carrier_data["order_volume_7d"] = latest["m1_order_volume_7d"]
        if latest.get("m1_order_volume_7d_change_rate") is not None:
            carrier_data["order_volume_7d_change_rate"] = latest["m1_order_volume_7d_change_rate"]

        untracked[carrier] = carrier_data

    if untracked:
        metrics["untracked_rate"] = untracked

        # Add carrier-level baseline for same target date
        carrier_baseline_context = {}
        for carrier in TRACKED_CARRIERS:
            b = carrier_baseline.get(carrier)
            if b and b.get("untracked_rate") is not None:
                carrier_baseline_context[carrier] = {
                    "carrier_untracked_rate": b["untracked_rate"],
                    "carrier_rolling_avg_30d": b.get("rolling_avg_30d"),
                }
        if carrier_baseline_context:
            metrics["carrier_baseline"] = carrier_baseline_context

        untracked_score = _compute_untracked_score(rows)
        metrics["untracked_score"] = untracked_score

        if untracked_score >= 3:
            price_weight = 1.0
        elif untracked_score >= 1.5:
            price_weight = 0.6
        else:
            price_weight = 0.3
        metrics["price_weight"] = price_weight

    # ── FedEx pickup lag (carrier = 'ALL') ───────────────────
    lag_rows = sorted(
        [r for r in rows if r.get("carrier") == "ALL" and r.get("m3a_avg_pickup_lag") is not None],
        key=lambda r: str(r.get("last_purchase_date") or r.get("run_date") or ""),
    )
    if lag_rows:
        latest = lag_rows[-1]
        lag_data = {}

        if latest.get("m3a_avg_pickup_lag") is not None:
            lag_data["latest_avg_lag"] = latest["m3a_avg_pickup_lag"]
        if latest.get("m3a_rolling_avg_30d") is not None:
            lag_data["rolling_avg_30d_lag"] = latest["m3a_rolling_avg_30d"]
        if latest.get("m3a_diff") is not None:
            lag_data["diff_vs_baseline"] = latest["m3a_diff"]

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
        key=lambda r: str(r.get("last_purchase_date") or r.get("run_date") or ""),
    )
    latest_all = all_rows[-1] if all_rows else {}

    latest_by_carrier = {}
    for carrier in TRACKED_CARRIERS:
        c_rows = sorted(
            [r for r in rows if r.get("carrier") == carrier and r.get("m1_untracked_rate") is not None],
            key=lambda r: str(r.get("last_purchase_date") or r.get("run_date") or ""),
        )
        if c_rows:
            latest_by_carrier[carrier] = c_rows[-1]

    metrics = []

    def _add(metric_id, value, unit):
        if value is not None:
            metrics.append({"metric_id": metric_id, "value": value, "unit": unit})

    _add("avg_price_zscore", latest_all.get("m2_zscore"), "standard_deviations")
    _add("max_order_price_zscore", latest_all.get("m2_max_zscore"), "standard_deviations")

    for carrier, row in latest_by_carrier.items():
        _add(f"untracked_rate_{carrier.lower()}", row.get("m1_untracked_rate"), "rate")
        _add(f"order_count_{carrier.lower()}", row.get("m1_total_orders"), "orders")

    _add("fedex_pickup_lag_days", latest_all.get("m3a_avg_pickup_lag"), "days")
    _add("fedex_pickup_lag_vs_baseline", latest_all.get("m3a_diff"), "days")

    last_purchase_date = max(
        (str(r["last_purchase_date"]) for r in rows if r.get("last_purchase_date")),
        default=None,
    )

    return {
        "table_name": "ship_tracking",
        "supplier_key": supplier_key,
        "supplier_name": None,
        "report_date": _today_et().isoformat(),
        "last_purchase_date": str(last_purchase_date) if last_purchase_date else None,
        "metrics": metrics,
        "trigger_reason": llm_result.get("trigger_reason"),
        "overall_risk_score": round(llm_result.get("overall_risk_score", 0)),
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


def _has_sufficient_volume(rows: list[dict]) -> bool:
    """
    Returns True if at least one carrier has enough orders today to produce
    a reliable untracked_rate signal, OR if there is price escalation data.
    Suppliers below the volume threshold are skipped — their metrics are too
    volatile to score meaningfully.
    """
    # Price escalation: only meaningful if there are enough orders today
    price_row = next(
        (r for r in sorted(rows, key=lambda r: str(r.get("last_purchase_date") or r.get("run_date") or ""), reverse=True)
         if r.get("carrier") == "ALL" and r.get("m2_zscore") is not None
         and (r.get("m2_total_orders") or 0) >= MIN_ORDERS_FOR_PRICE),
        None,
    )
    if price_row:
        return True

    # Check if any tracked carrier clears the order volume threshold today
    for carrier in TRACKED_CARRIERS:
        latest = next(
            (r for r in sorted(rows, key=lambda r: str(r.get("last_purchase_date") or r.get("run_date") or ""), reverse=True)
             if r.get("carrier") == carrier and r.get("m1_total_orders") is not None),
            None,
        )
        if latest and latest["m1_total_orders"] >= MIN_ORDERS_FOR_SIGNAL:
            return True

    return False


def _score_supplier(
    supplier_key: str,
    rows: list[dict],
    client: OpenAI,
    system_prompt: str,
    carrier_baseline: dict,
) -> dict | None:
    if not _has_sufficient_volume(rows):
        logger.debug(f"  Skipping {supplier_key}: insufficient order volume")
        return _build_output_row(
            supplier_key,
            rows,
            {
                "overall_risk_score": 0,
                "trigger_reason": "Insufficient order volume across all carriers for a reliable signal. Score defaulted to 0.",
            },
        )

    context = _build_supplier_context(supplier_key, rows, carrier_baseline)
    llm_result = _call_llm(client, system_prompt, context)
    if llm_result is None:
        return None
    return _build_output_row(supplier_key, rows, llm_result)


def run(grouped: dict[str, list[dict]], carrier_baseline: dict) -> list[dict]:
    """
    Score each supplier with the LLM in parallel using today's BQ rows.
    Returns list of rows ready for supplier_risk_scores upsert.
    """
    total = len(grouped)
    logger.info(f"[LLM Scorer] Scoring {total} suppliers with {MAX_WORKERS} workers...")

    system_prompt = _load_system_prompt()
    client = OpenAI(api_key=GEMINI_API_KEY, base_url=GATEWAY_URL)

    results = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_score_supplier, supplier_key, rows, client, system_prompt, carrier_baseline): supplier_key
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