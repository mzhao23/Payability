"""
Daily Merchant Risk Pipeline (v3 - production-ready)

Data source:
- BigQuery: bigqueryexport-183608.amazon.customer_health_metrics

Outputs:
1) Supabase table: public.merchant_daily_risk_v2
   - PK: (report_date, mp_sup_key)
   - status columns (lowercase)
   - risk_score (int 0..10), risk_level (text), risk_reason (text)
   - drivers (json), metric_scores (json), raw_status (json), raw_total (int)

2) Unified JSON export file: risk_output.json
   - [
       {
         "table_name": "customer_health_metrics",
         "supplier_key": "...",
         "report_date": "YYYY-MM-DD",
         "metrics": [
           {"metric_id": "...", "value": "...", "unit": "...", "explanation": "..."}
         ],
         "overall_risk_score": 0.0~1.0
       }
     ]

Scoring:
- Raw points: Good=0, Fair=1, Bad=2  (max_raw = 2 * N_STATUS_COLS)
- Stored risk_score: normalized to 0..10 (integer)
"""

import os
import json
import time
import math
import argparse
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple, Optional

from google.cloud import bigquery
from supabase import create_client


# ----------------------------
# CONFIG
# ----------------------------
BQ_PROJECT = "bigqueryexport-183608"
BQ_DATASET = "amazon"
BQ_TABLE = "customer_health_metrics"
BQ_FULL_TABLE = f"`{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"

SUPABASE_TABLE = "merchant_daily_risk_v2"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")  # service role key

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment variables.")


# ----------------------------
# CLIENTS
# ----------------------------
bq = bigquery.Client(project=BQ_PROJECT)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ----------------------------
# STATUS COLUMNS
# BigQuery uses camelCase; Supabase table uses lowercase.
# ----------------------------
BQ_STATUS_COLS = [
    "contactResponseTime_status",
    "customerServiceDissatisfactionRate_status",
    "intellectualProperty_status",
    "lateShipmentRate_status",
    "listingPolicyStatus_status",
    "orderCancellationRate_status",
    "orderDefectRate_status",
    "policyViolation_status",
    "productAuthenticityStatus_status",
    "productSafetyStatus_status",
    "returnDissatisfactionRate_status",
]
SB_STATUS_COLS = [c.lower() for c in BQ_STATUS_COLS]

PTS = {"Good": 0, "Fair": 1, "Bad": 2}
MAX_RAW = 2 * len(SB_STATUS_COLS)


# ----------------------------
# UTIL
# ----------------------------
def iso(d: Any) -> str:
    return d.isoformat() if hasattr(d, "isoformat") else str(d)


def clamp_int(x: float, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(x)))


def backoff_sleep(attempt: int, base: float = 0.6, cap: float = 6.0) -> None:
    # exponential backoff with cap
    t = min(cap, base * (2 ** attempt))
    time.sleep(t)


# ----------------------------
# SCORING
# ----------------------------
def normalize_raw_to_0_10(raw_total: int) -> int:
    """
    Map raw_total in [0..MAX_RAW] to integer [0..10].
    Boss requirement: 0-10.
    """
    if MAX_RAW <= 0:
        return 0
    score = round((raw_total / MAX_RAW) * 10)  # integer 0..10
    return clamp_int(score, 0, 10)


def score_status_map(status_map: Dict[str, Any]) -> Tuple[int, int, str, List[str], Dict[str, int]]:
    """
    status_map keys are lowercase status columns.
    Returns:
      raw_total (0..MAX_RAW),
      risk_score_0_10 (0..10),
      risk_level,
      drivers,
      metric_scores (col->pts)
    """
    metric_scores: Dict[str, int] = {}
    drivers: List[str] = []
    raw_total = 0

    for col in SB_STATUS_COLS:
        v = status_map.get(col)
        pts = PTS.get(v, 0)  # None/unknown -> 0
        metric_scores[col] = pts
        raw_total += pts
        if pts > 0:
            drivers.append(col)

    risk_score_0_10 = normalize_raw_to_0_10(raw_total)

    # Thresholds based on 0..10 (tune as needed)
    if risk_score_0_10 <= 2:
        level = "Low"
    elif risk_score_0_10 <= 6:
        level = "Medium"
    else:
        level = "High"

    return raw_total, risk_score_0_10, level, drivers, metric_scores


# ----------------------------
# BIGQUERY
# ----------------------------
def get_latest_report_date() -> date:
    q = f"""
    SELECT MAX(DATE(snapshot_date)) AS report_date
    FROM {BQ_FULL_TABLE}
    """
    rows = list(bq.query(q).result())
    return rows[0]["report_date"]


def fetch_latest_per_merchant(report_date: date, limit: int = 2000) -> List[Dict[str, Any]]:
    """
    Fetch latest snapshot per merchant for the given report_date.
    Aliases *_status cols to lowercase to match Supabase schema.
    """
    status_select = ",\n        ".join([f"`{c}` AS `{c.lower()}`" for c in BQ_STATUS_COLS])

    q = f"""
    WITH latest_per_merchant AS (
      SELECT
        DATE(snapshot_date) AS report_date,
        mp_sup_key,
        {status_select},
        snapshot_date,
        ROW_NUMBER() OVER (
          PARTITION BY mp_sup_key, DATE(snapshot_date)
          ORDER BY snapshot_date DESC
        ) AS rn
      FROM {BQ_FULL_TABLE}
      WHERE DATE(snapshot_date) = @report_date
    )
    SELECT * EXCEPT(snapshot_date, rn)
    FROM latest_per_merchant
    WHERE rn = 1
    LIMIT @lim
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("report_date", "DATE", report_date),
            bigquery.ScalarQueryParameter("lim", "INT64", limit),
        ]
    )

    job = bq.query(q, job_config=job_config)
    return [dict(r) for r in job.result()]


def count_rows_for_date(report_date: date) -> int:
    q = f"""
    SELECT COUNT(*) AS c
    FROM {BQ_FULL_TABLE}
    WHERE DATE(snapshot_date) = @report_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("report_date", "DATE", report_date)]
    )
    rows = list(bq.query(q, job_config=job_config).result())
    return int(rows[0]["c"])


# ----------------------------
# SUPABASE (robust upsert)
# ----------------------------
def build_supabase_payload(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    payload: List[Dict[str, Any]] = []

    for r in rows:
        rd = r.get("report_date")
        rd_str = iso(rd)
        mp = r.get("mp_sup_key")
        if mp is None:
            continue

        status_map = {c: r.get(c) for c in SB_STATUS_COLS}
        raw_total, score_0_10, level, drivers, metric_scores = score_status_map(status_map)

        risk_reason = (
            f"Top drivers: {', '.join(drivers[:5])}" if drivers else "All metrics are Good (or empty)."
        )

        item: Dict[str, Any] = {
            "report_date": rd_str,
            "mp_sup_key": str(mp),
            "risk_score": int(score_0_10),          # <- 0..10 integer
            "risk_level": level,
            "risk_reason": risk_reason,
            "drivers": drivers,                     # jsonb
            "metric_scores": metric_scores,          # jsonb (points)
            "raw_status": status_map,                # jsonb (Good/Fair/Bad)
            "raw_total": int(raw_total),             # keep raw sum for audit
        }

        # also store each status in its own column for filtering
        item.update(status_map)

        payload.append(item)

    # de-dupe by PK inside same run
    dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for x in payload:
        dedup[(x["report_date"], x["mp_sup_key"])] = x
    return list(dedup.values())


def upsert_supabase(payload: List[Dict[str, Any]], chunk_size: int = 500, max_retries: int = 4) -> int:
    if not payload:
        print("No rows to upsert.")
        return 0

    total_written = 0
    chunks = math.ceil(len(payload) / chunk_size)

    for i in range(chunks):
        part = payload[i * chunk_size : (i + 1) * chunk_size]

        # retry loop
        for attempt in range(max_retries + 1):
            try:
                # IMPORTANT: do NOT print res (it can contain large JSON)
                supabase.table(SUPABASE_TABLE).upsert(
                    part,
                    on_conflict="report_date,mp_sup_key",
                ).execute()

                total_written += len(part)
                print(f"[OK] Upsert chunk {i+1}/{chunks}: {len(part)} rows")
                break

            except Exception as e:
                if attempt >= max_retries:
                    raise
                print(f"[WARN] Upsert failed (chunk {i+1}/{chunks}, attempt {attempt+1}/{max_retries}). Error: {e}")
                backoff_sleep(attempt)

    print(f"[OK] Upserted total {total_written} rows into {SUPABASE_TABLE}.")
    return total_written


def verify_supabase_count(report_date: str) -> Optional[int]:
    """
    Lightweight verification: count rows for date in Supabase.
    Note: depends on RLS/permissions. With service_role it should work.
    """
    try:
        res = supabase.table(SUPABASE_TABLE).select("report_date", count="exact").eq("report_date", report_date).execute()
        return getattr(res, "count", None)
    except Exception as e:
        print(f"[WARN] Verify failed: {e}")
        return None


# ----------------------------
# UNIFIED JSON EXPORT (schema you shared)
# ----------------------------
def build_unified_json(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    {
      "table_name": "customer_health_metrics",
      "supplier_key": "...",
      "report_date": "YYYY-MM-DD",
      "metrics": [
         {"metric_id":"...", "value":..., "unit":"...", "explanation":"..."}
      ],
      "overall_risk_score": 0.0~1.0
    }
    """
    report_date_str = iso(row.get("report_date"))
    supplier_key = str(row.get("mp_sup_key"))

    status_map = {c: row.get(c) for c in SB_STATUS_COLS}
    raw_total, score_0_10, level, drivers, metric_scores = score_status_map(status_map)

    # overall_risk_score in 0..1 (raw-based is more stable)
    overall = round(float(raw_total) / float(MAX_RAW), 4) if MAX_RAW > 0 else 0.0

    metrics = []
    for c in SB_STATUS_COLS:
        v = status_map.get(c)
        pts = metric_scores.get(c, 0)
        if pts == 0:
            explanation = "OK (Good or empty)."
        elif pts == 1:
            explanation = "Fair status contributes moderate risk."
        else:
            explanation = "Bad status contributes high risk."

        metrics.append(
            {
                "metric_id": c.upper(),
                "value": v,
                "unit": "status",
                "explanation": explanation,
            }
        )

    return {
        "table_name": "customer_health_metrics",
        "supplier_key": supplier_key,
        "report_date": report_date_str,
        "metrics": metrics,
        "overall_risk_score": overall,
    }


def export_unified_json(rows: List[Dict[str, Any]], output_file: str = "risk_output.json") -> int:
    out = []
    for r in rows:
        if r.get("mp_sup_key") is None:
            continue
        out.append(build_unified_json(r))

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"[OK] Exported {len(out)} unified JSON records -> {output_file}")
    return len(out)


# ----------------------------
# PIPELINE
# ----------------------------
def run_for_date(report_date: date, limit: int, chunk_size: int, export_json: bool, verify: bool, dry_run: bool) -> None:
    print("=" * 70)
    print(f"Report date: {report_date.isoformat()}")

    bq_count = count_rows_for_date(report_date)
    print(f"BigQuery rows for this date (all snapshots): {bq_count}")

    rows = fetch_latest_per_merchant(report_date, limit=limit)
    print(f"Latest-per-merchant rows fetched (after window rn=1): {len(rows)}")
    if rows:
        print(f"Sample mp_sup_key: {rows[0].get('mp_sup_key')}")

    if not rows:
        print("[INFO] No data returned for this report_date. Skip.")
        return

    payload = build_supabase_payload(rows)
    print(f"Prepared Supabase payload rows: {len(payload)}")
    if payload:
        # quick sanity: score range
        scores = [p["risk_score"] for p in payload]
        print(f"Risk score range (0..10): min={min(scores)} max={max(scores)}")

    if dry_run:
        print("[DRY-RUN] Skipping Supabase write.")
    else:
        written = upsert_supabase(payload, chunk_size=chunk_size)
        if verify:
            c = verify_supabase_count(report_date.isoformat())
            print(f"[VERIFY] Supabase count for report_date={report_date.isoformat()}: {c}")

    if export_json:
        export_unified_json(rows, output_file="risk_output.json")

    print("[DONE]")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--chunk-size", type=int, default=500)
    parser.add_argument("--days-back", type=int, default=0, help="Run for latest_date and previous N days (0=only latest).")
    parser.add_argument("--report-date", type=str, default="", help="Override report date (YYYY-MM-DD).")
    parser.add_argument("--no-export-json", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    export_json = not args.no_export_json

    if args.report_date:
        rd = date.fromisoformat(args.report_date)
        run_for_date(rd, limit=args.limit, chunk_size=args.chunk_size, export_json=export_json, verify=args.verify, dry_run=args.dry_run)
        return

    latest = get_latest_report_date()
    # run latest and optionally backfill
    for i in range(args.days_back + 1):
        rd = latest - timedelta(days=i)
        run_for_date(rd, limit=args.limit, chunk_size=args.chunk_size, export_json=export_json, verify=args.verify, dry_run=args.dry_run)


if __name__ == "__main__":
    main()