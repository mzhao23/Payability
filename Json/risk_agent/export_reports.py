"""export_reports.py — Download risk reports from Supabase to a local JSON file.

Usage:
    python export_reports.py                        # all reports
    python export_reports.py --limit 100            # latest 100
    python export_reports.py --date 2026-03-10      # specific date
    python export_reports.py --output my_file.json  # custom output path
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL             = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
SUPABASE_RISK_TABLE      = os.environ.get("SUPABASE_RISK_TABLE", "supplier_risk_reports")


def fetch_reports(filter_date: str | None, limit: int | None) -> list[dict]:
    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    query = client.table(SUPABASE_RISK_TABLE).select(
        "table_name, supplier_key, mp_sup_key, supplier_name, "
        "report_date, metrics, trigger_reason, overall_risk_score"
    )

    if filter_date:
        query = query.eq("report_date", filter_date)

    query = query.order("report_date", desc=True)

    if limit:
        query = query.limit(limit)

    response = query.execute()
    return response.data or []


def format_report(row: dict) -> dict:
    """Reshape a Supabase row into the target JSON schema."""
    # metrics may be stored as a JSON string or already a list
    metrics = row.get("metrics") or []
    if isinstance(metrics, str):
        try:
            metrics = json.loads(metrics)
        except json.JSONDecodeError:
            metrics = []

    return {
        "table_name":        row.get("table_name"),
        "supplier_key":      row.get("supplier_key"),
        "mp_sup_key":        row.get("mp_sup_key"),
        "supplier_name":     row.get("supplier_name"),
        "report_date":       row.get("report_date"),
        "metrics":           metrics,
        "trigger_reason":    row.get("trigger_reason"),
        "overall_risk_score": row.get("overall_risk_score"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Supabase risk reports to JSON.")
    parser.add_argument("--date",   help="Filter by report_date (YYYY-MM-DD)")
    parser.add_argument("--limit",  type=int, help="Max number of reports to fetch")
    parser.add_argument("--output", default="risk_reports_export.json", help="Output file path")
    args = parser.parse_args()

    print(f"Fetching from {SUPABASE_RISK_TABLE}...")
    rows = fetch_reports(filter_date=args.date, limit=args.limit)
    print(f"Fetched {len(rows)} reports.")

    reports = [format_report(r) for r in rows]

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(reports, f, indent=2, default=str)

    print(f"Saved to {args.output}")


if __name__ == "__main__":
    main()