"""sync_suppliers.py — Sync unique suppliers from BigQuery (last 3 days) to Supabase.

Table: suppliers
  mp_sup_key    TEXT PRIMARY KEY
  supplier_key  TEXT
  supplier_name TEXT
  notes         TEXT   -- append-only log of any field changes over time
  updated_at    TIMESTAMPTZ

First write: inserts all fields.
Subsequent runs: if any field changed, appends to notes. Original values are never overwritten.

Usage:
    python sync_suppliers.py
    python sync_suppliers.py --days 7
    python sync_suppliers.py --dry-run
    python sync_suppliers.py --print-migration
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import pathlib
from datetime import date

sys.path.insert(0, str(pathlib.Path(__file__).parent))

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from supabase import create_client

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
BQ_PROJECT_ID             = os.environ["BQ_PROJECT_ID"]
BQ_DATASET                = os.environ["BQ_DATASET"]
BQ_TABLE                  = os.environ["BQ_TABLE"]
BQ_SERVICE_ACCOUNT_PATH   = os.environ.get("BQ_SERVICE_ACCOUNT_PATH", "")
BQ_SERVICE_ACCOUNT_JSON   = os.environ.get("BQ_SERVICE_ACCOUNT_JSON", "")
SUPABASE_URL              = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
SUPPLIERS_TABLE           = os.environ.get("SUPABASE_SUPPLIERS_TABLE", "suppliers")

# Fields to track for changes (besides mp_sup_key which is the key)
TRACKED_FIELDS = ["supplier_key", "supplier_name"]


# ── BigQuery ──────────────────────────────────────────────────────────────────
def _build_bq_client() -> bigquery.Client:
    if BQ_SERVICE_ACCOUNT_PATH:
        creds = service_account.Credentials.from_service_account_file(
            BQ_SERVICE_ACCOUNT_PATH,
            scopes=[
                "https://www.googleapis.com/auth/bigquery",
                "https://www.googleapis.com/auth/cloud-platform",
            ],
        )
    elif BQ_SERVICE_ACCOUNT_JSON:
        try:
            info = json.loads(BQ_SERVICE_ACCOUNT_JSON)
        except json.JSONDecodeError as exc:
            raise EnvironmentError("BQ_SERVICE_ACCOUNT_JSON is not valid JSON.") from exc
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=[
                "https://www.googleapis.com/auth/bigquery",
                "https://www.googleapis.com/auth/cloud-platform",
            ],
        )
    else:
        raise EnvironmentError("Set BQ_SERVICE_ACCOUNT_PATH or BQ_SERVICE_ACCOUNT_JSON.")
    return bigquery.Client(project=BQ_PROJECT_ID, credentials=creds)


def fetch_suppliers(lookback_days: int) -> list[dict]:
    """Fetch one row per mp_sup_key (most recent) from the last N days."""
    full_table = f"`{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`"
    query = f"""
        WITH ranked AS (
            SELECT
                mp_sup_key,
                marketplace_ext_data_key AS supplier_key,
                COALESCE(
                    NULLIF(JSON_VALUE(data, '$.Supplier Name'),       ''),
                    NULLIF(JSON_VALUE(data, '$.Legal Business Name'), ''),
                    NULLIF(JSON_VALUE(data, '$.Store Name'),          ''),
                    NULLIF(JSON_VALUE(data, '$.store_name'),          ''),
                    'Unknown'
                ) AS supplier_name,
                ROW_NUMBER() OVER (
                    PARTITION BY mp_sup_key
                    ORDER BY create_ts DESC
                ) AS rn
            FROM {full_table}
            WHERE create_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY)
              AND mp_sup_key IS NOT NULL
        )
        SELECT mp_sup_key, supplier_key, supplier_name
        FROM ranked
        WHERE rn = 1
        ORDER BY supplier_name
    """
    client = _build_bq_client()
    print(f"Querying BigQuery (last {lookback_days} days)...")
    rows = [dict(row) for row in client.query(query).result()]
    print(f"Found {len(rows)} unique suppliers in BQ.")
    return rows


# ── Supabase ──────────────────────────────────────────────────────────────────
def fetch_existing(sb_client, keys: list[str]) -> dict[str, dict]:
    """Fetch existing rows from Supabase for the given mp_sup_keys.
    Batched in chunks of 100 to avoid URL length limits with .in_().
    """
    if not keys:
        return {}
    result = {}
    chunk_size = 100
    for i in range(0, len(keys), chunk_size):
        chunk = keys[i : i + chunk_size]
        response = (
            sb_client.table(SUPPLIERS_TABLE)
            .select("mp_sup_key, supplier_key, supplier_name, notes")
            .in_("mp_sup_key", chunk)
            .execute()
        )
        for row in (response.data or []):
            result[row["mp_sup_key"]] = row
    return result


def _append_note(existing_notes: str | None, new_note: str) -> str:
    """Append a new note line to the existing notes string."""
    lines = existing_notes.strip().splitlines() if existing_notes else []
    lines.append(new_note)
    return "\n".join(lines)


def sync(bq_rows: list[dict], dry_run: bool = False) -> None:
    sb_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    today = date.today().isoformat()

    # Fetch all existing rows in one query
    all_keys = [r["mp_sup_key"] for r in bq_rows]
    existing = fetch_existing(sb_client, all_keys)
    print(f"Existing in Supabase: {len(existing)} / {len(bq_rows)} suppliers.")

    to_insert = []
    to_update = []  # list of (mp_sup_key, {notes: "..."})

    for row in bq_rows:
        key = row["mp_sup_key"]

        if key not in existing:
            # New supplier — insert as-is, no notes needed
            to_insert.append({
                "mp_sup_key":    key,
                "supplier_key":  row["supplier_key"],
                "supplier_name": row["supplier_name"],
                "notes":         None,
            })
            continue

        # Existing supplier — check for changes
        ex = existing[key]
        change_lines = []

        for field in TRACKED_FIELDS:
            old_val = (ex.get(field) or "").strip()
            new_val = (row.get(field) or "").strip()
            if old_val and new_val and old_val != new_val:
                change_lines.append(
                    f"{today}: {field} changed from '{old_val}' to '{new_val}'"
                )

        if change_lines:
            updated_notes = _append_note(ex.get("notes"), "\n".join(change_lines))
            to_update.append((key, {"notes": updated_notes}))

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\nNew suppliers to insert : {len(to_insert)}")
    print(f"Suppliers with changes  : {len(to_update)}")
    print(f"Suppliers unchanged     : {len(bq_rows) - len(to_insert) - len(to_update)}")

    if dry_run:
        print("\n[DRY RUN] Sample inserts:")
        for r in to_insert[:5]:
            print(f"  INSERT {r['mp_sup_key'][:8]}... {r['supplier_name']}")
        print("\n[DRY RUN] Sample updates (notes appended):")
        for key, patch in to_update[:5]:
            print(f"  UPDATE {key[:8]}...")
            for line in patch["notes"].splitlines()[-3:]:
                print(f"    {line}")
        return

    # ── Write inserts ─────────────────────────────────────────────────────────
    chunk_size = 500
    for i in range(0, len(to_insert), chunk_size):
        sb_client.table(SUPPLIERS_TABLE).insert(to_insert[i:i+chunk_size]).execute()
    if to_insert:
        print(f"Inserted {len(to_insert)} new suppliers.")

    # ── Write updates (notes only — other fields never overwritten) ───────────
    for key, patch in to_update:
        sb_client.table(SUPPLIERS_TABLE).update(patch).eq("mp_sup_key", key).execute()
    if to_update:
        print(f"Updated notes for {len(to_update)} suppliers.")

    print("Sync complete.")


# ── Migration SQL ─────────────────────────────────────────────────────────────
MIGRATION_SQL = """
-- Run once in Supabase SQL editor before using this script

CREATE TABLE IF NOT EXISTS suppliers (
    mp_sup_key    TEXT PRIMARY KEY,
    supplier_key  TEXT,
    supplier_name TEXT,
    notes         TEXT,            -- append-only log of field changes over time
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER suppliers_updated_at
    BEFORE INSERT OR UPDATE ON suppliers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
"""


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Sync suppliers from BQ to Supabase.")
    parser.add_argument("--days",            type=int, default=3,  help="Lookback days (default: 3)")
    parser.add_argument("--dry-run",         action="store_true",  help="Preview without writing")
    parser.add_argument("--print-migration", action="store_true",  help="Print migration SQL and exit")
    args = parser.parse_args()

    if args.print_migration:
        print(MIGRATION_SQL)
        return

    bq_rows = fetch_suppliers(lookback_days=args.days)
    if not bq_rows:
        print("No suppliers found — nothing to sync.")
        return

    sync(bq_rows, dry_run=args.dry_run)


if __name__ == "__main__":
    main()