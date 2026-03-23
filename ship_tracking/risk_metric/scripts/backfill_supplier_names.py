"""
Backfill supplier_name in Supabase supplier_risk_scores table
from BigQuery vm_transaction_summary table.

Usage:
    python scripts/backfill_supplier_names.py
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.bigquery_client import BigQueryClient
from core.supabase_client import SupabaseClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BQ_NAME_QUERY = """
SELECT DISTINCT supplier_key, supplier_name
FROM `bigqueryexport-183608.PayabilitySheets.vm_transaction_summary`
WHERE supplier_name IS NOT NULL
  AND supplier_key IS NOT NULL
"""

def run():
    bq = BigQueryClient()
    sb = SupabaseClient()

    # Step 1: Get supplier_key -> supplier_name mapping from BigQuery
    logger.info("Fetching supplier names from BigQuery...")
    rows = bq.run_query(BQ_NAME_QUERY)
    name_map = {str(r["supplier_key"]): r["supplier_name"] for r in rows}
    logger.info(f"  → {len(name_map)} supplier names found in BigQuery")

    # Step 2: Get all rows in supplier_risk_scores with no name
    logger.info("Fetching supplier_risk_scores rows with missing names...")
    result = sb.client.table("supplier_risk_scores") \
        .select("id, supplier_key") \
        .is_("supplier_name", "null") \
        .execute()
    rows_to_update = result.data
    logger.info(f"  → {len(rows_to_update)} rows need supplier_name")

    # Step 3: Update each row
    matched = 0
    not_found = 0
    for row in rows_to_update:
        key = str(row["supplier_key"])
        name = name_map.get(key)
        if name:
            sb.client.table("supplier_risk_scores") \
                .update({"supplier_name": name}) \
                .eq("id", row["id"]) \
                .execute()
            matched += 1
        else:
            not_found += 1

    logger.info(f"✅ Done! Matched: {matched}, Not found: {not_found}")

if __name__ == "__main__":
    run()