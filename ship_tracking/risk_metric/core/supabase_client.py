import logging
from datetime import date
from supabase import create_client, Client
from config.settings import SUPABASE_URL, SUPABASE_KEY

logger = logging.getLogger(__name__)

# Unique constraint columns per table
# Must match the UNIQUE constraint defined in migrations
TABLE_CONFLICT_COLUMNS = {
    "supplier_daily_metrics": "run_date,supplier_key,carrier",
    "supplier_risk_scores": "report_date,supplier_key",
}


class SupabaseClient:
    def __init__(self):
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    def upsert(self, table: str, rows: list[dict]):
        """Upsert rows into a Supabase table."""
        if not rows:
            logger.info(f"  → No rows to upsert into {table}")
            return

        # Convert date objects to ISO string for JSON serialization
        serialized = []
        for row in rows:
            serialized.append({
                k: v.isoformat() if isinstance(v, date) else v
                for k, v in row.items()
            })

        # Use on_conflict to tell Supabase which columns define uniqueness
        on_conflict = TABLE_CONFLICT_COLUMNS.get(table)
        if on_conflict:
            response = self.client.table(table).upsert(
                serialized,
                on_conflict=on_conflict
            ).execute()
        else:
            response = self.client.table(table).upsert(serialized).execute()

        logger.info(f"  → Upserted {len(serialized)} rows into '{table}'")
        return response