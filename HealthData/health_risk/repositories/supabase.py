from __future__ import annotations

import math
from typing import Any, Dict, List

from supabase import Client

from health_risk.config import Settings
from health_risk.utils import backoff_sleep, normalize_key


class SupabaseRepository:
    """Supabase mapping reads and risk-table writes."""

    def __init__(self, client: Client, settings: Settings) -> None:
        self._sb = client
        self._settings = settings

    def fetch_supplier_mapping(self) -> Dict[str, Dict[str, Any]]:
        offset = 0
        page_size = 1000
        rows: List[Dict[str, Any]] = []

        while True:
            resp = (
                self._sb.table(self._settings.supabase_mapping_table)
                .select("mp_sup_key,supplier_key,supplier_name")
                .range(offset, offset + page_size - 1)
                .execute()
            )

            batch = getattr(resp, "data", None) or []
            rows.extend(batch)

            if len(batch) < page_size:
                break

            offset += page_size

        mapping: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            mp_sup_key = normalize_key(r.get("mp_sup_key"))
            if mp_sup_key is None:
                continue

            mapping[mp_sup_key] = {
                "supplier_key": r.get("supplier_key"),
                "supplier_name": r.get("supplier_name"),
            }

        return mapping

    def upsert_health_daily_risk(
        self,
        payload: List[Dict[str, Any]],
        *,
        chunk_size: int = 500,
        max_retries: int = 4,
    ) -> int:
        if not payload:
            print("[INFO] No rows to upsert.")
            return 0

        total_written = 0
        chunks = math.ceil(len(payload) / chunk_size)
        table = self._settings.supabase_output_table

        for i in range(chunks):
            part = payload[i * chunk_size : (i + 1) * chunk_size]

            for attempt in range(max_retries + 1):
                try:
                    (
                        self._sb.table(table)
                        .upsert(part, on_conflict="report_date,mp_sup_key")
                        .execute()
                    )
                    total_written += len(part)
                    print(f"[OK] Upsert chunk {i + 1}/{chunks}: {len(part)} rows")
                    break
                except Exception as e:
                    if attempt >= max_retries:
                        raise
                    print(
                        f"[WARN] Upsert failed chunk {i + 1}/{chunks}, attempt {attempt + 1}: {e}"
                    )
                    backoff_sleep(attempt)

        print(f"[OK] Upserted total {total_written} rows into {table}.")
        return total_written

    def upsert_consolidated_flagged(
        self,
        rows: List[Dict[str, Any]],
        *,
        chunk_size: int = 500,
        max_retries: int = 4,
    ) -> int:
        if not rows:
            print("[INFO] No high-risk suppliers to write into consolidated table.")
            return 0

        total_written = 0
        chunks = math.ceil(len(rows) / chunk_size)
        table = self._settings.consolidated_table

        for i in range(chunks):
            part = rows[i * chunk_size : (i + 1) * chunk_size]

            for attempt in range(max_retries + 1):
                try:
                    (
                        self._sb.table(table)
                        .upsert(part, on_conflict="supplier_key,source")
                        .execute()
                    )
                    total_written += len(part)
                    print(f"[OK] Upserted chunk {i + 1}/{chunks}: {len(part)} rows into {table}")
                    break
                except Exception as e:
                    if attempt >= max_retries:
                        raise
                    print(
                        f"[WARN] Consolidated insert failed chunk {i + 1}/{chunks}, "
                        f"attempt {attempt + 1}: {e}"
                    )
                    backoff_sleep(attempt)

        print(f"[INFO] High risk suppliers count: {len(rows)}")
        print(f"[OK] Inserted total {total_written} flagged suppliers into {table}.")
        return total_written
