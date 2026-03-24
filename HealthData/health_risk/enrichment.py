from __future__ import annotations

from typing import Any, Dict, List

from health_risk.repositories.bigquery import BigQueryRepository
from health_risk.repositories.supabase import SupabaseRepository
from health_risk.utils import normalize_key


class SupplierContextEnricher:
    """
    Joins BigQuery health rows with Supabase mp->supplier mapping and Payability status.
    """

    def __init__(
        self,
        bigquery_repo: BigQueryRepository,
        supabase_repo: SupabaseRepository,
    ) -> None:
        self._bq = bigquery_repo
        self._sb = supabase_repo

    def enrich(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        mapping = self._sb.fetch_supplier_mapping()
        payability_map = self._bq.fetch_payability_status_map()

        enriched: List[Dict[str, Any]] = []
        for row in rows:
            mp_sup_key_norm = normalize_key(row.get("mp_sup_key"))
            map_row = mapping.get(mp_sup_key_norm, {})

            supplier_key = map_row.get("supplier_key")
            supplier_key_norm = normalize_key(supplier_key)
            pay_row = payability_map.get(supplier_key_norm, {})

            row["supplier_key"] = supplier_key
            row["supplier_name"] = map_row.get("supplier_name") or pay_row.get("supplier_name")
            row["payability_status"] = pay_row.get("payability_status")

            enriched.append(row)

        return enriched
