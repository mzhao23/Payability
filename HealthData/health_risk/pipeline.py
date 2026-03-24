from __future__ import annotations

from datetime import date

from typing import Any, Dict, List

from health_risk.config import Settings
from health_risk.enrichment import SupplierContextEnricher
from health_risk.filters import filter_active_population
from health_risk.export import export_unified_json
from health_risk.flagged import build_consolidated_flagged_rows
from health_risk.repositories.bigquery import BigQueryRepository
from health_risk.repositories.supabase import SupabaseRepository
from health_risk.scoring.engine import RiskScoreEngine


class HealthRiskPipeline:
    """
    Orchestrates fetch -> enrich -> filter -> score -> sinks.
    Dependencies are injected for testing and alternative implementations.
    """

    def __init__(
        self,
        *,
        settings: Settings,
        bigquery_repo: BigQueryRepository,
        supabase_repo: SupabaseRepository,
        enricher: SupplierContextEnricher,
        scorer: RiskScoreEngine,
    ) -> None:
        self._settings = settings
        self._bq = bigquery_repo
        self._sb = supabase_repo
        self._enricher = enricher
        self._scorer = scorer

    def get_latest_report_date(self) -> date:
        return self._bq.get_latest_report_date()

    def run_for_date(
        self,
        report_date: date,
        *,
        limit: int,
        chunk_size: int,
        export_json: bool,
        dry_run: bool,
    ) -> None:
        print("=" * 80)
        print(f"Report date: {report_date.isoformat()}")

        health_rows = self._bq.fetch_latest_health_snapshot(report_date=report_date, limit=limit)
        print(f"[INFO] Health rows fetched from BigQuery: {len(health_rows)}")

        if not health_rows:
            print("[INFO] No rows found. Skip.")
            return

        enriched_rows = self._enricher.enrich(health_rows)

        print(f"[INFO] Rows after supplier mapping enrichment: {len(enriched_rows)}")

        supplier_key_count = sum(1 for r in enriched_rows if r.get("supplier_key"))
        payability_count = sum(1 for r in enriched_rows if r.get("payability_status"))

        print(f"[DEBUG] Rows with supplier_key: {supplier_key_count}")
        print(f"[DEBUG] Rows with payability_status: {payability_count}")

        for r in enriched_rows[:5]:
            print(
                "[DEBUG SAMPLE]",
                r.get("mp_sup_key"),
                r.get("supplier_key"),
                r.get("payability_status"),
            )

        filtered_rows = filter_active_population(enriched_rows)

        print(f"[INFO] Rows after payability filter (exclude suspended/pending): {len(filtered_rows)}")

        payload: List[Dict[str, Any]] = self._scorer.build_payload(filtered_rows)
        print(f"[INFO] Payload rows prepared: {len(payload)}")

        if payload:
            scores = [float(p["risk_score"]) for p in payload]
            print(f"[INFO] Risk score range: min={min(scores):.2f}, max={max(scores):.2f}")

        if dry_run:
            print("[DRY-RUN] Skip Supabase write.")
        else:
            self._sb.upsert_health_daily_risk(payload, chunk_size=chunk_size)
            flagged = build_consolidated_flagged_rows(payload)
            self._sb.upsert_consolidated_flagged(flagged, chunk_size=chunk_size)

        if export_json:
            export_unified_json(payload, self._settings, output_file="risk_output.json")

        print("[DONE]")
