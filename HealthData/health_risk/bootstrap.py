from __future__ import annotations

from google.cloud import bigquery
from supabase import create_client

from health_risk.config import Settings, load_settings
from health_risk.enrichment import SupplierContextEnricher
from health_risk.pipeline import HealthRiskPipeline
from health_risk.repositories.bigquery import BigQueryRepository
from health_risk.repositories.supabase import SupabaseRepository
from health_risk.scoring.engine import RiskScoreEngine


def create_clients(settings: Settings):
    bq = bigquery.Client(project=settings.bq_project)
    supabase = create_client(settings.supabase_url, settings.supabase_key)
    return bq, supabase


def build_pipeline(
    settings: Settings,
    *,
    bq_client: bigquery.Client | None = None,
    supabase_client=None,
) -> HealthRiskPipeline:
    """
    Wire repositories, enricher, and scorer. Pass explicit clients in tests; otherwise
    they are constructed from settings.
    """
    if bq_client is None or supabase_client is None:
        bq_new, sb_new = create_clients(settings)
        if bq_client is None:
            bq_client = bq_new
        if supabase_client is None:
            supabase_client = sb_new

    bq_repo = BigQueryRepository(bq_client, settings)
    sb_repo = SupabaseRepository(supabase_client, settings)
    enricher = SupplierContextEnricher(bq_repo, sb_repo)
    scorer = RiskScoreEngine()

    return HealthRiskPipeline(
        settings=settings,
        bigquery_repo=bq_repo,
        supabase_repo=sb_repo,
        enricher=enricher,
        scorer=scorer,
    )


def build_default_pipeline() -> HealthRiskPipeline:
    settings = load_settings()
    print("SUPABASE_URL =", settings.supabase_url)
    print("SUPABASE_MAPPING_TABLE =", settings.supabase_mapping_table)
    print("CONSOLIDATED_TABLE =", settings.consolidated_table)
    return build_pipeline(settings)
