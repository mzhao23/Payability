import logging
from google.cloud import bigquery
from config.settings import BQ_PROJECT

logger = logging.getLogger(__name__)


class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client(project=BQ_PROJECT)

    def run_query(self, sql: str) -> list[dict]:
        """Execute a SQL query and return results as list of dicts."""
        logger.info("Running BigQuery query...")
        query_job = self.client.query(sql)
        results = query_job.result()
        rows = [dict(row) for row in results]
        logger.info(f"  → {len(rows)} rows returned")
        return rows

    def load_and_run(self, sql_path: str, params: dict) -> list[dict]:
        """Load SQL from file, inject params, and run."""
        with open(sql_path, "r") as f:
            sql = f.read().format(**params)
        return self.run_query(sql)