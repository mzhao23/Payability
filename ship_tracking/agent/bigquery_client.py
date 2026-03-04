# ============================================================
# bigquery_client.py
# Handles all BigQuery connections and query execution.
# ============================================================

import json
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

def get_client() -> bigquery.Client:
    """
    Initialize and return a BigQuery client.
    Reads credentials from the GOOGLE_SERVICE_ACCOUNT_KEY env variable.
    """
    # key_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_KEY")
    # if not key_json:
    #     raise ValueError("GOOGLE_SERVICE_ACCOUNT_KEY environment variable is not set")

    # key_dict = json.loads(key_json)
    # credentials = service_account.Credentials.from_service_account_info(
    #     key_dict,
    #     scopes=["https://www.googleapis.com/auth/bigquery"]
    # )

    # return bigquery.Client(
    #     credentials=credentials,
    #     project=key_dict["project_id"]
    # )

    key_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_KEY_PATH")
    if not key_path:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_KEY_PATH is not set")

    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    with open(key_path) as f:
        project_id = json.load(f)["project_id"]

    return bigquery.Client(credentials=credentials, project=project_id)


def run_query(sql: str) -> list[dict]:
    """
    Execute a SQL query and return results as a list of dicts.
    """
    client = get_client()
    query_job = client.query(sql)
    results = query_job.result()

    return [dict(row) for row in results]


def run_queries(queries: list[dict]) -> dict:
    """
    Execute multiple queries and return results keyed by query id.

    Args:
        queries: list of dicts with keys: id, name, description, sql

    Returns:
        {
            "untracked_orders": [...rows...],
            "high_value_items": [...rows...],
            ...
        }
    """
    client = get_client()
    results = {}

    for query in queries:
        query_id = query["id"]
        sql = query["sql"]

        print(f"Running query: {query_id}...")
        try:
            query_job = client.query(sql)
            rows = query_job.result()
            results[query_id] = [dict(row) for row in rows]
            print(f"  ✓ {query_id}: {len(results[query_id])} rows returned")
        except Exception as e:
            print(f"  ✗ {query_id} failed: {e}")
            results[query_id] = {"error": str(e)}

    return results


if __name__ == "__main__":
    # Quick connection test
    test_sql = """
        SELECT COUNT(*) as total_rows
        FROM payabilitytest.tracking.track_test
        WHERE _sdc_deleted_at IS NULL
    """
    print("Testing BigQuery connection...")
    rows = run_query(test_sql)
    print(f"Connection successful! Total rows: {rows[0]['total_rows']}")