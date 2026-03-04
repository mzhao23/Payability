from google.cloud import bigquery
from supabase import create_client
import os

# -------------------------
# CONFIG
# -------------------------

BIGQUERY_PROJECT = "bigqueryexport-183608"
DATASET = "amazon"
TABLE = "customer_health_metrics"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# clients
bq_client = bigquery.Client(project=BIGQUERY_PROJECT)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# -------------------------
# RISK LOGIC
# -------------------------

WEIGHTS = {
    "lateShipmentRate_status": 30,
    "orderDefectRate_status": 30,
    "productSafetyStatus_status": 40
}


def calculate_risk(row):

    score = 0

    for metric, weight in WEIGHTS.items():

        status = row.get(metric)

        if status == "Bad":
            score += weight

        elif status == "Fair":
            score += weight * 0.5

    return score


def risk_level(score):

    if score >= 70:
        return "High"

    elif score >= 40:
        return "Medium"

    return "Low"


# -------------------------
# FETCH DATA
# -------------------------

def fetch_metrics():

    query = f"""
    SELECT
      DATE(snapshot_date) AS report_date,
      mp_sup_key,
      lateShipmentRate_status,
      orderDefectRate_status,
      productSafetyStatus_status
    FROM `{BIGQUERY_PROJECT}.{DATASET}.{TABLE}`
    LIMIT 50
    """

    print("Fetching data from BigQuery...")

    query_job = bq_client.query(query)

    rows = [dict(row) for row in query_job.result()]

    print("Rows fetched:", len(rows))

    return rows


# -------------------------
# SAVE TO SUPABASE
# -------------------------

def save_to_supabase(rows):

    inserts = []

    for r in rows:

        score = calculate_risk(r)

        inserts.append({

            "mp_sup_key": r.get("mp_sup_key"),
            "report_date": str(r.get("report_date")),
            "risk_score": score,
            "risk_level": risk_level(score)

        })

    print("Saving to Supabase...")

    supabase.table("merchant_daily_risk").insert(inserts).execute()

    print("Saved.")


# -------------------------
# MAIN
# -------------------------

def run_pipeline():

    rows = fetch_metrics()

    save_to_supabase(rows)

    print("Pipeline finished.")


if __name__ == "__main__":
    run_pipeline()
