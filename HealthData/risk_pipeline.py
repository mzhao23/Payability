from google.cloud import bigquery
from supabase import create_client
import os

BIGQUERY_PROJECT = "bigqueryexport-183608"
DATASET = "amazon"
TABLE = "customer_health_metrics"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars.")

bq_client = bigquery.Client(project=BIGQUERY_PROJECT)
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

WEIGHTS = {
    "lateShipmentRate_status": 30,
    "productSafetyStatus_status": 40,
    # "orderDefectRate_status": 30,  # only enable after confirming the column exists
}

def calculate_risk(row: dict) -> float:
    score = 0.0
    for metric, weight in WEIGHTS.items():
        status = row.get(metric)
        if status == "Bad":
            score += weight
        elif status == "Fair":
            score += weight * 0.5
    return score

def risk_level(score: float) -> str:
    if score >= 70:
        return "High"
    elif score >= 40:
        return "Medium"
    return "Low"

def fetch_metrics() -> list[dict]:
    query = f"""
    DECLARE report_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    WITH latest AS (
      SELECT
        DATE(snapshot_date) AS report_date,
        mp_sup_key,
        policyViolation_status,
        listingPolicyStatus_status,
        customerServiceDissatisfactionRate_status,
        returnDissatisfactionRate_status,
        contactResponseTime_status,
        productSafetyStatus_status,
        lateShipmentRate_status,
        intellectualProperty_status,
        snapshot_date,
        ROW_NUMBER() OVER(
          PARTITION BY DATE(snapshot_date), mp_sup_key
          ORDER BY snapshot_date DESC
        ) AS rn
      FROM `{BIGQUERY_PROJECT}.{DATASET}.{TABLE}`
      WHERE DATE(snapshot_date) = report_date
    )
    SELECT
      report_date,
      mp_sup_key,
      policyViolation_status,
      listingPolicyStatus_status,
      customerServiceDissatisfactionRate_status,
      returnDissatisfactionRate_status,
      contactResponseTime_status,
      productSafetyStatus_status,
      lateShipmentRate_status,
      intellectualProperty_status
    FROM latest
    WHERE rn = 1
    """

    query_job = bq_client.query(query)
    return [dict(row) for row in query_job]

def save_to_supabase(rows: list[dict]) -> None:
    inserts = []
    for r in rows:
        score = calculate_risk(r)
        rd = r.get("report_date")
        # ensure JSON-friendly date
        report_date_str = rd.isoformat() if hasattr(rd, "isoformat") else str(rd)

        inserts.append({
            "mp_sup_key": r.get("mp_sup_key"),
            "report_date": report_date_str,
            "total_risk_score": int(score),   # your table column is total_risk_score (per your schema)
            "risk_level": risk_level(score),
            "risk_reason": None,              # optional for now
            # keep original statuses (optional but useful)
            "policyViolation_status": r.get("policyViolation_status"),
            "listingPolicyStatus_status": r.get("listingPolicyStatus_status"),
            "customerServiceDissatisfactionRate_status": r.get("customerServiceDissatisfactionRate_status"),
            "returnDissatisfactionRate_status": r.get("returnDissatisfactionRate_status"),
            "contactResponseTime_status": r.get("contactResponseTime_status"),
            "productSafetyStatus_status": r.get("productSafetyStatus_status"),
            "lateShipmentRate_status": r.get("lateShipmentRate_status"),
            "intellectualProperty_status": r.get("intellectualProperty_status"),
        })

    supabase.table("merchant_daily_risk").upsert(inserts).execute()

def run_pipeline():
    print("Fetching data from BigQuery...")
    rows = fetch_metrics()
    print(f"{len(rows)} rows loaded")

    if not rows:
        print("No rows for yesterday. Done.")
        return

    print("Saving risk scores to Supabase...")
    save_to_supabase(rows)
    print("Done.")

if __name__ == "__main__":
    run_pipeline()
