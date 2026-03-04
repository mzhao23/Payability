# ============================================================
# supabase_client.py
# Handles all Supabase database operations.
# Saves and retrieves daily risk reports.
# ============================================================

import json
import os
from datetime import date
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()


def get_client() -> Client:
    """
    Initialize and return a Supabase client.
    """
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")

    if not url or not key:
        raise ValueError("SUPABASE_URL or SUPABASE_SERVICE_KEY is not set")

    return create_client(url, key)


def save_report(
    report_date: date,
    overall_risk_level: str,
    executive_summary: str,
    metrics: dict,
    key_findings: list,
    supplier_breakdown: list,
    carrier_breakdown: list,
    recommendations: list,
    raw_sql_results: dict,
    gemini_full_report: dict,
) -> dict:
    """
    Save a daily risk report to Supabase.
    Returns the saved record.
    """
    client = get_client()

    record = {
        "report_date": report_date.isoformat(),
        "overall_risk_level": overall_risk_level,
        "executive_summary": executive_summary,

        # Metrics (extracted from SQL results by Python, not Gemini)
        "untracked_rate": metrics.get("untracked_rate"),
        "untracked_rate_delta": metrics.get("untracked_rate_delta"),
        "avg_order_value": metrics.get("avg_order_value"),
        "avg_order_value_delta": metrics.get("avg_order_value_delta"),
        "avg_init_to_pickup_hours": metrics.get("avg_init_to_pickup_hours"),
        "avg_pickup_to_delivery_hours": metrics.get("avg_pickup_to_delivery_hours"),
        "overdue_unpickup_count": metrics.get("overdue_unpickup_count"),

        # Gemini analysis output
        "key_findings": key_findings,
        "supplier_breakdown": supplier_breakdown,
        "carrier_breakdown": carrier_breakdown,
        "recommendations": recommendations,

        # Raw data for debugging
        "raw_sql_results": raw_sql_results,
        "gemini_full_report": gemini_full_report,
    }
    # # Convert any non-serializable objects in raw_sql_results
    # raw_sql_results = json.loads(json.dumps(raw_sql_results, default=str))
    # gemini_full_report = json.loads(json.dumps(gemini_full_report, default=str))
    
    # Serialize entire record to handle all non-JSON-serializable objects
    record = json.loads(json.dumps(record, default=str))

    print(f"Saving report for {report_date} to Supabase...")
    response = client.table("daily_reports").insert(record).execute()

    if response.data:
        print(f"  ✓ Report saved, id: {response.data[0]['id']}")
        return response.data[0]
    else:
        raise Exception(f"Failed to save report: {response}")


def get_recent_reports(days: int = 30) -> list[dict]:
    """
    Fetch the most recent N days of reports.
    Used for historical baseline comparison.
    """
    client = get_client()

    response = (
        client.table("daily_reports")
        .select("*")
        .order("report_date", desc=True)
        .limit(days)
        .execute()
    )

    return response.data


def get_report_by_date(report_date: date) -> dict | None:
    """
    Fetch a specific report by date.
    Returns None if not found.
    """
    client = get_client()

    response = (
        client.table("daily_reports")
        .select("*")
        .eq("report_date", report_date.isoformat())
        .execute()
    )

    if response.data:
        return response.data[0]
    return None
def save_supplier_risks(report_date: str, anomalies: dict, sql_results: dict):
    """
    Save per-supplier risk data to supplier_daily_risks table.
    """
    client = get_client()

    rows_to_insert = []

    # Collect all supplier metrics from sql_results
    supplier_metrics = {}

    for row in sql_results.get("untracked_orders", []):
        sk = row.get("supplier_key")
        if sk:
            if sk not in supplier_metrics:
                supplier_metrics[sk] = {}
            supplier_metrics[sk]["untracked_rate"] = row.get("untracked_rate")

    for row in sql_results.get("high_value_items", []):
        sk = row.get("supplier_key")
        if sk:
            if sk not in supplier_metrics:
                supplier_metrics[sk] = {}
            supplier_metrics[sk]["avg_order_value"] = row.get("avg_order_value_7d")

    for row in sql_results.get("logistics_timing", []):
        sk = row.get("supplier_key")
        if sk:
            if sk not in supplier_metrics:
                supplier_metrics[sk] = {}
            supplier_metrics[sk]["avg_init_to_pickup_hours"] = row.get("avg_init_to_pickup_hours")
            supplier_metrics[sk]["overdue_unpickup_count"] = row.get("overdue_unpickup_count")

    for row in sql_results.get("order_package_ratio", []):
        sk = row.get("supplier_key")
        if sk:
            if sk not in supplier_metrics:
                supplier_metrics[sk] = {}
            supplier_metrics[sk]["avg_packages_per_order"] = row.get("avg_packages_per_order")

    # Collect flagged suppliers from anomalies
    flagged_suppliers = {}

    for item in anomalies.get("untracked_orders", {}).get("top_offenders", []):
        sk = item.get("supplier_key")
        if sk:
            if sk not in flagged_suppliers:
                flagged_suppliers[sk] = []
            flagged_suppliers[sk].append("high_untracked_rate")

    for item in anomalies.get("high_value_items", {}).get("top_offenders", []):
        sk = item.get("supplier_key")
        if sk:
            if sk not in flagged_suppliers:
                flagged_suppliers[sk] = []
            flagged_suppliers[sk].append("high_value_increase")

    for item in anomalies.get("logistics_timing", {}).get("top_overdue_suppliers", []):
        sk = item.get("supplier_key")
        if sk:
            if sk not in flagged_suppliers:
                flagged_suppliers[sk] = []
            flagged_suppliers[sk].append("overdue_unpickup")

    for item in anomalies.get("logistics_timing", {}).get("top_slow_pickup_suppliers", []):
        sk = item.get("supplier_key")
        if sk:
            if sk not in flagged_suppliers:
                flagged_suppliers[sk] = []
            flagged_suppliers[sk].append("slow_pickup")

    for item in anomalies.get("order_package_ratio", {}).get("top_offenders", []):
        sk = item.get("supplier_key")
        if sk:
            if sk not in flagged_suppliers:
                flagged_suppliers[sk] = []
            flagged_suppliers[sk].append("unusual_package_ratio")

    # Only save flagged suppliers
    for sk, issues in flagged_suppliers.items():
        metrics = supplier_metrics.get(sk, {})
        issue_count = len(issues)

        # Simple risk score: 1-10 based on number and type of issues
        risk_score = min(10, issue_count * 2 + (
            3 if "overdue_unpickup" in issues else 0
        ))
        risk_level = (
            "HIGH" if risk_score >= 7
            else "MEDIUM" if risk_score >= 4
            else "LOW"
        )

        rows_to_insert.append({
            "report_date": report_date,
            "supplier_key": sk,
            "risk_score": risk_score,
            "risk_level": risk_level,
            "untracked_rate": metrics.get("untracked_rate"),
            "avg_order_value": metrics.get("avg_order_value"),
            "avg_init_to_pickup_hours": metrics.get("avg_init_to_pickup_hours"),
            "overdue_unpickup_count": metrics.get("overdue_unpickup_count"),
            "avg_packages_per_order": metrics.get("avg_packages_per_order"),
            "issues": json.dumps(issues),
        })

    if not rows_to_insert:
        print("  No flagged suppliers to save")
        return 0

    # Serialize and insert
    serialized = json.loads(json.dumps(rows_to_insert, default=str))
    result = client.table("supplier_daily_risks").insert(serialized).execute()
    print(f"  ✓ Saved {len(rows_to_insert)} supplier risk records")
    return len(rows_to_insert)

if __name__ == "__main__":
    # Quick connection test
    print("Testing Supabase connection...")
    client = get_client()
    response = client.table("daily_reports").select("count").execute()
    print(f"  ✓ Supabase connected successfully")
