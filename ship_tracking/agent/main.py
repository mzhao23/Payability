# ============================================================
# main.py
# Main entry point for the Risk Agent.
# Orchestrates the full pipeline:
# 1. Generate SQL queries via Gemini
# 2. Execute queries against BigQuery
# 3. Extract metrics from SQL results
# 4. Generate risk report via Gemini
# 5. Save report to Supabase
# ============================================================

import json
from datetime import date, datetime
from dotenv import load_dotenv
from metrics_calculator import calculate_metrics, build_gemini_input
from prompt_loader import render_sql_generation_prompt, render_risk_analysis_prompt
from bigquery_client import run_queries
from gemini_client import generate_sql_queries, generate_risk_report
from supabase_client import save_report, get_recent_reports, save_supplier_risks

load_dotenv()


def extract_metrics(sql_results: dict) -> dict:
    """
    Extract key numeric metrics from SQL results.
    This is done in Python (not Gemini) for accuracy and stability.
    """
    metrics = {
        "untracked_rate": None,
        "untracked_rate_delta": None,
        "avg_order_value": None,
        "avg_order_value_delta": None,
        "avg_init_to_pickup_hours": None,
        "avg_pickup_to_delivery_hours": None,
        "overdue_unpickup_count": None,
    }

    # Untracked orders rate (overall average across all suppliers)
    if "untracked_orders" in sql_results:
        rows = sql_results["untracked_orders"]
        if rows and not isinstance(rows, dict):
            rates = [r.get("untracked_rate") for r in rows if r.get("untracked_rate") is not None]
            if rates:
                metrics["untracked_rate"] = round(sum(rates) / len(rates), 4)

    # Average order value (overall average across all suppliers)
    if "high_value_items" in sql_results:
        rows = sql_results["high_value_items"]
        if rows and not isinstance(rows, dict):
            values = [r.get("avg_order_value") for r in rows if r.get("avg_order_value") is not None]
            deltas = [r.get("avg_order_value_delta") for r in rows if r.get("avg_order_value_delta") is not None]
            if values:
                metrics["avg_order_value"] = round(sum(values) / len(values), 2)
            if deltas:
                metrics["avg_order_value_delta"] = round(sum(deltas) / len(deltas), 4)

    # Logistics timing
    if "logistics_timing" in sql_results:
        rows = sql_results["logistics_timing"]
        if rows and not isinstance(rows, dict):
            pickup_times = [r.get("avg_init_to_pickup_hours") for r in rows if r.get("avg_init_to_pickup_hours") is not None]
            delivery_times = [r.get("avg_pickup_to_delivery_hours") for r in rows if r.get("avg_pickup_to_delivery_hours") is not None]
            overdue = [r.get("overdue_unpickup_count") for r in rows if r.get("overdue_unpickup_count") is not None]
            if pickup_times:
                metrics["avg_init_to_pickup_hours"] = round(sum(pickup_times) / len(pickup_times), 2)
            if delivery_times:
                metrics["avg_pickup_to_delivery_hours"] = round(sum(delivery_times) / len(delivery_times), 2)
            if overdue:
                metrics["overdue_unpickup_count"] = sum(overdue)

    return metrics


def build_historical_context(recent_reports: list[dict]) -> str:
    """
    Build a summary of historical metrics to pass to Gemini
    for comparison context.
    """
    if not recent_reports:
        return "No historical data available. This is the first report."

    context = {
        "available_days": len(recent_reports),
        "date_range": {
            "from": recent_reports[-1]["report_date"],
            "to": recent_reports[0]["report_date"],
        },
        "historical_metrics": [
            {
                "date": r["report_date"],
                "overall_risk_level": r["overall_risk_level"],
                "untracked_rate": r["untracked_rate"],
                "avg_order_value": r["avg_order_value"],
                "avg_init_to_pickup_hours": r["avg_init_to_pickup_hours"],
                "overdue_unpickup_count": r["overdue_unpickup_count"],
            }
            for r in recent_reports[:7]  # Last 7 days for context
        ]
    }

    return json.dumps(context, indent=2, default=str)


def run_agent():
    """
    Main agent pipeline. Runs the full risk report generation.
    """
    today = date.today()
    print(f"\n{'='*50}")
    print(f"Running Risk Agent for {today}")
    print(f"{'='*50}\n")

    # date
    from bigquery_client import run_query

    # ── Step 1: Generate SQL queries via Gemini ──────────
    print("Step 1: Generating SQL queries...")
    sql_prompt = render_sql_generation_prompt()
    queries = generate_sql_queries(sql_prompt)

    # Print generated SQL for verification
    print("\n=== Generated SQL Queries ===")
    for q in queries:
        print(f"\n-- {q['id']} --")
        print(q['sql'])
    print("=== End of SQL Queries ===\n")

    # ── Step 2: Execute queries against BigQuery ─────────
    print("\nStep 2: Executing queries against BigQuery...")
    sql_results = run_queries(queries)

    carrier_data = sql_results.get("carrier_breakdown_analysis", [])
    order_data = sql_results.get("order_to_package_ratio", [])

    print(json.dumps(sql_results.get("high_value_items", [])[:1], indent=2, default=str))

    # print(f"carrier_breakdown_analysis key exists: {'carrier_breakdown_analysis' in sql_results}")
    # print(f"carrier rows count: {len(carrier_data) if isinstance(carrier_data, list) else 'not a list'}")
    # print(f"First carrier row: {carrier_data[0] if carrier_data else 'empty'}")

    # print(f"order_to_package_ratio key exists: {'order_to_package_ratio' in sql_results}")
    # print(f"order rows count: {len(order_data) if isinstance(order_data, list) else 'not a list'}")
    # print(f"First order row: {order_data[0] if order_data else 'empty'}")

    print(json.dumps(sql_results.get("high_value_items", [])[:1], indent=2, default=str))

    # ── Step 3: Extract numeric metrics from results ─────
    # print("\nStep 3: Extracting metrics from results...")
    # metrics = extract_metrics(sql_results)
    # print(f"  Metrics: {json.dumps(metrics, indent=2)}")

    print("\nStep 3: Extracting metrics and anomalies...")
    metrics = calculate_metrics(sql_results)
    gemini_input = build_gemini_input(sql_results, metrics)
    anomalies = gemini_input["anomalies"]
    print(f"  Metrics: {json.dumps(metrics, indent=2)}")
    print(f"  Issues detected: {gemini_input['total_issues_detected']}")

    # ── Step 4: Get historical context from Supabase ─────
    print("\nStep 4: Fetching historical context...")
    recent_reports = get_recent_reports(days=30)
    historical_context = build_historical_context(recent_reports)
    print(f"  Found {len(recent_reports)} historical reports")

    # ── Step 5: Generate risk report via Gemini ───────────
    print("\nStep 5: Generating risk report...")
    # analysis_prompt = render_risk_analysis_prompt(
    #     sql_results=json.dumps(sql_results, indent=2, default=str),
    #     historical_context=historical_context,
    # )
    print(f"  Gemini input size: {len(json.dumps(gemini_input, default=str))} characters")
    print(f"  Anomalies breakdown: { {k: len(v) for k, v in gemini_input['anomalies'].items()} }")

    analysis_prompt = render_risk_analysis_prompt(
    sql_results=json.dumps(gemini_input, indent=2, default=str),
    historical_context=historical_context,
    )   
    report = generate_risk_report(analysis_prompt)

    # ── Step 6: Save report to Supabase ───────────────────
    print("\nStep 6: Saving report to Supabase...")
    saved = save_report(
        report_date=today,
        overall_risk_level=report.get("overall_risk_level", "UNKNOWN"),
        executive_summary=report.get("executive_summary", ""),
        metrics=metrics,
        key_findings=report.get("key_findings", []),
        supplier_breakdown=report.get("supplier_breakdown", []),
        carrier_breakdown=report.get("carrier_breakdown", []),
        recommendations=report.get("recommendations", []),
        raw_sql_results=sql_results,
        gemini_full_report=report,
    )

    print(f"\n{'='*50}")
    print(f"✓ Report complete!")
    print(f"  Date: {today}")
    print(f"  Risk Level: {report.get('overall_risk_level')}")
    print(f"  Summary: {report.get('executive_summary')[:100]}...")
    print(f"  Report ID: {saved['id']}")
    print(f"{'='*50}\n")

    # Step 7: Saving supplier risks
    print("\nStep 7: Saving supplier risks...")
    save_supplier_risks(
        report_date=today,
        anomalies=anomalies,
        sql_results=sql_results
    )

    return saved


if __name__ == "__main__":
    run_agent()