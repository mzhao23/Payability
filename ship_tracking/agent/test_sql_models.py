# ============================================================
# test_sql_models.py
# Tests SQL generation quality across different models.
# Metrics:
#   1. Dry run pass rate (syntax check)
#   2. Judge score (business logic vs risk matrix)
# Judge: anthropic/claude-sonnet-4 
# ============================================================

import json
import time
from datetime import datetime
from dotenv import load_dotenv
from openai import OpenAI
from google.cloud import bigquery
import os

from prompt_loader import render_sql_generation_prompt
from bigquery_client import get_client as get_bq_client

load_dotenv()

# ── Model Configuration ───────────────────────────────────
MODELS_TO_TEST = [
    "google/gemini-2.0-flash", # $0.15/M $0.60/M
    "google/gemini-2.5-flash", # $0.30/M $2.50/M
    "anthropic/claude-haiku-4.5", # $1.00/M $5.00/M
    "anthropic/claude-sonnet-4.5", # $3.00/M $15.00/M
    "openai/gpt-4o-mini", # $0.40/M $1.60/M
    "openai/gpt-4o", # $2.50/M $10.00/M
]

JUDGE_MODEL = "anthropic/claude-sonnet-4"
GATEWAY_URL = "https://ai-gateway.vercel.sh/v1"


# ── Gateway Client ────────────────────────────────────────
def get_gateway_client() -> OpenAI:
    return OpenAI(
        api_key=os.environ.get("AI_GATEWAY_API_KEY"),
        base_url=GATEWAY_URL
    )


# ── Helpers ───────────────────────────────────────────────
def clean_response(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1]
        raw = raw.rsplit("```", 1)[0]
    return raw.strip()


# ── SQL Generation ────────────────────────────────────────
def generate_sql_with_model(model: str, prompt: str) -> list[dict]:
    """Generate SQL queries using a specific model."""
    client = get_gateway_client()

    response = client.chat.completions.create(
        model=model,
        temperature=0.1,
        max_tokens=20000,
        messages=[{"role": "user", "content": prompt}]
    )

    raw = response.choices[0].message.content
    cleaned = clean_response(raw)
    parsed = json.loads(cleaned)
    return parsed["queries"]


# ── Dry Run ───────────────────────────────────────────────
def dry_run_queries(queries: list[dict]) -> dict:
    """
    Dry run each SQL query against BigQuery (syntax check only).
    Returns pass/fail per query and overall pass rate.
    """
    bq_client = get_bq_client()
    results = []

    for query in queries:
        query_id = query["id"]
        sql = query["sql"]

        try:
            job_config = bigquery.QueryJobConfig(dry_run=True)
            bq_client.query(sql, job_config=job_config)
            results.append({
                "id": query_id,
                "passed": True,
                "error": None
            })
        except Exception as e:
            # Extract just the meaningful error message
            error_msg = str(e)
            results.append({
                "id": query_id,
                "passed": False,
                "error": error_msg
            })

    passed = sum(1 for r in results if r["passed"])
    total = len(results)

    return {
        "passed": passed,
        "total": total,
        "pass_rate": round(passed / total, 2) if total > 0 else 0,
        "results": results
    }


# ── Judge Evaluation ──────────────────────────────────────
def judge_sql_quality(queries: list[dict], dry_run_result: dict) -> dict:
    """
    Use judge model to evaluate SQL against the risk matrix.
    Focuses on business logic correctness, not just syntax.
    """
    client = get_gateway_client()

    judge_prompt = f"""
You are an expert BigQuery SQL reviewer for a fintech risk team.
Evaluate whether these SQL queries correctly implement the required risk metrics.

## Table Schema
- tracking_id (STRING)
- init_timestamp (STRING): label creation time, cast with SAFE.PARSE_TIMESTAMP
- pickup_timestamp (STRING): carrier pickup time, NULL means not picked up yet
- delivery_timestamp (STRING): delivery time, NULL means not delivered yet
- order_date (STRING): date order was placed, cast before date arithmetic
- total_cost (STRING): order value in USD, cast with SAFE_CAST(total_cost AS FLOAT64)
- supplier_key (STRING): merchant identifier, always group by this
- order_id (STRING): one order can have multiple packages, deduplicate for $ calculations
- package_id (STRING): individual package identifier
- carrier (STRING): shipping carrier, Amazon has no tracking - exclude from timing analysis
- _sdc_deleted_at (STRING): soft delete flag, always filter IS NULL

## Required Risk Matrix
1. **untracked_orders**: % of orders without tracking per supplier, vs 30-day historical avg
2. **high_value_items**: avg order value per supplier (deduplicated at order_id), 7-day vs 30-day avg
3. **logistics_timing**: init→pickup and pickup→delivery hours per supplier, vs historical avg. Flag overdue (>48hrs no pickup). Exclude Amazon carrier.
4. **carrier_breakdown**: same timing metrics but broken down by carrier. Identify systemic carrier issues vs supplier issues.
5. **order_package_ratio**: avg packages per order per supplier, vs historical avg

## Mandatory Rules
1. Filter _sdc_deleted_at IS NULL
2. Cast timestamps with SAFE.PARSE_TIMESTAMP
3. Cast total_cost with SAFE_CAST(total_cost AS FLOAT64)
4. Deduplicate at order_id level for dollar calculations
5. Exclude carrier = 'Amazon' from timing anomaly calculations
6. Group by supplier_key
7. Include CURRENT_DATE() AS report_date
8. Compare today vs historical baseline (30-day avg)

## Dry Run Results
{json.dumps(dry_run_result["results"], indent=2)}

## SQL Queries to Evaluate
{json.dumps(queries, indent=2)}

## Instructions
Evaluate each query against the risk matrix and mandatory rules.
Return a JSON object only, no markdown, no backticks, no code fences:

{{
  "overall_score": 0-100,
  "summary": "2 sentence overall assessment",
  "queries": [
    {{
      "id": "query_id",
      "syntax_passed": true/false,
      "covers_risk_requirement": true/false,
      "business_logic_correct": true/false,
      "rules_compliant": true/false,
      "score": 0-100,
      "issues": ["specific issue 1", "specific issue 2"]
    }}
  ]
}}
"""

    response = client.chat.completions.create(
        model=JUDGE_MODEL,
        temperature=0,
        max_tokens=2000,
        messages=[{"role": "user", "content": judge_prompt}]
    )

    raw = response.choices[0].message.content
    cleaned = clean_response(raw)
    return json.loads(cleaned)


# ── Test Single Model ─────────────────────────────────────
def test_model(model: str, prompt: str) -> dict:
    """Run full evaluation for a single model."""
    print(f"\n{'─'*50}")
    print(f"Testing: {model}")
    print(f"{'─'*50}")

    result = {
        "model": model,
        "sql_generated": False,
        "dry_run_pass_rate": 0,
        "judge_score": 0,
        "total_time_seconds": 0,
        "error": None,
    }

    start_time = time.time()

    # Step 1: Generate SQL
    try:
        queries = generate_sql_with_model(model, prompt)
        result["sql_generated"] = True
        result["queries"] = queries
        print(f"  ✓ SQL generated ({len(queries)} queries)")
    except Exception as e:
        result["error"] = str(e)
        print(f"  ✗ SQL generation failed: {e}")
        result["total_time_seconds"] = round(time.time() - start_time, 2)
        return result

    # Step 2: Dry run
    dry_run = dry_run_queries(queries)
    result["dry_run_pass_rate"] = dry_run["pass_rate"]
    result["dry_run_details"] = dry_run["results"]

    print(f"  Dry run: {dry_run['passed']}/{dry_run['total']} passed ({dry_run['pass_rate']*100:.0f}%)")
    for r in dry_run["results"]:
        status = "✓" if r["passed"] else "✗"
        error = f" → {r['error']}" if r["error"] else ""
        print(f"    {status} {r['id']}{error}")

    # Step 3: Judge evaluation
    try:
        judge = judge_sql_quality(queries, dry_run)
        result["judge_score"] = judge["overall_score"]
        result["judge_details"] = judge
        print(f"  Judge score: {judge['overall_score']}/100")
        print(f"  Summary: {judge['summary']}")
    except Exception as e:
        print(f"  ✗ Judge evaluation failed: {e}")

    result["total_time_seconds"] = round(time.time() - start_time, 2)
    return result


# ── Main Test Runner ──────────────────────────────────────
def run_tests():
    print("=" * 60)
    print("SQL Generation Model Evaluation")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Models: {len(MODELS_TO_TEST)}")
    print(f"Judge: {JUDGE_MODEL}")
    print("=" * 60)

    prompt = render_sql_generation_prompt()
    all_results = []

    for model in MODELS_TO_TEST:
        try:
            result = test_model(model, prompt)
            all_results.append(result)
        except Exception as e:
            print(f"\n✗ {model} failed completely: {e}")
            all_results.append({
                "model": model,
                "sql_generated": False,
                "dry_run_pass_rate": 0,
                "judge_score": 0,
                "error": str(e),
            })
        time.sleep(2)

    # ── Print Summary ─────────────────────────────────────
    print(f"\n{'='*60}")
    print("RESULTS SUMMARY")
    print(f"{'='*60}")
    print(f"{'Model':<40} {'SQL Gen':<10} {'Dry Run':<12} {'Judge':<10} {'Time'}")
    print(f"{'─'*40} {'─'*10} {'─'*12} {'─'*10} {'─'*8}")

    for r in all_results:
        sql_gen = "✅" if r.get("sql_generated") else "❌"
        dry_run = f"{r.get('dry_run_pass_rate', 0)*100:.0f}%"
        judge = f"{r.get('judge_score', 0)}/100"
        t = f"{r.get('total_time_seconds', 0)}s"
        print(f"{r['model']:<40} {sql_gen:<10} {dry_run:<12} {judge:<10} {t}")

    # Save full results
    output_path = "test_results.json"
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\nFull results saved to {output_path}")


if __name__ == "__main__":
    run_tests()