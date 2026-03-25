# HealthData — Amazon Seller Health Risk Pipeline

Rule-based risk scoring for Amazon third-party sellers, with LLM-powered explanations for flagged accounts.

## What It Does

1. **Fetches** daily seller health snapshots from BigQuery (`customer_health_metrics`)
2. **Enriches** rows with supplier mapping and Payability status from Supabase
3. **Scores** each seller (0–10) using a deterministic rule engine — no LLM involvement in scoring
4. **Explains** high-risk sellers (score > 4) via OpenAI, listing which metrics are unhealthy
5. **Writes** results to Supabase (`health_daily_risk` + `consolidated_flagged_supplier_list`)
6. **Exports** a local `risk_output.json` file

## Project Structure

```
HealthData/
├── risk_pipeline.py                  # CLI entry point
├── requirements.txt
│
├── health_risk/
│   ├── cli.py                        # Argument parsing
│   ├── config.py                     # Settings from env vars
│   ├── bootstrap.py                  # Wires dependencies
│   ├── pipeline.py                   # Main orchestration: fetch → enrich → score → LLM → write
│   ├── enrichment.py                 # Joins BQ rows with Supabase supplier mapping
│   ├── filters.py                    # Exclude suspended/pending sellers
│   ├── flagged.py                    # Build consolidated flagged rows (metrics + reasons)
│   ├── export.py                     # JSON file export
│   ├── metrics_catalog.py            # 15 metric definitions (source columns, groups)
│   ├── utils.py                      # Helpers (clamp, iso, pct_to_ratio, etc.)
│   │
│   ├── scoring/
│   │   ├── engine.py                 # RiskScoreEngine — weighted formula, driver extraction
│   │   └── subscores.py              # Per-metric threshold → 0-10 scoring functions
│   │
│   ├── llm/
│   │   ├── __init__.py
│   │   └── high_risk_narrative.py    # OpenAI call for high-risk explanation text
│   │
│   └── repositories/
│       ├── bigquery.py               # BQ data access
│       └── supabase.py               # Supabase reads + upserts
│
└── tests/
    ├── conftest.py
    ├── test_scoring_engine.py
    ├── test_high_risk_narrative.py
    ├── test_cli_dry_run.py
    ├── test_cli_subprocess.py
    ├── test_filters.py
    └── test_utils.py
```

## Scoring Logic

### Metrics (15 total)

| Group | Metrics | Scoring |
|-------|---------|---------|
| **Outcome** (4) | Order Defect Rate, Chargeback Rate, A-to-Z Claim Rate, Negative Feedback Rate | Threshold ladder → 0/1/3/6/8-10 |
| **Operational** (6) | Late Shipment, Cancellation, Avg Response Hours, No Response >24h, Valid Tracking, On-Time Delivery | Threshold ladder → 0/3/6/9 |
| **Compliance** (5) | Product Safety, Authenticity, Policy Violation, Listing Policy, IP Status | Good=0, Fair/Watch=5, Bad=10 |

### Aggregation

```
outcome_score     = weighted sum (ODR 70%, Chargeback 15%, A-Z 10%, Feedback 5%)
operational_score = weighted sum (Late Ship 25%, rest 15% each)
compliance_score  = 0.7 × max + 0.3 × avg

base_risk  = 0.45 × outcome + 0.30 × operational + 0.20 × compliance + 0.05 × inactivity_penalty
risk_score = activity_gate × base_risk + (1 - activity_gate) × inactivity_penalty
```

Activity gate scales from 0.2 (no orders) to 1.0 (500+ orders in 60d).

### Risk Levels

| Score | Level |
|-------|-------|
| < 2 | Healthy |
| 2–4 | Watch |
| 4–7 | Risky |
| 7+ | Critical |

### Downstream Actions

- **score > 4** → written to `consolidated_flagged_supplier_list` + LLM explanation generated
- LLM output goes into the `reasons` column; flagged metrics go into `metrics` column

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SUPABASE_URL` | Yes | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Yes | Supabase service role key |
| `OPENAI_API_KEY` | For LLM | Enables high-risk narrative generation |
| `HEALTH_RISK_OPENAI_MODEL` | No | Default: `gpt-4o-mini` |
| `HEALTH_RISK_LLM_NARRATIVE` | No | `0` to disable LLM even with API key |
| `HEALTH_RISK_NARRATIVE_THRESHOLD` | No | Default: `4` (score must exceed this) |
| `HEALTH_RISK_LLM_MAX_WORKERS` | No | Default: `4` (concurrent LLM calls) |
| `HEALTH_RISK_STORE_LLM_IN_SUPABASE` | No | `1` to write LLM text into `health_daily_risk` |

## Usage

```bash
cd HealthData
pip install -r requirements.txt

# Dry run (no Supabase writes), latest date, no LLM
python risk_pipeline.py --dry-run --no-llm-narrative

# Production run for a specific date
export OPENAI_API_KEY="sk-..."
python risk_pipeline.py --report-date 2026-03-24

# Run latest date + previous 2 days
python risk_pipeline.py --days-back 2
```

## Testing

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -v --ignore=.pytest_local
```
