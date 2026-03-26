# HealthData — Amazon Seller Health Risk Pipeline

Rule-based risk scoring for Amazon third-party sellers, with LLM-powered narratives for high-risk accounts.

## What It Does

1. **Fetches** daily seller health snapshots from BigQuery (`customer_health_metrics`)
2. **Enriches** rows with supplier mapping (Supabase `suppliers` table) and Payability status (BigQuery `v_supplier_summary`)
3. **Filters** out suspended/pending sellers
4. **Scores** each seller (0–10) using a deterministic rule engine — no LLM involvement in scoring
5. **Explains** high-risk sellers (score > 6) via OpenAI: summary sentence + metric breakdown + recommendation
6. **Excludes** already-reviewed suppliers (Supabase `reviewed_suppliers` table) from the flagged list
7. **Writes** results to Supabase (`health_daily_risk` + `consolidated_flagged_supplier_list`)
8. **Exports** a local `risk_output.json` file

## Data Flow

```
BigQuery: customer_health_metrics
  │  (mp_sup_key + 15 health metrics)
  ▼
Supabase: suppliers table
  │  mp_sup_key → supplier_key, supplier_name
  ▼
BigQuery: v_supplier_summary
  │  supplier_key → payability_status
  ▼
Filter: remove suspended / pending
  ▼
RiskScoreEngine: threshold-based scoring (0–10)
  ▼
LLM Narrative: OpenAI generates explanation for score > 6
  ▼
Reviewed filter: skip suppliers in reviewed_suppliers table
  ▼
Write: health_daily_risk (all)  +  consolidated_flagged_supplier_list (score > 6, not reviewed)
```

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
│   ├── enrichment.py                 # Joins BQ rows with Supabase supplier mapping + payability status
│   ├── filters.py                    # Exclude suspended/pending sellers
│   ├── flagged.py                    # Build consolidated flagged rows (score > 6 threshold)
│   ├── export.py                     # JSON file export
│   ├── metrics_catalog.py            # 15 metric definitions (source columns, groups)
│   ├── utils.py                      # Helpers (clamp, iso, pct_to_ratio, normalize_key, etc.)
│   │
│   ├── scoring/
│   │   ├── engine.py                 # RiskScoreEngine — weighted formula, driver extraction
│   │   └── subscores.py              # Per-metric threshold → 0-10 scoring functions
│   │
│   ├── llm/
│   │   ├── __init__.py
│   │   └── high_risk_narrative.py    # OpenAI: summary + metric bullets + recommendation
│   │
│   └── repositories/
│       ├── bigquery.py               # BQ data access (health metrics + payability status)
│       └── supabase.py               # Supabase reads (mapping, reviewed) + upserts (risk, flagged)
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

## Scoring Logic (v3)

Pipeline version: `risk_formula_v3_production`

### Three Dimensions

| Dimension | Weight | Metrics |
|-----------|--------|---------|
| **Outcome** | 55% | Order Defect Rate, Chargeback Rate, A-to-Z Claim Rate, Negative Feedback Rate |
| **Operational** | 35% | Late Shipment, Pre-Fulfillment Cancel, Avg Response Hours, No Response >24h, Valid Tracking, On-Time Delivery |
| **Compliance** | 10% | Product Safety, Authenticity, Policy Violation, Listing Policy, Intellectual Property |

### Outcome Weights (within group)

| Metric | Weight |
|--------|--------|
| Order Defect Rate (60d) | 55% |
| Chargeback Rate (90d) | 18% |
| A-to-Z Claim Rate (90d) | 15% |
| Negative Feedback Rate (90d) | 12% |

### Operational Weights (within group)

| Metric | Weight |
|--------|--------|
| Late Shipment Rate (30d) | 25% |
| Pre-Fulfillment Cancel Rate (30d) | 20% |
| Valid Tracking Rate (30d) | 20% |
| On-Time Delivery Rate (30d) | 20% |
| Avg Response Hours (30d) | 8% |
| No Response >24h Count (30d) | 7% |

### Subscore Thresholds

Each metric is scored into tiers: **0 → 3 → 6 → 8 → 10**.

| Metric | 0 (OK) | 3 | 6 | 8 | 10 |
|--------|--------|---|---|---|-----|
| ODR | 0% | < 0.15% | < 0.4% | < 0.8% | ≥ 0.8% |
| Chargeback | 0% | < 0.02% | < 0.07% | < 0.15% | ≥ 0.15% |
| A-to-Z Claim | 0% | < 0.02% | < 0.1% | < 0.3% | ≥ 0.3% |
| Neg Feedback | 0% | < 0.15% | < 0.4% | < 1.2% | ≥ 1.2% |
| Late Shipment | < 0.8% | — | < 2% | < 4% | ≥ 4% |
| Cancel Rate | < 0.4% | — | < 1.5% | < 3% | ≥ 3% |
| Response Hours | < 5h | — | < 12h | < 24h | ≥ 24h |
| No Response | 0 | — | < 2 | < 6 | ≥ 6 |
| Valid Tracking | ≥ 98% | — | ≥ 97% | ≥ 94% | < 94% |
| On-Time Delivery | ≥ 97% | — | ≥ 95% | ≥ 91% | < 91% |
| Compliance Status | Good/OK | — | Fair/Watch: 7 | — | Other: 10 |

### Aggregation Formula

```
outcome_score     = weighted sum of outcome subscores
operational_score = weighted sum of operational subscores
compliance_score  = 0.7 × max(compliance subscores) + 0.3 × avg(compliance subscores)

base_risk  = 0.55 × outcome + 0.35 × operational + 0.10 × compliance
risk_score = activity_gate × base_risk + (1 - activity_gate) × inactivity_penalty
risk_score = clamp(risk_score, 0, 10)
```

### Activity Gate (order volume adjustment)

| 60-day Orders | Gate | Inactivity Penalty |
|---------------|------|--------------------|
| None / 0 | 0.40 | 5.0 |
| 1–24 | 0.60 | 3.0 |
| 25–99 | 0.80 | 0.0 |
| 100–499 | 0.95 | 0.0 |
| ≥ 500 | 1.00 | 0.0 |

### Risk Levels

| Score | Level |
|-------|-------|
| < 2 | Healthy |
| 2–4 | Watch |
| 4–7 | Risky |
| ≥ 7 | Critical |

### Downstream Actions

- **score > 6** → written to `consolidated_flagged_supplier_list` (unless in `reviewed_suppliers`)
- **score > 6** → LLM narrative generated (summary + metric breakdown + recommendation)
- LLM output goes into the `reasons` column; flagged metrics go into `metrics` column

## Supabase Tables

| Table | Purpose | Key |
|-------|---------|-----|
| `suppliers` | mp_sup_key → supplier_key mapping | mp_sup_key |
| `health_daily_risk` | Daily risk scores for all sellers | report_date + mp_sup_key |
| `consolidated_flagged_supplier_list` | High-risk suppliers for review | supplier_key + source |
| `reviewed_suppliers` | Suppliers already reviewed (excluded from flagged list) | supplier_key |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SUPABASE_URL` | Yes | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Yes | Supabase service role key |
| `OPENAI_API_KEY` | For LLM | Enables high-risk narrative generation |
| `HEALTH_RISK_OPENAI_MODEL` | No | Default: `gpt-4o-mini` |
| `HEALTH_RISK_LLM_NARRATIVE` | No | `0` to disable LLM even with API key |
| `HEALTH_RISK_NARRATIVE_THRESHOLD` | No | Default: `6` (score must exceed this) |
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
