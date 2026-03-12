# Supplier Risk Analysis Agent

An AI-powered pipeline that fetches Amazon seller data from BigQuery,
analyses risk using rule-based scoring + Claude AI, and writes structured
risk reports to Supabase.

## Project Structure

```
risk_agent/
├── main.py                        # Entry point — runs the full pipeline
├── requirements.txt
├── .env.example                   # Copy to .env and fill in your values
├── supabase_migration.sql         # Run once in Supabase SQL editor
│
├── input/                         # Auto-saved BQ snapshots for regression testing
│   └── 2026-03-10.json            # One file per date, named by report date
│
├── config/
│   ├── settings.py                # Env-var loader
│   └── models.py                  # Pydantic output models (RiskReport, Metric)
│
├── extractors/
│   ├── bq_loader.py               # BigQuery → two-query fetch + local file loader
│   └── feature_extractor.py      # JSON data column → FeatureSet dataclass
│
├── scoring/
│   └── rule_scorer.py             # Deterministic rule-based pre-scorer
│
├── agent/
│   └── claude_agent.py           # Claude API call + response parser
│                                  # (swap _call_llm() for Vertex AI / Gemini)
│
├── output/
│   └── supabase_writer.py        # Upsert RiskReport → Supabase
│
├── utils/
│   └── logger.py                  # Shared logger
│
└── tests/
    └── test_pipeline.py           # Offline unit tests (no cloud needed)
```

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env and fill in all values
```

### 3. Set up Supabase table
Run `supabase_migration.sql` in your Supabase SQL editor.

### 4. Run the pipeline
```bash
# Fetch latest data from BigQuery (default)
python main.py

# Fetch a specific date from BigQuery (saves snapshot to input/YYYY-MM-DD.json)
python main.py --date 2026-03-10

# Re-run using a saved local snapshot (no BQ cost)
python main.py --source local

# Re-run a specific date from local snapshot
python main.py --source local --date 2026-03-10

# Re-run using a specific local file
python main.py --source local --input-file input/2026-03-10.json
```

### 5. Run tests (offline, no cloud required)
```bash
python -m pytest tests/ -v
```

## Environment Variables

| Variable | Description |
|---|---|
| `ANTHROPIC_API_KEY` | Claude API key |
| `ANTHROPIC_MODEL` | Model to use (default: `claude-haiku-4-5-20251001`) |
| `BQ_SERVICE_ACCOUNT_PATH` | Path to GCP service account JSON file |
| `BQ_SERVICE_ACCOUNT_JSON` | Inline GCP service account JSON (alternative to path) |
| `BQ_PROJECT_ID` | GCP project ID |
| `BQ_DATASET` | BigQuery dataset name |
| `BQ_TABLE` | BigQuery table name |
| `BQ_LOOKBACK_HOURS` | Only process rows newer than N hours (0 = all rows) |
| `SUPABASE_URL` | Your Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase service role key |
| `SUPABASE_RISK_TABLE` | Target table name (default: `json_risk_report`) |
| `MAX_ROWS` | Max rows per run (0 = no limit) |
| `DRY_RUN` | If `true`, skips writing to Supabase |
| `LOG_LEVEL` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `PIPELINE_WORKERS` | Concurrent threads (default: 5, max ~10 before rate limits) |

## BigQuery Queries

The pipeline runs **two queries per run** to keep costs low:

1. **Main query** — fetches one row per `mp_sup_key` for the target window (deduped via `ROW_NUMBER() ORDER BY create_ts DESC`), with all columns needed for feature extraction
2. **Prev policy query** — for each `mp_sup_key` in the current window, fetches the summed `policy_compliance` total from the immediately preceding record. Only scalar values are extracted in BQ, keeping this query lightweight.

The prev policy total is injected into each row as `prev_policy_total` before feature extraction, enabling cross-period compliance trend detection.

### Regression Testing / Local Snapshots

Every BQ run automatically saves the fetched rows to `input/<date>.json`. This allows you to:
- Re-run the pipeline on historical data without hitting BigQuery
- Test rule changes against the same input data
- Debug issues on a specific day's dataset

## Scoring Mechanism

Every supplier starts at a base score of **1**. The rule engine applies two types of rules:

### Hard Rules — set a score floor (minimum)
Hard rules represent clear, directional risk signals. Each hard rule sets the score to *at least* its floor value. Multiple hard rules can fire simultaneously — the highest floor wins.

| Rule | Trigger | Floor |
|---|---|---|
| `ACCOUNT_STATUS` | Account not OK or Active | 8 |
| `LOAN_PAST_DUE` | Past-due loan amount > $0 | 9 |
| `ORDER_DEFECT_RATE` | Seller-fulfilled ODR > 1% (from Performance Over Time SF rows only; FBA-only or no SF data → skipped) | 8 |
| `LATE_SHIPMENT_RATE` | LSR > 4% (Amazon red line) | 8 |
| `NEG_FEEDBACK_TREND` | 30d neg rate ≥ 10pp above 60d window (min 10 orders) | 7 |
| `POLICY_COMPLIANCE_INCREASE` | Total policy violations up ≥ 5 vs previous BQ record | 7 |
| `ACCOUNT_LEVEL_RESERVE` | Negative reserve in ≥ 2 consecutive statement periods | 7 |
| `ACCOUNT_LEVEL_RESERVE` | Single-period negative reserve > $5,000 | 7 |
| `FAILED_DISBURSEMENT` | ≥ 2 cancelled/failed payout transfers within 90 days | 7 |
| `FAILED_DISBURSEMENT` | 1 cancelled/failed payout transfer within 90 days | 6 |

### Soft Rules — additive penalty points
Soft rules add penalty points to the score. They represent weaker signals that are only meaningful in combination.

| Category | Rule | Condition | Points |
|---|---|---|---|
| Fulfillment | Cancellation rate | > 2.5% / 1.5–2.5% | +2 / +1 |
| Fulfillment | Valid tracking rate | < 95% | +2 |
| Fulfillment | Delivered on time | < 85% | +1 |
| Fulfillment | Two-step verification | Inactive | +1 |
| Feedback | 30d negative rate | > 10% (min 10 orders) | +2 |
| Feedback | 30d negative rate | 5–10% (min 10 orders) | +1 |
| Loans | Outstanding balance | > $0 | +1 |
| Policy | Total violations | ≥ 20 / 5–19 | +2 / +1 |
| Policy | Violations increase | +2 to +4 vs prior record | +1 |
| Notifications | High-risk notifications | ≥ 10 / 5–9 / 2–4 | +2 / +2 / +1 |
| Payout | Deferred transactions | ≥ 50% and > $5,000 | +1 |
| Payout | Reserve | 1 period negative | +1 |
| Payout | Reserve | Worsening across periods | +1 |
| Payout | Unavailable balance (most recent statement only) | ≥ $1,000 | +1 |
| Account Health | Defect rate (no WoW data) | ≥ 1% / 0.5–1% | +2 / +1 |
| Complaints | Authenticity/Safety/IP/Policy | Each > 0 | +1 each |

### Scoring Formula

Scores are **floats** (e.g. 8.33, 9.17) rounded to 2 decimal places.

**Hard rules fired:**
```
final = min(10, max_floor + sum(other_floors) / 6 + min(soft_penalty, 6) / 6)
```
- `max_floor` — highest fired hard rule floor
- `sum(other_floors) / 6` — every additional hard rule contributes its floor divided by 6
- `min(soft_penalty, 6) / 6` — soft signals contribute at most +1 on top of hard rules

**No hard rules (soft only):**
```
final = min(6.0, 1 + soft_penalty)
```
- Pure soft path maxes out at **6.0** — by design, never exceeds the lowest possible hard rule floor (6)

**Examples:**

| Case | Calculation | Score |
|---|---|---|
| Single floor 8, no soft | 8 + 0 + 0 | 8.0 |
| Single floor 8, 2 soft | 8 + 0 + 2/6 | 8.33 |
| Floor 8 + floor 6, no soft | 8 + 6/6 + 0 | 9.0 |
| Floor 8 + floor 6, 2 soft | 8 + 6/6 + 2/6 | 9.33 |
| Floor 9 + floor 8, no soft | 9 + 8/6 + 0 | 10.0 (capped) |
| Soft only, 5 signals | 1 + 5 | 6.0 |
| Soft only, 10 signals | 1 + 10 → capped | 6.0 |

### FBA vs Seller-Fulfilled ODR
ODR is only evaluated using seller-fulfilled data from the `Performance Over Time` section:
- **FBA-only sellers** (no seller-fulfilled orders): ODR skipped entirely
- **Mixed or self-ship sellers with SF data**: uses `seller_fulfilled_odr` from Performance Over Time SF rows
- **No SF data available**: ODR skipped — no fallback to global ODR field

### Data Quality Short-Circuit
If the data collection returned an error flag, the supplier is scored directly — no rules evaluated, no LLM called:

| Flag | Score |
|---|---|
| `not_authorized` | 8 |
| `login_error` / `wrong_password` | 7 |
| `bank_page_error` | 5 |
| `internal_error` / `json_parse_error` | 4 |
| `advance_only` / `onboarding_only` | 2 |

### LLM Usage
Claude AI is only called when the rule engine pre-score is **≥ 5**. Rows scoring below 5 receive a rule-engine-only report. The LLM may adjust the final score up or down from the pre-score based on contextual analysis.

## Risk Score Guide

| Score | Level | Description |
|---|---|---|
| 1–2 | Very Low | Healthy metrics, no compliance issues |
| 3–4 | Low | Minor issues, within Amazon thresholds |
| 5–6 | Moderate | Approaching thresholds, some violations |
| 7–8 | High | Breaching thresholds, active violations |
| 9–10 | Critical | Account suspension risk, past-due loans |

## Output Schema

```json
{
  "table_name": "project.dataset.table",
  "supplier_key": "uuid",
  "mp_sup_key": "uuid",
  "supplier_name": "Seller Name",
  "report_date": "2026-03-10",
  "metrics": [
    {"metric_id": "order_defect_rate", "value": 0.13, "unit": "%"},
    {"metric_id": "feedback_negative_30d", "value": 18.5, "unit": "%"},
    {"metric_id": "feedback_negative_trend_delta", "value": 12.3, "unit": "pp"},
    {"metric_id": "policy_compliance_total", "value": 14, "unit": null},
    {"metric_id": "policy_compliance_delta", "value": 6, "unit": null},
    {"metric_id": "stmt_reserve_consecutive_negative", "value": 3, "unit": "periods"},
    {"metric_id": "stmt_reserve_max_negative", "value": 8200.0, "unit": "USD"},
    {"metric_id": "failed_disbursement_count", "value": 2, "unit": null}
  ],
  "trigger_reason": "Negative feedback rate has increased 12.3pp in the last 30 days vs the prior 60-day window, and policy compliance violations have risen by 6 vs the previous record. Account level reserve has been negative for 3 consecutive statement periods.",
  "overall_risk_score": 8.33
}
```

## Supabase Table Setup

Run this in Supabase SQL editor:

```sql
CREATE TABLE IF NOT EXISTS json_risk_report (
    id                  BIGSERIAL PRIMARY KEY,
    table_name          TEXT,
    supplier_key        TEXT,
    mp_sup_key          TEXT,
    supplier_name       TEXT,
    report_date         DATE,
    metrics             JSONB,
    trigger_reason      TEXT,
    overall_risk_score  NUMERIC(5,2),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (supplier_key, report_date)
);

CREATE INDEX IF NOT EXISTS idx_json_risk_report_mp_sup_key ON json_risk_report (mp_sup_key);
CREATE INDEX IF NOT EXISTS idx_json_risk_report_report_date ON json_risk_report (report_date);
CREATE INDEX IF NOT EXISTS idx_json_risk_report_score ON json_risk_report (overall_risk_score);
```

To clear all records and reset IDs:
```sql
TRUNCATE TABLE json_risk_report RESTART IDENTITY;
```

## Tooling

**`export_reports.py`** — Download risk reports from Supabase to a local JSON file:
```bash
python export_reports.py                        # all reports
python export_reports.py --date 2026-03-10      # specific date
python export_reports.py --limit 100            # latest 100
python export_reports.py --output my_file.json  # custom filename
```

**`sync_suppliers.py`** — Sync unique suppliers from BQ to a `suppliers` table. Tracks field changes in an append-only `notes` column:
```bash
python sync_suppliers.py                # last 3 days
python sync_suppliers.py --days 7       # last 7 days
python sync_suppliers.py --dry-run      # preview without writing
python sync_suppliers.py --print-migration  # print setup SQL
```

## Swapping AI Provider (Future: Vertex AI / Gemini)

Only one function needs to change — `_call_llm()` in `agent/claude_agent.py`:

```python
# Current: Claude
def _call_llm(user_message: str) -> str:
    response = _client.messages.create(...)
    return response.content[0].text

# Future: Vertex AI / Gemini
def _call_llm(user_message: str) -> str:
    model = GenerativeModel("gemini-2.0-flash-001")
    response = model.generate_content([_SYSTEM_PROMPT, user_message])
    return response.text
```

The system prompt, feature extraction, rule scoring, and output writing
are all provider-agnostic.