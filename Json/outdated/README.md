# Risk Data Agent (JSON `data` column)

This agent analyzes seller risk from daily JSON payloads stored in a table `data` column.

It supports:
- File input (`.json`, `.jsonl`, `.csv`)
- Direct BigQuery input
- Daily trend scoring per seller (7-day baseline)

## Output

```json
{
  "table_name": "marketplace_ext_data",
  "supplier_key": "...",
  "report_date": "2026-03-04",
  "metrics": [
    {
      "metric_id": "ODR",
      "value": 1.7,
      "unit": "%",
      "explanation": "Order Defect Rate is 1.7%.",
      "risk_points": 2.5
    }
  ],
  "overall_risk_score": 6.3
}
```

`overall_risk_score` is normalized to **1-10**.

## Run with file input

```bash
python3 risk_agent.py \
  --input sample_input.json \
  --table-name marketplace_ext_data \
  --output sample_output.json
```

Default behavior writes results to `v1_output` when `--output` is not provided.

By default, it emits the **latest row per seller** (best for daily monitoring).
To emit all rows:

```bash
python3 risk_agent.py --input sample_input.json --all-rows
```

## Run with BigQuery

Prerequisites:
- `pip install google-cloud-bigquery`
- GCP auth configured (for example `gcloud auth application-default login`)

Example:

```bash
python3 risk_agent.py \
  --bq-table your-project.your_dataset.marketplace_ext_data \
  --bq-project your-project \
  --bq-days 45 \
  --table-name marketplace_ext_data \
  --output risk_output.json
```

Optional filter:

```bash
python3 risk_agent.py \
  --bq-table your-project.your_dataset.marketplace_ext_data \
  --bq-where "mp_sup_key = 'a401ac24-73a7-479e-8f91-a19f42154a6d'"
```

## Store output in Supabase

`risk_agent.py` reads Supabase credentials from `.env.supabase` (same folder as script), or from environment variables.

Required variables:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

Optional:
- `SUPABASE_TABLE` (default: `seller_risk_scores`)

Example:

```bash
export SUPABASE_URL="https://your-project-ref.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="your-service-role-key"

python3 risk_agent.py --to-supabase --supabase-table seller_risk_scores
```

Or use `.env.supabase`:

```bash
SUPABASE_URL=https://your-project-ref.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
SUPABASE_TABLE=seller_risk_scores
```

It inserts the analysis records through Supabase REST endpoint:
`/rest/v1/<supabase_table>`.

## Runtime output

`risk_agent.py` saves JSON results to the output file (default `v1_output`) and prints a short status line:

```text
Saved <N> records to v1_output
```

## Current scoring model

Final score:
- `total_points = max(0, sum(risk_points))`
- `overall_risk_score = min(10, round(1 + total_points / 6, 2))`

### Base (daily row) metrics and thresholds

- `SYSTEM_ERROR`
  - Any `Error`: `+9.0`
  - Contains `password is incorrect` or `not authorized`: `+9.5`
  - Contains `internal error`: `+7.0`

- `FEEDBACK_STAR_RATING` (from `feedback.Summary`)
  - `< 3.0`: `+8.0`
  - `< 3.5`: `+6.0`
  - `< 4.0`: `+4.0`
  - `< 4.5`: `+2.0`
  - `>= 4.5`: `+0`

- `NEGATIVE_FEEDBACK_30D` (from `feedback.Negative["30 days"]`)
  - `<= 5%`: `+0.5`
  - `<= 10%`: `+1.5`
  - `<= 20%`: `+3.0`
  - `<= 40%`: `+5.5`
  - `> 40%`: `+8.5`

- `NEG_FEEDBACK_RATE_ACCEL_30V60`
  - Uses feedback counts to compare:
    - `30d negative rate` vs `prior 60d negative rate` (computed from `90d - 30d`)
  - Only added when `delta = rate_30d - rate_prior_60d > 0`
  - Delta thresholds:
    - `<= 2pp`: `+0.8`
    - `<= 5pp`: `+1.8`
    - `<= 10pp`: `+3.2`
    - `<= 20pp`: `+5.0`
    - `> 20pp`: `+7.0`

- Account Performance Info metrics
  - `ODR`:
    - `<= 1%`: `+0.5`
    - `<= 2%`: `+2.5`
    - `<= 3%`: `+5.5`
    - `> 3%`: `+8.5`
  - `LSR`:
    - `<= 2%`: `+0.5`
    - `<= 4%`: `+2.5`
    - `<= 8%`: `+5.5`
    - `> 8%`: `+8.0`
  - `CANCEL_RATE`:
    - `<= 1%`: `+0.4`
    - `<= 2.5%`: `+2.0`
    - `<= 5%`: `+4.5`
    - `> 5%`: `+7.0`
  - `VTR`:
    - `<= 90%`: `+8.0`
    - `<= 95%`: `+5.0`
    - `<= 98%`: `+2.5`
    - `> 98%`: `+0.2`
  - `ON_TIME_DELIVERY`:
    - `<= 80%`: `+7.0`
    - `<= 90%`: `+4.0`
    - `<= 95%`: `+2.0`
    - `> 95%`: `+0.3`

- `NEG_FEEDBACK_COUNT`
  - Applied only if count `> 10`
  - `risk_points = min(6.0, 1.5 + count/15)`

- `POLICY_VIOLATION_LOAD`
  - Weighted sum from `policy_compliance`
  - Weights:
    - keys containing `intellectual property` or `authenticity`: `1.8`
    - keys containing `safety` or `restricted`: `1.6`
    - keys containing `regulatory`: `1.5`
    - otherwise: `1.0`
  - `risk_points = min(10.0, 2.0 + sqrt(weighted_sum)/2)`

- `SEVERE_NOTIFICATIONS`
  - Counts titles matching keywords: `urgent`, `at risk`, `deactivated`, `removed`, `suspended`, `action required`, `trademark`, `compliance`, `restricted products`, `policy warning`
  - `risk_points = min(9.0, 1.5 + 0.45 * severe_count)`

- `PERMISSION_STATUS`
  - If `permissions.status != approved`: `+7.5`

- `LOW_AVAILABLE_FUNDS_RATIO`
  - If `Funds Available / Total Balance < 0.2`: `+4.0`

- `ACCOUNT_LEVEL_RESERVE_AMOUNT`
  - Uses latest statement-level `Account Level Reserve`
  - Applied when absolute reserve `>= $5,000`
  - Thresholds:
    - `<= $10,000`: `+1.0`
    - `<= $25,000`: `+2.0`
    - `<= $50,000`: `+3.5`
    - `<= $100,000`: `+5.0`
    - `> $100,000`: `+6.5`

- `ACCOUNT_RESERVE_RECENT_SPIKE`
  - Needs at least 2 closed statements with reserves
  - Trigger when `recent >= 1.5 * typical` and `recent - typical >= $5,000`
  - `risk_points = min(4.0, 1.5 + (recent/typical - 1.0))`

- `LOAN_STATUS_BUFFER` (risk reducing)
  - If all `Closed Loans` are `PAID_OFF`: `-1.8`

### History/trend metrics (per `mp_sup_key`)

- `RISK_SCORE_SPIKE_7D` / `RISK_SCORE_RECOVERY_7D`
  - Compare current score to trailing 7-row average (requires at least 3 prior rows)
  - `delta >= 2.0`: `+3.0`
  - `delta >= 1.0`: `+1.5`
  - `delta <= -2.0`: `-1.0`

- `PERFORMANCE_TREND_DETERIORATION` (day-over-day)
  - Additive points:
    - ODR increase `>= 0.5pp`: `+1.5`
    - LSR increase `>= 1.0pp`: `+1.2`
    - Cancellation increase `>= 1.0pp`: `+1.0`
    - Delivered-on-time decrease `>= 1.0pp`: `+1.0`

- `POLICY_WARNINGS_INCREASE_DOD`
  - If weighted policy load rises by `>= 5` vs previous day
  - `risk_points = min(4.0, 1.2 + delta/8)`

- `POLICY_WARNINGS_INCREASE_7D`
  - If weighted policy load rises by `>= 8` vs trailing 7-row average
  - `risk_points = min(4.5, 1.5 + delta/10)`

- `ACCOUNT_RESERVE_INCREASE_DOD`
  - If latest reserve rises by `>= $10,000` vs previous day
  - `risk_points = min(4.5, 1.5 + delta/30000)`

- `ACCOUNT_RESERVE_INCREASE_7D`
  - If latest reserve is `>= 1.6x` trailing baseline and at least `$10,000` higher
  - `risk_points = min(4.5, 1.8 + (current/baseline - 1.0))`
