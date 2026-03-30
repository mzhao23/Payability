# Ship Tracking Risk Metric

Daily pipeline that evaluates supplier shipping behavior and produces a risk score (0–10) per supplier. Scores are written to Supabase and high-risk suppliers are surfaced for risk team review.

---

## Code Structure

```
risk_metric/
├── pipeline.py                  ← Main entry point. Orchestrates all steps.
│
├── config/
│   └── settings.py              ← All tunable parameters (SLA days, thresholds, BQ table, etc.)
│
├── core/
│   ├── llm_scorer.py            ← Computes untracked_score (Python) + calls LLM for final scoring
│   ├── bigquery_client.py       ← BigQuery connection and query runner
│   └── supabase_client.py       ← Supabase upsert client
│
├── metrics/
│   ├── metric_1_untracked.py    ← Runs untracked rate SQL, returns per-supplier rows
│   ├── metric_2_price.py        ← Runs price escalation SQL, returns per-supplier rows
│   └── metric_3_pickup_lag.py   ← Runs FedEx pickup lag SQL, returns per-supplier rows
│
├── queries/
│   ├── metric_1_untracked.sql   ← Untracked rate query (per supplier per carrier)
│   ├── metric_2_price.sql       ← Price escalation query (z-score vs 30d baseline)
│   └── metric_3_pickup_lag.sql  ← FedEx pickup lag query
│
├── prompts/
│   └── llm_risk_scorer.md       ← Authoritative LLM scoring rules and prompt
│
└── docs/
    └── agent_system.md          ← Architecture doc for the self-improving agent system
```

**Pipeline execution order (`pipeline.py`):**
1. Run Metric 1, 2, 3 queries against BigQuery
2. Write carrier-level daily untracked rates to `carrier_daily_untracked`
3. Group all metric rows by `supplier_key`
4. Run LLM scorer in parallel across all active suppliers (30 workers)
5. Backfill supplier names from BigQuery
6. Write scores to `ship_risk_scores`
7. Write high-risk suppliers (score ≥ 6) to `consolidated_flagged_supplier_list`

---

## Data Source

**Table:** `bigqueryexport-183608.dynamodb.app_production_tracking_hub_trackingLabels0BF56DC6_19QOKS1UQM9IN`

**Key characteristics:**
- CDC (Change Data Capture) table — multiple rows per package, deduplicated by latest `_sdc_sequence` per `sk`
- Soft deletes via `_sdc_deleted_at`
- Data refreshes twice daily at UTC 00:00 and 12:00 (primary batch at UTC 12:00)
- Pipeline runs at **UTC 14:00 (EST 9:00am)** to ensure completeness

**Carrier coverage (2026 data):**

| Carrier | init_timestamp | pickup_timestamp | Used in |
|---------|---------------|-----------------|---------|
| `FedEx` | ~94% | 100% of initialized | Metric 1 + 3 |
| `UPS` | ~93% | 0% | Metric 1 only |
| `USPS` | ~48% | 16% | Metric 1 (with caveat) |
| `FEDEX` (all-caps) | 0% | 0% | Ghost order detection |
| AMZN_US, OnTrac, others | 0% | 0% | Excluded |

> `FedEx` and `FEDEX` are two distinct entries in the data. The all-caps variant has zero timestamp coverage and is treated as a separate fraud signal (`FEDEX_UNACTIVATED`).

---

## Metrics

### Metric 1 — Untracked Order Rate

**What it measures:** The proportion of a supplier's orders with no carrier scan (`init_timestamp`) after a 3-day SLA buffer. A high rate means the supplier is not handing packages to the carrier.

**Logic:**
- An order is **untracked** if all its packages have no `init_timestamp` after `ship_sla_days = 3` days from `order_date`
- Computed daily per supplier per carrier
- Compared against the supplier's own 30-day rolling average (`diff = today_rate - rolling_avg_30d`)
- Also compared against the carrier-wide untracked rate on the same day to separate supplier-specific anomalies from systemic carrier issues

**Carriers scored:**

| Carrier | Signal type |
|---------|------------|
| `FEDEX` | Primary — high coverage, reliable |
| `UPS` | Primary — high coverage, reliable |
| `USPS` | Secondary — ~48% coverage; compared against carrier baseline |
| `FEDEX_UNACTIVATED` | Special — always 100% untracked; risk signal is order **volume**, not rate |

---

### Metric 2 — Price Escalation

**What it measures:** Whether a supplier's order values have spiked significantly compared to their own 30-day historical baseline.

**Logic:**
- `zscore`: z-score of today's average order value vs 30-day rolling mean/stddev of daily averages
- `max_zscore`: z-score of today's single highest-value order vs 30-day rolling mean/stddev of **daily max order values** (uses its own distribution, not the average's distribution)
- `total_cost` is order-level and repeated across packages — deduplicated by `order_id` before aggregation

**Key design decisions:**
- `zscore = NULL` when supplier has < 30 days of history — treated as neutral, not risky
- `max_zscore` uses the historical distribution of max order values (not daily averages), so it is not artificially inflated by a small standard deviation
- `max_zscore` is only meaningful when `order_count_today ≥ 5`

---

### Metric 3 — FedEx Pickup Lag

**What it measures:** Days between label creation (`init_timestamp`) and carrier pickup (`pickup_timestamp`) for FedEx shipments. A rising trend may indicate the seller is delaying handoff.

**Why FedEx only:** Only carrier with reliable `pickup_timestamp` data (100% coverage for initialized records).

**Logic:**
- `pickup_lag_days = DATE_DIFF(pickup_date, init_date, DAY)` per package
- Aggregated daily per supplier: `avg_pickup_lag`
- Compared against 30-day rolling average: `diff = avg_pickup_lag - rolling_avg_30d`

> **Metric 3B (Stuck Orders) was deprecated.** FedEx 2026 has 0 stuck orders; UPS/USPS have 400k+ but confirmed to be a data integration gap, not real delays.

---

## Risk Scoring

Scoring is split between **Python** (quantitative untracked signal) and **LLM** (qualitative interpretation + price/lag scoring). Suppliers with insufficient volume are skipped and defaulted to score 0.

**Minimum volume to score:** at least one carrier with ≥ 10 orders, OR price data with ≥ 5 orders.

---

### Step 1 — Python computes `untracked_score` (0–8)

For each carrier (FEDEX, UPS, USPS) with ≥ 10 orders on the target date:

```
confidence = min(0.5, exp(order_volume / 50) / exp(3))
carrier_score = untracked_rate × confidence × 30
```

- **confidence** is an exponential function of order volume, capped at **0.5**. The cap prevents over-confidence at very high volumes — a 30% untracked rate should not score the same as a 100% rate regardless of volume.
- Carrier scores are combined via **sqrt(sum of squares)** and capped at 8. This gives diminishing returns for multi-carrier stacking.

**Confidence reference:**

| Orders | Confidence | 100% rate score | 30% rate score |
|--------|-----------|----------------|----------------|
| 10 | 0.030 | 0.9 | 0.3 |
| 50 | 0.135 | 4.1 | 1.2 |
| 100 | 0.368 | 8 (cap) | 3.3 |
| 115+ | 0.5 (cap) | 8 (cap) | 4.5 |

---

### Step 2 — Python computes `price_weight`

Price signals carry more weight when untracked activity is already elevated:

| `untracked_score` | `price_weight` |
|-------------------|---------------|
| ≥ 3 | 1.0 |
| 1.5 – 3 | 0.6 |
| < 1.5 | 0.3 |

---

### Step 3 — LLM adjusts `untracked_score` for carrier-level systemic issues

The carrier-wide untracked rate for the same day is passed to the LLM. This distinguishes supplier-specific risk from carrier-wide outages or data gaps.

| Carrier baseline | Supplier vs carrier | Adjustment |
|-----------------|---------------------|------------|
| ≥ 50% | `supplier_rate - carrier_rate ≤ 15pp` | × 0.5 — systemic carrier issue |
| ≥ 50% | `supplier_rate - carrier_rate > 15pp` | × 1.0 — supplier meaningfully worse than carrier |
| 20–50% | `supplier_rate - carrier_rate ≤ 15pp` | × 0.75 — mixed signal |
| 20–50% | `supplier_rate - carrier_rate > 15pp` | × 1.0 — supplier-specific risk |
| < 20% | Supplier significantly higher | × 1.0 — clear supplier-specific risk |
| Absent | — | × 1.0 |

> The ≤ 15pp condition covers both cases where the supplier rate is below the carrier rate AND where it is only slightly above — both indicate a systemic rather than supplier-specific issue.

---

### Step 4 — LLM scores price escalation

Raw price score from `latest_zscore`:

| Z-score | Raw price score |
|---------|----------------|
| NULL or < 2.0 | +0 |
| 2.0 – 3.0 | +1 |
| 3.0 – 4.5 | +2 |
| > 4.5 | +3 |

**Special case — concealed high-value order:** If `latest_zscore < 0` (average below baseline) but `latest_max_zscore > 10` with ≥ 5 orders: +1. Indicates a single large order hidden among normal/low-price orders.

**Additional +1** if `latest_max_zscore` is much higher than `latest_zscore` with ≥ 5 orders and `max_zscore > 5.0`.

```
price_contribution = raw_price_score × price_weight
```

---

### Step 5 — LLM applies FedEx pickup lag adjustment (supporting signal only)

Only added when untracked or price is already elevated:
- `diff_vs_baseline > 2 days` → +1

---

### Final Score

```
overall_risk_score = (untracked_score × carrier_adjustment) + price_contribution + lag_adjustment
```

Capped at 10. Rounded to nearest integer.

**Score interpretation:**

| Score | Meaning |
|-------|---------|
| 0–2 | Normal behavior, no action needed |
| 3–4 | Weak signal, low volume or moderate rate — monitor only |
| 5–6 | Meaningful risk signal on at least one metric — flag for review |
| 7–8 | Strong signal, elevated untracked rate with supporting evidence |
| 9–10 | Critical risk — both untracked and price escalation significantly elevated |

---

## Output Tables

**`ship_risk_scores`** — one row per run per supplier:
- `overall_risk_score`: 0–10 integer
- `trigger_reason`: 2–3 sentences citing specific raw values (rate, volume, z-score)
- `metrics`: JSON snapshot of latest metric values
- `report_date`, `last_purchase_date`, `supplier_name`

**`consolidated_flagged_supplier_list`** — upserted, always reflects latest score:
- Suppliers with `overall_risk_score ≥ 6`

**`carrier_daily_untracked`** — carrier-level baseline written each run:
- Used by LLM scorer to contextualize supplier-level signals

---

## Configuration

All tunable parameters are in `config/settings.py`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ship_sla_days` | 3 | Days buffer before flagging order as untracked |
| `window_days` | 7 | Rolling window for untracked rate |
| `zscore_threshold` | 2.0 | Minimum z-score to flag price escalation |
| `stuck_days` | 5 | Days before flagging as stuck (FedEx only, deprecated) |
| `baseline_days` | 30 | Historical baseline period |

Authoritative LLM scoring rules: [`prompts/llm_risk_scorer.md`](./prompts/llm_risk_scorer.md)
