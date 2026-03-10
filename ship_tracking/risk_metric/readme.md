# Shipping Behavior Risk Metrics

## Data Source

**Table:** `app_production_tracking_hub_trackingLabels`

**Key characteristics:**
- CDC (Change Data Capture) table — multiple rows per package, deduplicated by latest `_sdc_sequence` per `sk`
- Soft deletes via `_sdc_deleted_at`
- Data updates twice daily at UTC 00:00 and 12:00, with the primary batch arriving at UTC 12:00
- Pipeline is scheduled to run at **UTC 14:00 (EST 9:00am)** daily to ensure data completeness

**Carrier coverage findings (2026 data):**

| Carrier | init_timestamp | pickup_timestamp | Usable For |
|---|---|---|---|
| FedEx (`carrier = 'FedEx'`) | ~94% | 100% of initialized | Metric 1 + 3 |
| UPS | ~93% | 0% | Metric 1 only |
| USPS | ~48% | 16% | Metric 1 (with caveat) |
| `FEDEX` (all caps) | 0% | 0% | Ghost order detection |
| AMZN_US, OnTrac, others | 0% | 0% | Excluded |

**Key discovery:** `carrier = 'FedEx'` and `carrier = 'FEDEX'` are two distinct data sources. The all-caps variant has zero timestamp coverage and is treated separately as a fraud signal (`FEDEX_UNACTIVATED`).

---

## Metric 1 — Untracked Order Rate

**What it measures:** The proportion of orders that have no `init_timestamp` (i.e. no record of the shipment being handed to a carrier) after a 3-day SLA buffer.

**Logic:**
- An order is considered **untracked** if ALL of its packages have no `init_timestamp` after 3 days from `order_date`
- Computed daily per supplier per carrier
- Compared against a 30-day rolling average baseline to surface anomalies (`diff = today - rolling_avg_30d`)

![carrier](./Summary.png)

**Carriers included:**

| carrier_normalized | Reason |
|---|---|
| `FEDEX` | High coverage, reliable signal |
| `UPS` | High coverage, reliable signal |
| `USPS` | Lower coverage (~48%), included for comparison against carrier baseline |
| `FEDEX_UNACTIVATED` | Always untracked_rate = 1.0; high order volume = strong fraud signal |

**Key design decisions:**
- Only orders older than `ship_sla_days = 3` are evaluated, to avoid flagging same-day orders that haven't shipped yet
- `diff` is compared against the carrier-level baseline on the same day to distinguish supplier-specific anomalies from systemic carrier issues
- `FEDEX_UNACTIVATED` is treated separately: `diff ≈ 0` and `untracked_rate = 1.0` are both expected and permanent for this carrier — the risk signal is **order volume**, not rate deviation

**2026 findings:**

| Carrier | Suppliers | Avg Untracked Rate | Avg Diff |
|---|---|---|---|
| FEDEX | 65 | 56.6% | +0.121 |
| FEDEX_UNACTIVATED | 138 | 100% | ~0 |
| UPS | 331 | 4.9% | +0.030 |
| USPS | 382 | 28.1% | +0.029 |

**Notable:** 138 suppliers have orders sitting in `FEDEX_UNACTIVATED` status — labels created but never activated in FedEx's system. This warrants further investigation as a potential fraud signal.

---

## Metric 2 — Price Escalation Detection

**What it measures:** Whether a supplier's average order value has spiked significantly compared to their own historical baseline.

**Logic:**
- Computes daily `avg_order_value` per supplier
- Calculates z-score against the supplier's own 30-day rolling mean and standard deviation
- Two signals: `zscore` (average daily price) and `max_zscore` (single highest-value order)
- `total_cost` is an order-level field repeated across packages, so it is deduplicated by `order_id` before aggregation

**Key design decisions:**
- Suppliers with fewer than 30 days of history or only 1 data point return `zscore = NULL` — treated as neutral, not risky
- `max_zscore` catches one-off large orders that would be diluted in the average
- `zscore >= 2.0` — average daily order value is significantly elevated (flags gradual price inflation)
- `max_zscore >= 5.0` — a single order is extremely far above the supplier's norm (flags one-off large suspicious orders that would be diluted in the daily average)


---

## Metric 3 — FedEx Pickup Lag (3A)

**What it measures:** The time between label creation (`init_timestamp`) and carrier pickup (`pickup_timestamp`) for FedEx shipments. A rising trend may indicate the seller is delaying handing off packages — a potential fraud signal.

**Why FedEx only:**
- FedEx is the only carrier with reliable pickup_timestamp data in 2026 (100% coverage for initialized records)
![carrier](./carrier.png)


**Logic:**
- `pickup_lag_days = DATE_DIFF(pickup_date, init_date, DAY)` per package
- Aggregated daily per supplier: `avg_pickup_lag`, `max_pickup_lag`
- Compared against 30-day rolling average: `diff = avg_pickup_lag - rolling_avg_30d`

**Note on Metric 3B (Stuck Orders):**
We initially designed a "stuck orders" metric for packages with `init` but no `pickup` after 5 days. This was deprecated because:
- FedEx 2026: 0 stuck orders (100% init-to-pickup sync) — metric produces no signal
- UPS/USPS: 400,000+ "stuck" orders, but confirmed to be a data integration gap, not real shipping delays


---

## Risk Scoring

After the three metrics are computed and stored in `supplier_daily_metrics`, an LLM-based scorer evaluates each supplier daily using the last 7 days of metric trends. 

`
You are evaluating the supplier's risk as of today (the most recent date in the data). The 7-day trend is provided as context to help you assess whether signals are improving or deteriorating.
`

**Inputs to LLM per supplier:**
- 7-day trend for price z-score
- 7-day trend for untracked rate per carrier (where applicable)
- 7-day trend for FedEx pickup lag (where applicable)
- FEDEX_UNACTIVATED order volume

**Output written to `supplier_risk_scores`:**
- `overall_risk_score`: integer 0–10 (higher = more dangerous)
- `trigger_reason`: natural language explanation of key risk signals
- `metrics`: snapshot of latest metric values

**Scoring guidance provided to LLM:**

| Score | Interpretation |
|---|---|
| 0–2 | Normal behavior, no action needed |
| 3–4 | Minor anomalies, worth monitoring |
| 5–6 | Meaningful risk signals present |
| 7–8 | Strong signals, multiple metrics elevated |
| 9–10 | Critical risk, strong fraud or default indicators |

---

## Pending

- The thresholds need to confrimed.
- We right now use llm to evaluate the risk per supplier everyday. Maybe it will miss the overall trend.