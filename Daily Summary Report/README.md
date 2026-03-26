# Supplier Risk Engine — Daily Summary Report

> **Policy version: v4.2.0**
> This document describes the complete risk scoring logic. It is intended to be
> read by both humans and AI agents to understand how suppliers are flagged and
> scored.



## Supplier Universe

Only suppliers with `payability_status = 'Active'` in `v_supplier_summary` are
evaluated. A supplier's latest row is only used if their current liability is
at least **$100** — this filters out dormant accounts with no real exposure.

---

## Data Inputs (from BigQuery)

| Field | Description |
|---|---|
| `today_receivable` | Current receivables |
| `prev_receivable` | Previous period receivables |
| `today_liability` | Current liability (must be ≥ $100 to be included) |
| `today_net_earning` | Net earning = receivables − chargebacks |
| `today_chargeback` | Chargeback amount today |
| `today_available_balance` | Cash available to the supplier |
| `today_outstanding_bal` | Total outstanding exposure |
| `today_due_from_supplier` | Amount supplier owes back |
| `prev_due_from_supplier` | Previous period due from supplier |
| `trailing_median_receivable` | Median receivable over trailing 6 periods |
| `trailing_median_chargeback` | Median chargeback over trailing 6 periods |
| `negative_net_earning_streak` | Consecutive periods where net earning was negative (up to last 3) |
| `recent_3_net_earning_sum` | Sum of net earning across the most recent 3 periods |
| `last_activation_date` | Most recent activation or reactivation date |
| `days_since_last_activation` | Days since the most recent activation |
| `has_payment_since_activation` | Whether any marketplace payment has occurred since last activation |
| `days_since_last_marketplace_payment` | Days since last positive marketplace payment |
| `historical_median_payment_gap_days` | Supplier's typical payment gap |
| `transaction_records_last_21d` | Number of transaction records in last 21 days |
| `days_since_latest_transaction` | Days since the most recent transaction |
| `receivable_down_streak_3` | How many of the last 3 periods showed declining receivables (0, 2, or 3) |

---

## Metric 1 — RECEIVABLE_SURGE

**What it measures:** Whether receivables have jumped sharply upward compared
to both the prior period (week-over-week) and the supplier's own history.

**Absolute gates (both must pass):**

| Severity | Today's receivable ≥ | Delta vs prev ≥ |
|---|---|---|
| MEDIUM | $3,000 | $2,000 |
| HIGH / CRITICAL | $5,000 | $2,000 |

**Severity levels (WoW % AND vs trailing median must both trigger):**

| Severity | WoW Change | vs Trailing Median | Score Contribution |
|---|---|---|---|
| MEDIUM | ≥ 50% | ≥ 2.0x | ~45% of weight (weight = 8) |
| HIGH | ≥ 100% | ≥ 4.0x | ~70% of weight |
| CRITICAL | ≥ 200% | ≥ 8.0x | ~90% of weight |

**Key insight:** A receivable surge only matters if the dollar amounts are
meaningful. A $50 spike on a $200 account will not trigger this rule even if
the percentage change is large.

---

## Metric 2 — RECEIVABLE_DROP

**What it measures:** Whether receivables are declining sharply or showing a
sustained downward trend. This is a new metric in v4.2.0 — it was not present
in previous versions.

**Two independent trigger paths:**

**Path A — Sharp single-period drop:**

| Severity | Condition |
|---|---|
| MEDIUM | WoW drop ≥ 50% (prev receivable must be ≥ $5,000) |
| HIGH | WoW drop ≥ 70% AND current receivable ≤ 0.6x trailing median (median ≥ $3,000) |

**Path B — Sustained downward streak:**

| Severity | Condition |
|---|---|
| MEDIUM | Down for 2 of last 3 periods AND trailing median ≥ $3,000 |
| HIGH | Down for all 3 of last 3 periods AND current ≤ 0.8x trailing median |

**Score contributions:**

| Severity | Score Contribution |
|---|---|
| MEDIUM | ~60% of weight (weight = 10) |
| HIGH | 100% of weight |

**Key insight:** A receivable drop signals declining cash flow, reducing the
supplier's ability to repay outstanding exposure. Unlike a surge (growing
liability risk), a drop is a forward-looking collection risk.

---

## Metric 3 — MARKETPLACE_PAYMENT_DELAY

**What it measures:** Whether the supplier's marketplace payment is overdue.

This metric is **activation-aware** — the measurement basis depends on
whether a payment has been received since the supplier's most recent
activation date.

### Eligibility Gate (must pass before any delay is assessed)

The supplier must show **recent sustained transaction activity**:
- At least **2 transaction records** in the last 21 days, AND
- Most recent transaction was within the last **14 days**

If these conditions are not met, the delay check is skipped entirely.

### Two Measurement Modes

**Mode A — No payment since activation:**
Used when the supplier has been active (or reactivated) but has not yet
received any marketplace payment in the current cycle. Delay is measured
from the **activation date**.

**Mode B — Payment already received since activation:**
Used when at least one payment has arrived in the current cycle. Delay is
measured from the **last marketplace payment date**.

**Severity thresholds (same for both modes):**

| Severity | Days Elapsed | Score Contribution |
|---|---|---|
| MEDIUM | > 21 days | ~45% of weight (weight = 12) |
| HIGH | > 28 days | ~70% of weight |
| CRITICAL | > 35 days | 100% of weight |

**Key insight:** The activation-aware logic prevents false positives for
newly reactivated suppliers. A supplier reactivated 10 days ago is not
"delayed" — they just haven't had time to receive a payment yet. A supplier
reactivated 40 days ago with active transactions and no payment is alarming.

---

## Metric 4 — CHARGEBACK_ANOMALY

**What it measures:** Whether chargebacks are abnormally high relative to
receivables and the supplier's own history.

**Absolute gates (at least one must pass before severity scoring):**

| Gate Level | Condition |
|---|---|
| Medium | Today's chargeback ≥ $200 OR delta vs median ≥ $200 |
| High / Critical | Today's chargeback ≥ $500 OR delta vs median ≥ $200 |

**Severity levels (chargeback ratio AND vs trailing median must both trigger):**

| Severity | Chargeback Ratio | vs Trailing Median | Score Contribution |
|---|---|---|---|
| MEDIUM | ≥ 0.60 | ≥ 2.0x | ~45% of weight (weight = 18) |
| HIGH | ≥ 1.00 | ≥ 4.5x | ~70% of weight |
| CRITICAL | ≥ 1.80 | ≥ 10.0x | ~90% of weight |

Only evaluated when today's receivable is greater than zero.

**Key insight:** This metric carries the highest base weight (18). A
chargeback ratio above 1.0 means chargebacks now exceed receivables — the
supplier is losing more than they earn.

---

## Metric 5 — NET_EARNING

**What it measures:** Whether net earnings are negative, and whether the
negative trend is persistent.

**Trigger A — Current period value:**

| Severity | Net Earning | Score Contribution |
|---|---|---|
| MEDIUM | ≤ -$500 | ~60% of weight (weight = 15) |
| HIGH | ≤ -$10,000 | 100% of weight |

**Trigger B — Consecutive negative streak (both conditions required):**

| Severity | Streak | Additional Condition | Score Contribution |
|---|---|---|---|
| HIGH | ≥ 2 consecutive | Current net earning also ≤ -$500 | 100% of weight |
| CRITICAL | ≥ 3 consecutive | 3-period cumulative sum < -$1,000 | weight + 8 bonus |

**Key insight:** The streak triggers require the current period to also be
negative — a supplier recovering from two bad periods will not be escalated.
The CRITICAL path requires the cumulative 3-period loss to exceed $1,000,
filtering out repeated tiny negatives.

---

## Metric 6 — AVAILABLE_BALANCE

**What it measures:** Whether the supplier's available balance has gone
negative, and how deeply.

| Severity | Available Balance | Score Contribution |
|---|---|---|
| MEDIUM | ≤ -$500 | ~65% of weight (weight = 20) |
| HIGH | ≤ -$2,000 | 100% of weight |
| CRITICAL | ≤ -$7,000 | weight + 5 bonus |

**Key insight:** Highest-weighted metric (20). A negative available balance
means the supplier actively owes money to the platform right now. The minimum
triggering threshold is -$500 — small overdrafts below this are ignored.

---

## Metric 7 — DUE_FROM_SUPPLIER

**What it measures:** Whether the supplier has an amount due back to
Payability, with special attention to whether this is a new event.

| Severity | Condition | Score Contribution |
|---|---|---|
| MEDIUM | > 0 AND ≥ 10% of outstanding exposure | ~65% of weight (weight = 19) |
| HIGH | > 0 AND ≥ 25% of outstanding exposure | 100% of weight |
| HIGH | Just turned positive, but below CRITICAL criteria | 100% of weight |
| CRITICAL | Just turned positive AND ≥ $100 AND ≥ 5% of outstanding | weight + 6 bonus |

**Key insight:** The `turnedPositive` event signals a structural break —
marketplace payments can no longer cover the full exposure. The CRITICAL
threshold requires both a minimum amount ($100) and a minimum ratio (5%) to
avoid triggering on tiny rounding differences.

---

## Metric 8 — OUTSTANDING_EXPOSURE (Context Only)

Never triggered, never contributes to score. Provided for context only —
shows the magnitude of exposure behind other triggered metrics.

---

## Hard Escalation Rules

After all metric scores are summed, the engine may force the score upward.

**The five hard trigger conditions:**
1. `due_from_supplier` turned positive AND ≥ $100 AND ≥ 5% of outstanding
2. Net earning negative for ≥ 3 periods AND 3-period sum < -$1,000
3. Available balance ≤ -$2,000
4. Chargeback severity is CRITICAL
5. Marketplace payment delay severity is CRITICAL

| Triggers Fired | Minimum Engine Score |
|---|---|
| 1 | Forced to at least **65** → Risk score ≥ 7 |
| ≥ 2 | Forced to at least **85** → Risk score ≥ 9 |

---

## Minimum Score to Flag

A supplier is only included in the flagged output if:
1. At least one metric was triggered, AND
2. `engine_suggested_risk_score` **≥ 3**

Suppliers with only minor signals (score 1–2) are intentionally excluded.

---

## Score Mapping: Engine Score (0–100) → Risk Score (1–10)

| Engine Score | Risk Score |
|---|---|
| 0–9 | 1 |
| 10–19 | 2 |
| 20–29 | 3 |
| 30–39 | 4 |
| 40–49 | 5 |
| 50–59 | 6 |
| 60–69 | 7 |
| 70–79 | 8 |
| 80–89 | 9 |
| 90–100 | 10 |

---

## Metric Weights Summary

| Metric | Base Weight | Max Bonus | Notes |
|---|---|---|---|
| RECEIVABLE_SURGE | 8 | — | Upward spikes only |
| RECEIVABLE_DROP | 10 | — | Downward trends only |
| MARKETPLACE_PAYMENT_DELAY | 12 | — | Activation-aware |
| CHARGEBACK_ANOMALY | 18 | — | Highest base weight |
| NET_EARNING | 15 | +8 | CRITICAL streak bonus |
| AVAILABLE_BALANCE | 20 | +5 | CRITICAL balance bonus |
| DUE_FROM_SUPPLIER | 19 | +6 | CRITICAL turn bonus |

---

## AI Agent Usage Guidelines

1. **Activation context matters more than calendar days.** Always check
   `has_payment_since_activation` before interpreting payment delay. A newly
   reactivated supplier without a payment is not necessarily overdue.

2. **Surge and drop are both risks, but different kinds.** A surge creates
   growing liability that may not be collectible. A drop reduces future cash
   flow available to repay existing exposure.

3. **Streaks compound risk.** Three consecutive negative net earning periods
   with a meaningful cumulative loss is a structural problem, not a one-time
   event. The current period must also be negative for the streak to escalate.

4. **The `turnedPositive` event for `due_from_supplier` is the most important
   structural signal.** This is a regime change — the marketplace can no longer
   cover what the supplier owes. Treat it with higher urgency than a
   persistently positive but stable due-from-supplier.

5. **Absolute dollar amounts matter as much as ratios.** The engine gates on
   minimum thresholds before applying percentage rules. Always consider scale —
   a 300% receivable surge from $100 to $400 is not flagged.

6. **Two or more hard triggers override arithmetic.** When multiple CRITICAL
   conditions fire simultaneously, the score is forced to at least 85 (risk ≥ 9)
   regardless of individual metric scores.

7. **Score ≥ 3 is the noise floor.** All flagged suppliers have already passed
   a minimum signal threshold. You can trust that minor one-off signals have
   been filtered out.

8. **Outstanding exposure is context, not a score driver.** Use the exposure
   amount to calibrate urgency in your narrative, but do not treat it as a
   risk trigger in its own right.

---

## Pipeline Architecture

```
BigQuery
  vm_transaction_summary
  v_supplier_summary  (Active filter)
  reactivations       (activation dates)
        ↓
  getSupplierRiskInputData()
  [Active suppliers only, liability ≥ $100, activation-aware fields]
        ↓
  flagSuppliers()  ← risk-engine.ts  (policy v4.2.0)
      ├── RECEIVABLE_SURGE
      ├── RECEIVABLE_DROP
      ├── MARKETPLACE_PAYMENT_DELAY (activation-aware)
      ├── CHARGEBACK_ANOMALY
      ├── NET_EARNING
      ├── AVAILABLE_BALANCE
      ├── DUE_FROM_SUPPLIER
      ├── OUTSTANDING_EXPOSURE (context only)
      ├── Hard escalation rules
      └── Minimum score gate (≥ 3)
        ↓
  Compare vs consolidated_flagged_supplier_list
  [Only changed or new suppliers written]
        ↓
  generateRiskReportJSON()  ← ai-report.ts
  [AI writes trigger_reason, may adjust score ±1 point only]
        ↓
  Supabase:
    agent_run_daily_summary_report          (one row per run)
    daily_summary_report_flagged_suppliers  (append-only history)
    consolidated_flagged_supplier_list      (upsert: latest state)
        ↓
  Vercel Cron (daily at 13:00 UTC)
```

---

*Policy version: v4.2.0 — sourced from `lib/risk-policy.ts` and `lib/risk-engine.ts`*
