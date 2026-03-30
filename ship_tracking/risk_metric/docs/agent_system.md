# Risk Metric Multi-Agent System

## Overview

This system runs multiple risk analysis pipelines in parallel across different data sources, aggregates their results into a unified supplier risk score, and continuously improves the scoring logic based on feedback from the risk team.

The system consists of **8 agents** across two layers:

- **Analysis layer (5 agents):** 4 parallel data analysis agents + 1 aggregation agent
- **Improvement layer (3 agents):** Supervision, Implementation, and Backtester agents that refine scoring logic over time based on risk team feedback

**Framework: LangGraph** — chosen for its native support for parallel fan-out/fan-in execution, stateful loops, and human-in-the-loop gates.

---

## System Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    ANALYSIS LAYER                        │
│                                                          │
│  Agent 1            Agent 2            Agent 3           │
│  Ship Tracking  ─┐  Data Source B  ─┐  Data Source C  ─┐│
│  (existing)      │  (TBD)           │  (TBD)           ││
│                  │                  │                   ││
│                  │    Agent 4       │                   ││
│                  │    Data Source D─┘                   ││
│                  │    (TBD)                             ││
│                  └──────────────┬───────────────────────┘│
│                                 │ all 4 complete          │
│                                 ▼                        │
│                        Agent 5: Aggregation              │
│                        Combines scores into              │
│                        final supplier risk score         │
└─────────────────────────────────┬────────────────────────┘
                                  │
                         Risk team reviews
                         marks False Positives
                         in supplier_reviews
                                  │
┌─────────────────────────────────▼────────────────────────┐
│                   IMPROVEMENT LAYER                      │
│                                                          │
│                  Agent 6: Supervision                    │
│                  Analyzes FP batch,                      │
│                  produces recommendations                │
│                          │                              │
│                   Slack notification                     │
│                   ← Human: Approve / Reject              │
│                          │                              │
│                  Agent 7: Implementation                 │
│                  Applies approved changes                │
│                  to a new git branch                     │
│                          │                              │
│                  Agent 8: Backtester                     │
│                  Validates changes on                    │
│                  historical data                         │
│                     │         │                          │
│                   Pass       Fail                        │
│                   Open PR    Loop back to Agent 6        │
│                              (max 3 iterations)          │
└──────────────────────────────────────────────────────────┘
```

---

## Analysis Layer

### Agent 1: Ship Tracking Risk Scorer (Existing)

**Role:** Evaluates supplier shipping behavior using untracked order rate, price escalation, and FedEx pickup lag signals.

**Data source:** BigQuery — `app_production_tracking_hub_trackingLabels`

**Key files:**
- `pipeline.py` — main entry point
- `core/llm_scorer.py` — scoring logic
- `prompts/llm_risk_scorer.md` — LLM prompt
- `config/settings.py` — tunable parameters

**Output:** Per-supplier risk score + trigger reason → passed to Aggregation Agent

---

### Agent 2–4: Additional Analysis Agents (TBD)

**Role:** Each analyzes a different data source using the same pipeline pattern as Agent 1.

**Structure:** Same as Agent 1 — BigQuery query → metric computation → LLM scoring → structured output

**Output:** Per-supplier risk score + signal summary → passed to Aggregation Agent

> These agents are not yet defined. Each will have its own `pipeline.py`, `llm_scorer.py`, and prompt file following the same pattern.

---

### Agent 5: Aggregation Agent

**Role:** Waits for all 4 analysis agents to complete, then combines their outputs into a single final risk score per supplier.

**Inputs:**
- Risk score + signal summary from each of Agents 1–4
- Presence/absence flags (some suppliers may only appear in some data sources)

**Responsibilities:**
1. Collect outputs from all 4 parallel agents
2. For each supplier, combine scores across data sources (weighted or max — TBD)
3. Determine final risk tier and overall trigger reason
4. Write final scores to Supabase

**Output:** Final per-supplier risk score written to `ship_risk_scores` and high-risk entries to `consolidated_flagged_supplier_list`

**Open question:** How to weight scores across data sources — equal weight, or does one source carry more signal?

---

## Improvement Layer

The improvement layer applies to the scoring logic of any analysis agent. When the risk team flags False Positives, the Supervision Agent identifies which data source / scoring layer caused the error and recommends targeted fixes.

### Agent 6: Supervision Agent

**Role:** Analyze a batch of False Positive reviews, identify root causes, and produce a structured recommendation list. Always analyzes the full batch — never acts on a single record.

**Trigger:** After risk team completes a review batch (minimum 3 False Positives). Manual trigger initially; can be automated later.

**Inputs:**
- All False Positive records in the current batch (`comment`, `supplier_key`, `flagged_record_id`, `verdict`)
- Original scoring details from `ship_risk_scores`
- Raw source data from BigQuery for each affected supplier
- Current scoring files for the relevant agent(s): prompt, scorer, config

**Data access (read-only):**

| System | Access |
|--------|--------|
| Supabase | `supplier_reviews`, `ship_risk_scores`, `carrier_daily_untracked` |
| BigQuery | All raw source tables |
| Local files | `prompts/`, `core/llm_scorer.py`, `config/settings.py` |

**No write access.**

**Output format (example):**
```json
{
  "analysis_date": "2026-03-27",
  "false_positive_count": 6,
  "affected_agent": "Agent 1 (Ship Tracking)",
  "common_patterns": [
    "USPS carrier-wide rate was above 35%, but supplier rate was below the carrier average",
    "All affected suppliers had 100-200 orders with moderate untracked rates (20-30%)"
  ],
  "root_causes": [
    {
      "layer": "llm_prompt",
      "file": "prompts/llm_risk_scorer.md",
      "issue": "No rule for supplier rate ≤ carrier rate in the 20-50% carrier range",
      "affected_cases": 4
    }
  ],
  "recommendations": [
    {
      "priority": "high",
      "layer": "llm_prompt",
      "description": "Add rule: if carrier_rate 20-50% AND supplier rate ≤ carrier rate, multiply untracked_score by 0.3",
      "rationale": "4 of 6 false positives had supplier rate at or below carrier baseline"
    }
  ]
}
```

---

### Agent 7: Implementation Agent

**Role:** Apply approved changes to the scoring system. Does nothing until the risk team explicitly approves via Slack.

### Human Approval Gate

Slack notification sent to risk team containing:
- Number of False Positives analyzed and patterns found
- Each proposed change in diff form
- Estimated impact (which supplier profiles are affected)

| Response | Action |
|----------|--------|
| `approve` | Implementation Agent proceeds |
| `reject` | Logged; pipeline ends for this cycle |
| No response within 24h | Marked pending; carried over to next cycle |

### Allowed Modifications

| File | Allowed changes |
|------|----------------|
| `prompts/llm_risk_scorer.md` | Rule descriptions, score values, carrier baseline logic |
| `config/settings.py` | Numeric values in `PARAMS` and `RISK_THRESHOLDS` |
| `core/llm_scorer.py` | Score formula coefficients (confidence cap, multiplier) |

**Not allowed to modify:** database schema, `pipeline.py`, `queries/`, production data.

**Output:** Modified files on a new feature branch + change summary → passed to Backtester Agent

---

### Agent 8: Backtester Agent

**Role:** Re-run the updated scoring logic on the same historical data that triggered the False Positives, and validate the fix does not break True Positive detection.

**Inputs:**
- Change summary from Implementation Agent
- Original False Positive suppliers (`supplier_key` + flagged date)
- True Positive suppliers from the same period
- Raw BigQuery data for all of the above

**Pass criteria:**
- False Positive fix rate ≥ 50%
- True Positive retention rate ≥ 90%
- Both must be met

**Outcome handling:**

| Result | Action |
|--------|--------|
| Pass | Slack report with metrics; Implementation Agent opens a PR |
| Fail | Results returned to Supervision Agent; re-run analysis (max 3 iterations) |
| Max iterations reached | Alert risk team for manual review; halt pipeline |

---

## Data Flow Summary

| Step | Source | Destination |
|------|--------|-------------|
| Daily scoring (×4 agents) | BigQuery tables | Per-agent risk scores |
| Aggregation | Agents 1–4 outputs | Supabase `ship_risk_scores` |
| Risk team review | `consolidated_flagged_supplier_list` | `supplier_reviews` |
| Feedback analysis | `supplier_reviews` + `ship_risk_scores` + BigQuery | Supervision Agent |
| Recommendations | Supervision Agent | Slack + Implementation Agent |
| Human approval | Risk team via Slack | Implementation Agent |
| Modified code | Implementation Agent | Backtester Agent |
| Backtest results | Backtester Agent | Slack / next Supervision loop |

---

## Tech Stack

- **Language:** Python
- **Orchestration:** LangGraph — for parallel fan-out/fan-in, stateful loop, and human-in-the-loop gate
- **LLM:** Gemini 2.5 Flash via OpenAI-compatible API
- **Notifications:** Slack Incoming Webhook (to be configured)
- **Version control:** All changes on isolated feature branches; no direct pushes to `main`
- **Scheduling:** Cron job triggers the analysis layer daily; improvement layer triggered after each review batch

---

## Planned Directory Structure

```
risk_metric/               ← Agent 1 (existing, unchanged)
├── pipeline.py
├── core/
├── prompts/
├── config/
├── metrics/
├── queries/
│
agents/                    ← Agents 2–8 (new)
├── graph.py               ← LangGraph graph definition
├── aggregation_agent.py   ← Agent 5
├── improvement/
│   ├── supervision_agent.py     ← Agent 6
│   ├── implementation_agent.py  ← Agent 7
│   ├── backtester_agent.py      ← Agent 8
│   └── prompts/
│       ├── supervision_system.md
│       └── backtester_system.md
├── agent_b/               ← Agent 2 (TBD)
├── agent_c/               ← Agent 3 (TBD)
└── agent_d/               ← Agent 4 (TBD)
```

---

## Open Questions

1. **Aggregation logic:** How should scores from Agents 1–4 be combined — equal weight, weighted by data source reliability, or take the max?
2. **Improvement scope:** Does the improvement loop apply to all 4 analysis agents, or only Agent 1 for now?
3. **Backtester pass criteria:** Fix rate ≥ 50% + retention ≥ 90% — are these thresholds right?
4. **Slack setup:** Which channel, who can approve?
5. **Agents 2–4 data sources:** What are the other 3 tables being analyzed?
