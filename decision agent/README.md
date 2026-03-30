# Supplier Risk Decision Agent

A daily automated downstream decision agent that aggregates outputs from four upstream risk agents, calls an LLM to produce a consolidated risk score (5–10) and rationale for each flagged supplier, and writes results to the database.

---

## Architecture Overview

```
consolidated_flagged_supplier_list   ← suppliers flagged today by at least one upstream agent
        |
        ▼
  profile_builder.py                 ← builds a full risk profile per supplier
        |
        ├── today scores  (get_today_scores)   ← queries 4 upstream agent tables
        └── 7-day history (get_history_7d)     ← queries 4 upstream agent tables (flagged days only)
        |
        ▼
     llm.py                          ← calls Gemini with the profile, returns final_score + reason
        |
        ▼
  decision_agent_daily_report        ← results upserted to Supabase
```

---

## Four Upstream Agents

| Agent | Table | Risk Type | Flag Threshold |
|---|---|---|---|
| `daily_summary_report` | `daily_summary_report_flagged_suppliers` | **Financial risk** (balance, receivables, net earnings) | score >= 3 |
| `json_report` | `json_risk_report` | Amazon account health risk | score >= 8 |
| `health_report` | `health_daily_risk` | Operational / compliance risk | score > 6 |
| `ship_tracking` | `ship_risk_scores` | Shipping anomaly risk | score >= 6 |

> `daily_summary_report` is the highest-priority signal. Its low threshold (>=3) is intentional — any flag represents a real financial event, not noise.

---

## File Structure

```
decision agent/
├── main.py              # Production entry point: processes all flagged suppliers and writes to DB
├── smoke_test.py        # Test entry point: runs a single supplier, outputs JSON files to outputs/
├── db.py                # All Supabase read/write logic (with field-name auto-detection fallback)
├── profile_builder.py   # Assembles supplier profile (today scores + 7-day history + resonance_count)
├── llm.py               # Gemini API call, output validation, retry on failure
├── SKILL.md             # LLM system prompt: scoring rules, agent priorities, output format
├── outputs/             # Local JSON outputs from smoke_test (not written to DB)
├── .env                 # Environment variables (not committed to git)
└── requirements.txt     # Python dependencies
```

---

## Environment Variables

Copy `.env.example` to `.env` and fill in:

```
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_SERVICE_KEY=sb_secret_xxx
GEMINI_API_KEY=AIzaSyxxx

# Optional
REPORT_DATE=2026-03-30     # defaults to today if not set
LOG_LEVEL=INFO
MAX_WORKERS=10
GEMINI_MODEL=gemini-2.0-flash
```

---

## Running

**Production run (writes to database):**
```bash
python main.py
```

**Re-run for a specific date:**
```bash
REPORT_DATE=2026-03-30 python main.py
```

**Test a single supplier (local JSON output only, no DB write):**
```bash
python smoke_test.py --supplier-key <uuid> --report-date 2026-03-30
```

---

## Scoring Logic

The LLM follows rules defined in `SKILL.md` and outputs an integer score from 5 to 10:

| Score | Meaning |
|---|---|
| 5 | Single weak-threshold trigger, no history |
| 6 | Moderate signal, or `daily_summary_report` flagged alone (hard floor) |
| 7 | Strong single-agent signal or two-agent weak resonance. **Ceiling for single-agent flags with no history.** |
| 8 | Requires: 2+ agents flagged today, or single severe flag + history, or strong `daily_summary_report` evidence |
| 9 | Critical multi-dimensional risk: severe flag + 4+ days of persistent history |
| 10 | Emergency: 3–4 agents simultaneously flagged with persistent history |

**Key rules:**
- `resonance_count` (number of agents flagged today) sets the base floor: 1→≥5, 2→≥6, 3→≥7, 4→≥8
- `daily_summary_report` flagged today → final_score hard floor of 6
- `history_7d` must materially affect the score, not just serve as context
- `daily_summary_report` appearing in history raises the score even if not flagged today

---

## Database Tables

| Table | Purpose |
|---|---|
| `consolidated_flagged_supplier_list` | Today's flagged supplier list (input) |
| `daily_summary_report_flagged_suppliers` | Financial risk agent data |
| `json_risk_report` | Amazon account risk agent data |
| `health_daily_risk` | Operational/compliance risk agent data |
| `ship_risk_scores` | Shipping anomaly agent data |
| `decision_agent_daily_report` | Final decision output (upsert on supplier_key + report_date) |

---

## Known Data Notes

- `daily_summary_report_flagged_suppliers` uses `created_at` as the date field (other tables use `report_date`)
- `health_daily_risk` data is written at UTC midnight; `report_date` reflects the previous day in New York local time — a known timezone offset
- `db.py` includes automatic field-name fallback detection, but `daily_summary_report` is now hardcoded to use `created_at` + `overall_risk_score` + `reasons` for efficiency
