// lib/ai-report.ts
//import { anthropic } from "@ai-sdk/anthropic";
import { generateText } from "ai";
import type { FlaggedSupplier } from "@/lib/risk-engine";
//import { openai } from "@ai-sdk/openai";

export type RiskTier = "CRITICAL" | "HIGH" | "MONITOR";

export type RiskReportJSON = {
  report_date: string;
  suppliers_reviewed: number;
  portfolio_summary: {
    critical_count: number;
    high_count: number;
    monitor_count: number;
    notes: string[];
  };
  suppliers: Array<{
    supplier_key: string;
    supplier_name: string;
    risk_tier: RiskTier;
    headline: string;
    key_metrics: {
      receivable: number;
      chargeback: number;
      computed_net_earning: number;
      available_balance: number;
      outstanding_balance: number;
      receivable_wow_pct: number | null;
      liability_wow_pct: number | null;
    };
    triggers: string[];
    assessment: string[];
    actions: string[];
  }>;
};

function safeNum(x: unknown): number {
  const n = typeof x === "number" ? x : Number(x);
  return Number.isFinite(n) ? n : 0;
}

function coerceTier(x: unknown): RiskTier {
  return x === "CRITICAL" || x === "HIGH" || x === "MONITOR" ? x : "HIGH";
}

function stripCodeFences(s: string) {
  return s.replace(/```json\s*/g, "").replace(/```\s*/g, "").trim();
}

/**
 * ✅ Main API: JSON structured report.
 */
export async function generateRiskReportJSON(
  flagged: FlaggedSupplier[],
  opts?: { model?: string }
): Promise<RiskReportJSON> {
  const reportDate = new Date().toISOString().slice(0, 10);

  const payload = flagged.map((s) => {
    const receivable = safeNum(s.today_receivable);
    const chargeback = safeNum(s.today_chargeback);
    const computedNet = receivable - chargeback;

    return {
      supplier_key: s.supplier_key,
      supplier_name: s.supplier_name,
      receivable,
      chargeback,
      computed_net_earning: computedNet,
      available_balance: safeNum(s.today_available_balance),
      outstanding_balance: safeNum(s.today_outstanding_bal),
      receivable_wow_pct: s.receivable_change_pct ?? null,
      liability_wow_pct: s.liability_change_pct ?? null,
      flag_reasons: Array.isArray(s.flag_reasons) ? s.flag_reasons : [],
    };
  });

  const system = `
You are a risk analyst.
You MUST output valid JSON and NOTHING else (no markdown, no backticks, no commentary).

Rules:
- Do NOT invent triggers. "triggers" must be derived ONLY from input.flag_reasons (you may shorten/rephrase).
- Use tiers:
  - CRITICAL: immediate escalation/freeze recommended
  - HIGH: manual review required within 24-72h
  - MONITOR: watchlist

Return a single JSON object exactly matching the schema.
`.trim();

  const prompt = `
Return JSON matching this schema:

{
  "report_date": string,
  "suppliers_reviewed": number,
  "portfolio_summary": {
    "critical_count": number,
    "high_count": number,
    "monitor_count": number,
    "notes": string[]
  },
  "suppliers": [
    {
      "supplier_key": string,
      "supplier_name": string,
      "risk_tier": "CRITICAL" | "HIGH" | "MONITOR",
      "headline": string,
      "key_metrics": {
        "receivable": number,
        "chargeback": number,
        "computed_net_earning": number,
        "available_balance": number,
        "outstanding_balance": number,
        "receivable_wow_pct": number|null,
        "liability_wow_pct": number|null
      },
      "triggers": string[],
      "assessment": string[],
      "actions": string[]
    }
  ]
}

Constraints:
- report_date must be "${reportDate}"
- suppliers_reviewed must be ${payload.length}
- suppliers array length must equal ${payload.length}
- triggers must be based ONLY on flag_reasons for each supplier.

Input suppliers:
${JSON.stringify(payload)}
`.trim();

  const { text } = await generateText({
    //model: openai("gpt-4o-mini"),
    model: "openai/gpt-4o-mini",
    system,
    prompt,
    temperature: 0.2,
  });

  const raw = stripCodeFences(text);

  let parsed: any;
  try {
    parsed = JSON.parse(raw);
  } catch {
    const start = raw.indexOf("{");
    const end = raw.lastIndexOf("}");
    if (start >= 0 && end > start) parsed = JSON.parse(raw.slice(start, end + 1));
    else throw new Error("AI report is not valid JSON.");
  }

  const suppliers = Array.isArray(parsed?.suppliers) ? parsed.suppliers : [];

  const report: RiskReportJSON = {
    report_date: String(parsed?.report_date ?? reportDate),
    suppliers_reviewed: safeNum(parsed?.suppliers_reviewed ?? payload.length),
    portfolio_summary: {
      critical_count: safeNum(parsed?.portfolio_summary?.critical_count),
      high_count: safeNum(parsed?.portfolio_summary?.high_count),
      monitor_count: safeNum(parsed?.portfolio_summary?.monitor_count),
      notes: Array.isArray(parsed?.portfolio_summary?.notes)
        ? parsed.portfolio_summary.notes.map(String)
        : [],
    },
    suppliers: suppliers.map((x: any) => ({
      supplier_key: String(x?.supplier_key ?? ""),
      supplier_name: String(x?.supplier_name ?? ""),
      risk_tier: coerceTier(x?.risk_tier),
      headline: String(x?.headline ?? ""),
      key_metrics: {
        receivable: safeNum(x?.key_metrics?.receivable),
        chargeback: safeNum(x?.key_metrics?.chargeback),
        computed_net_earning: safeNum(x?.key_metrics?.computed_net_earning),
        available_balance: safeNum(x?.key_metrics?.available_balance),
        outstanding_balance: safeNum(x?.key_metrics?.outstanding_balance),
        receivable_wow_pct:
          x?.key_metrics?.receivable_wow_pct === null
            ? null
            : Number.isFinite(Number(x?.key_metrics?.receivable_wow_pct))
            ? Number(x?.key_metrics?.receivable_wow_pct)
            : null,
        liability_wow_pct:
          x?.key_metrics?.liability_wow_pct === null
            ? null
            : Number.isFinite(Number(x?.key_metrics?.liability_wow_pct))
            ? Number(x?.key_metrics?.liability_wow_pct)
            : null,
      },
      triggers: Array.isArray(x?.triggers) ? x.triggers.map(String) : [],
      assessment: Array.isArray(x?.assessment) ? x.assessment.map(String) : [],
      actions: Array.isArray(x?.actions) ? x.actions.map(String) : [],
    })),
  };

  return report;
}

/**
 * ✅ Compatibility alias (optional).
 * If any old code still imports generateRiskReport, it will still work.
 * You can remove this alias later once everything is migrated.
 */
export async function generateRiskReport(flagged: FlaggedSupplier[]) {
  const json = await generateRiskReportJSON(flagged);
  return JSON.stringify(json, null, 2);
}