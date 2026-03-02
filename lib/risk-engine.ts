// lib/risk-engine.ts

export type DailyChangeRow = {
  supplier_key: string | null;
  supplier_name: string | null;

  // from your bigquery.ts SELECT aliases
  today_receivable: number | null;
  prev_receivable: number | null;

  today_liability: number | null;
  prev_liability: number | null;

  today_net_earning: number | null;
  today_chargeback: number | null;
  today_available_balance: number | null;
  today_outstanding_bal: number | null;

  receivable_change_pct: number | null;
  liability_change_pct: number | null;
};

export type FlaggedSupplier = {
  supplier_key: string;
  supplier_name: string;

  prev_receivable: number;
  today_receivable: number;
  prev_liability: number;
  today_liability: number;

  receivable_change_pct: number | null;
  liability_change_pct: number | null;

  today_net_earning: number;
  today_chargeback: number;
  today_available_balance: number;
  today_outstanding_bal: number;

  receivable_flagged: boolean;
  liability_flagged: boolean;
  flag_reasons: string[];

  // for debugging / interpretability
  has_prev_week_data: boolean;
};

export type RiskEngineResult = {
  total: number;
  flagged: FlaggedSupplier[];
};

function n(v: number | null | undefined): number {
  return typeof v === "number" && Number.isFinite(v) ? v : 0;
}

function isNonZero(v: number): boolean {
  // For FLOAT, treat tiny noise as zero
  return Math.abs(v) >= 1e-6;
}

export function flagSuppliers(rows: DailyChangeRow[]): RiskEngineResult {
  const total = Array.isArray(rows) ? rows.length : 0;
  const flagged: FlaggedSupplier[] = [];

  for (const r of rows || []) {
    const supplier_key = (r.supplier_key ?? "").toString();
    const supplier_name = (r.supplier_name ?? "").toString();

    const today_receivable = n(r.today_receivable);
    const prev_receivable = n(r.prev_receivable);

    const today_liability = n(r.today_liability);
    const prev_liability = n(r.prev_liability);

    const today_net_earning = n(r.today_net_earning);
    const today_chargeback = n(r.today_chargeback);
    const today_available_balance = n(r.today_available_balance);
    const today_outstanding_bal = n(r.today_outstanding_bal);

    const receivable_change_pct =
      typeof r.receivable_change_pct === "number" && Number.isFinite(r.receivable_change_pct)
        ? r.receivable_change_pct
        : null;

    const liability_change_pct =
      typeof r.liability_change_pct === "number" && Number.isFinite(r.liability_change_pct)
        ? r.liability_change_pct
        : null;

    // if prev week values exist (>0) or pct exists, we treat as having baseline
    const has_prev_week_data =
      prev_receivable > 0 || prev_liability > 0 || receivable_change_pct !== null || liability_change_pct !== null;

    const reasons: string[] = [];

    // --- VERY LOOSE RULES (debug mode) ---

    // 1) Any pct change (non-zero) when available
    const receivable_changed = receivable_change_pct !== null && receivable_change_pct !== 0;
    const liability_changed = liability_change_pct !== null && liability_change_pct !== 0;

    // 2) New activity without baseline (prev=0) but this week >0
    const receivable_new_activity = !has_prev_week_data && today_receivable > 0;
    const liability_new_activity = !has_prev_week_data && today_liability > 0;

    // 3) Any non-zero net earning / chargeback / balances (you can remove later)
    // These make it almost impossible to get 0 flagged.
    const negative_net = today_net_earning < 0;
    const any_chargeback = today_chargeback > 0;

    // Optional: include these if you want *extremely* loose
    const any_receivable = today_receivable > 0;
    const any_liability = today_liability > 0;

    const receivable_flagged = receivable_changed || receivable_new_activity || any_receivable;
    const liability_flagged = liability_changed || liability_new_activity || any_liability;

    if (receivable_changed) reasons.push(`Receivable changed WoW (${receivable_change_pct}%)`);
    if (receivable_new_activity) reasons.push("Receivable > 0 with no prior-week baseline");
    if (any_receivable && !receivable_changed && !receivable_new_activity)
      reasons.push("Receivable present (debug-wide)");

    if (liability_changed) reasons.push(`Liability changed WoW (${liability_change_pct}%)`);
    if (liability_new_activity) reasons.push("Liability > 0 with no prior-week baseline");
    if (any_liability && !liability_changed && !liability_new_activity)
      reasons.push("Liability present (debug-wide)");

    if (negative_net) reasons.push("Net earning is negative");
    if (any_chargeback) reasons.push("Chargeback is non-zero");

    const is_flagged = reasons.length > 0;

    if (is_flagged) {
      flagged.push({
        supplier_key,
        supplier_name,
        prev_receivable,
        today_receivable,
        prev_liability,
        today_liability,
        receivable_change_pct,
        liability_change_pct,
        today_net_earning,
        today_chargeback,
        today_available_balance,
        today_outstanding_bal,
        receivable_flagged,
        liability_flagged,
        flag_reasons: reasons,
        has_prev_week_data,
      });
    }
  }

  return { total, flagged };
}