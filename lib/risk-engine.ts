// lib/risk-engine.ts

export type DailyChangeRow = {
  supplier_key: string | null;
  supplier_name: string | null;

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

  // From BigQuery query (optional, but supported)
  has_prev_week_data?: boolean | null;
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

  // extra context for debugging / prompt
  has_prev_week_data: boolean;

  flag_reasons: string[];
};

export type RiskEngineResult = {
  total: number;
  flagged: FlaggedSupplier[];
  unflagged: number;
};

// ====== Tunables (intentionally loose for MVP/framework) ======
const THRESHOLD_PCT = 10; // abs(% change) > 10 triggers
const CHARGEBACK_NONZERO = 0; // any non-zero triggers reason (loose)
const NEGATIVE_EARNING_FLAG = true;
const NEGATIVE_AVAILABLE_BALANCE_FLAG = true;

// ====== Helpers ======
function n(v: number | null | undefined): number {
  return typeof v === "number" && Number.isFinite(v) ? v : 0;
}

function pct(v: number | null | undefined): number | null {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

function money(v: number): string {
  // keep it simple; you can swap to Intl.NumberFormat later
  return `$${v.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
}

function absPctFlag(p: number | null, threshold: number): boolean {
  return p !== null && Math.abs(p) > threshold;
}

export function flagSuppliers(rows: DailyChangeRow[]): RiskEngineResult {
  const total = Array.isArray(rows) ? rows.length : 0;
  const flagged: FlaggedSupplier[] = [];

  for (const r of rows || []) {
    const supplier_key = (r?.supplier_key ?? "").toString().trim();
    const supplier_name = (r?.supplier_name ?? "").toString().trim();

    // Skip malformed rows (prevents empty supplier buckets)
    if (!supplier_key || !supplier_name) continue;

    const today_receivable = n(r.today_receivable);
    const prev_receivable = n(r.prev_receivable);

    const today_liability = n(r.today_liability);
    const prev_liability = n(r.prev_liability);

    const today_net_earning = n(r.today_net_earning);
    const today_chargeback = n(r.today_chargeback);
    const today_available_balance = n(r.today_available_balance);
    const today_outstanding_bal = n(r.today_outstanding_bal);

    const receivable_change_pct = pct(r.receivable_change_pct);
    const liability_change_pct = pct(r.liability_change_pct);

    const has_prev_week_data = Boolean(r.has_prev_week_data);

    const reasons: string[] = [];

    // ===== Core rules: abs % change > threshold =====
    const receivable_flagged = absPctFlag(receivable_change_pct, THRESHOLD_PCT);
    const liability_flagged = absPctFlag(liability_change_pct, THRESHOLD_PCT);

    if (receivable_flagged) {
      const dir = receivable_change_pct! > 0 ? "up" : "down";
      reasons.push(
        `Receivable ${dir} ${Math.abs(receivable_change_pct!).toFixed(2)}% WoW: ${money(prev_receivable)} → ${money(today_receivable)}`
      );
    }

    if (liability_flagged) {
      const dir = liability_change_pct! > 0 ? "up" : "down";
      reasons.push(
        `Potential liability ${dir} ${Math.abs(liability_change_pct!).toFixed(2)}% WoW: ${money(prev_liability)} → ${money(today_liability)}`
      );
    }

    // ===== Loose auxiliary rules (helps MVP feel real) =====
    // 1) No baseline but non-zero today (data quality / new activity)
    if (!has_prev_week_data) {
      if (today_receivable !== 0) reasons.push("Receivable is non-zero with no prior-week baseline");
      if (today_liability !== 0) reasons.push("Liability is non-zero with no prior-week baseline");
    }

    // 2) Any chargeback activity (very loose)
    if (today_chargeback !== CHARGEBACK_NONZERO) {
      reasons.push(`Chargeback is non-zero: ${money(today_chargeback)}`);
    }

    // 3) Negative net earning (loose)
    if (NEGATIVE_EARNING_FLAG && today_net_earning < 0) {
      reasons.push(`Net earning is negative: ${money(today_net_earning)}`);
    }

    // 4) Negative available balance (loose)
    if (NEGATIVE_AVAILABLE_BALANCE_FLAG && today_available_balance < 0) {
      reasons.push(`Available balance is negative: ${money(today_available_balance)}`);
    }

    // Decide: flag if any reason exists
    // (keeps it intentionally permissive for framework build)
    const isFlagged = reasons.length > 0;

    if (isFlagged) {
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

        has_prev_week_data,
        flag_reasons: reasons,
      });
    }
  }

  // Sorting: more severe first
  // - both change flags first
  // - then by max(abs change pct)
  // - then by absolute $ exposure proxy (outstanding + abs(liability) + receivable)
  flagged.sort((a, b) => {
    const aCount = (a.receivable_flagged ? 1 : 0) + (a.liability_flagged ? 1 : 0);
    const bCount = (b.receivable_flagged ? 1 : 0) + (b.liability_flagged ? 1 : 0);
    if (bCount !== aCount) return bCount - aCount;

    const aMax = Math.max(Math.abs(a.receivable_change_pct ?? 0), Math.abs(a.liability_change_pct ?? 0));
    const bMax = Math.max(Math.abs(b.receivable_change_pct ?? 0), Math.abs(b.liability_change_pct ?? 0));
    if (bMax !== aMax) return bMax - aMax;

    const aExposure = Math.abs(a.today_outstanding_bal) + Math.abs(a.today_liability) + Math.abs(a.today_receivable);
    const bExposure = Math.abs(b.today_outstanding_bal) + Math.abs(b.today_liability) + Math.abs(b.today_receivable);
    return bExposure - aExposure;
  });

  return { total, flagged, unflagged: total - flagged.length };
}