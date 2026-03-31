"use client";
import { useState, useEffect, useCallback, useMemo, type ReactNode } from "react";
import { getSupabaseBrowser } from "@/lib/supabase-browser";
import { EASTERN_TZ, easternDateYmd, easternYmdToUtcRange } from "@/lib/eastern-date";
import { useRouter } from "next/navigation";
import { ThemeToggle } from "@/components/ThemeToggle";

type DecisionAgentDailyRecord = {
  id: number;
  supplier_key: string | null;
  supplier_name: string | null;
  report_date: string | null; // date (YYYY-MM-DD)
  final_score: number | null;
  agent_scores: any; // jsonb
  history_summary: any; // jsonb
  resonance_count: number | null;
  reason: string | null;
  created_at: string;
};

type ConsolidatedFlaggedRecord = {
  id: number;
  supplier_key: string;
  supplier_name: string;
  source: string;
  overall_risk_score: number;
  reasons: string[];
  status: string;
  created_at: string;
  /** Optional jsonb: metrics array or object with nested `metrics` (e.g. daily risk blob). */
  metrics?: unknown;
};

type SupplierReview = {
  id: string;
  created_at: string;
  updated_at: string;
  flagged_record_id?: number | null;
  supplier_key: string;
  report_date?: string | null;
  reviewer_id: string;
  reviewer_email: string;
  /** Stored as "True Positive" / "False Positive"; legacy values normalized when editing/displaying. */
  verdict: string;
  comment: string | null;
  source: string;
  suspended: boolean;
  emailed: boolean;
  monitored: boolean;
};

const VERDICT_TRUE = "True Positive";
const VERDICT_FALSE = "False Positive";

type ReviewVerdict = typeof VERDICT_TRUE | typeof VERDICT_FALSE;

/** Map DB / legacy verdict strings to canonical form for UI and writes. */
function normalizeVerdictFromDb(v: string | null | undefined): ReviewVerdict {
  const s = (v ?? "").trim();
  if (s === VERDICT_TRUE || s === "correct_flag" || s === "True_Positive") return VERDICT_TRUE;
  if (s === VERDICT_FALSE || s === "incorrect_flag" || s === "False_Positive" || s === "False_positive")
    return VERDICT_FALSE;
  return VERDICT_TRUE;
}

function followUpSummary(r: SupplierReview) {
  const parts: string[] = [];
  if (r.suspended) parts.push("Suspended");
  if (r.emailed) parts.push("Emailed");
  if (r.monitored) parts.push("Monitored");
  return parts.length ? parts.join(", ") : "—";
}

/** RFC 4180-style CSV field (always quoted; doubles internal quotes). */
function csvEscapeCell(value: unknown): string {
  if (value === null || value === undefined) return '""';
  const s = String(value);
  return `"${s.replace(/"/g, '""')}"`;
}

const iconSvg = {
  className: "h-4 w-4 shrink-0",
  fill: "none" as const,
  viewBox: "0 0 24 24",
  strokeWidth: 1.5,
  stroke: "currentColor" as const,
};

function VerdictIconBadge({ verdict }: { verdict: ReviewVerdict }) {
  const isTrue = verdict === VERDICT_TRUE;
  return (
    <span
      role="img"
      aria-label={verdict}
      title={verdict}
      className={`inline-flex items-center justify-center rounded-lg border p-2 shadow-sm ${
        isTrue
          ? "border-emerald-300/80 bg-emerald-100 text-emerald-800 dark:border-emerald-700 dark:bg-emerald-950/60 dark:text-emerald-200"
          : "border-red-300/80 bg-red-100 text-red-800 dark:border-red-700 dark:bg-red-950/55 dark:text-red-200"
      }`}
    >
      {isTrue ? (
        <svg {...iconSvg} aria-hidden>
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
          />
        </svg>
      ) : (
        <svg {...iconSvg} aria-hidden>
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            d="m9.75 9.75 4.5 4.5m0-4.5-4.5 4.5M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
          />
        </svg>
      )}
    </span>
  );
}

function FollowUpIconBadges({ r }: { r: SupplierReview }) {
  type Key = "suspended" | "emailed" | "monitored";
  const items: { key: Key; title: string; className: string; node: ReactNode }[] = [];
  if (r.suspended) {
    items.push({
      key: "suspended",
      title: "Suspended",
      className:
        "border-amber-300/80 bg-amber-100 text-amber-950 dark:border-amber-700 dark:bg-amber-950/50 dark:text-amber-100",
      node: (
        <svg {...iconSvg} aria-hidden>
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            d="M18.364 18.364A9 9 0 005.636 5.636m12.728 12.728A9 9 0 015.636 5.636m12.728 12.728L5.636 5.636"
          />
        </svg>
      ),
    });
  }
  if (r.emailed) {
    items.push({
      key: "emailed",
      title: "Emailed",
      className:
        "border-violet-300/80 bg-violet-100 text-violet-950 dark:border-violet-700 dark:bg-violet-950/50 dark:text-violet-100",
      node: (
        <svg {...iconSvg} aria-hidden>
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            d="M21.75 6.75v10.5a2.25 2.25 0 01-2.25 2.25h-15a2.25 2.25 0 01-2.25-2.25V6.75m19.5 0A2.25 2.25 0 0019.5 4.5h-15a2.25 2.25 0 00-2.25 2.25m19.5 0v.243a2.25 2.25 0 01-1.07 1.916l-7.5 4.615a2.25 2.25 0 01-2.36 0L3.32 8.91a2.25 2.25 0 01-1.07-1.916V6.75"
          />
        </svg>
      ),
    });
  }
  if (r.monitored) {
    items.push({
      key: "monitored",
      title: "Monitored",
      className:
        "border-cyan-300/80 bg-cyan-100 text-cyan-950 dark:border-cyan-700 dark:bg-cyan-950/50 dark:text-cyan-100",
      node: (
        <svg {...iconSvg} aria-hidden>
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            d="M2.036 12.322a1.012 1.012 0 010-.639C3.423 7.51 7.36 4.5 12 4.5c4.638 0 8.573 3.007 9.963 7.178.07.207.07.431 0 .639C20.577 16.49 16.64 19.5 12 19.5c-4.638 0-8.573-3.007-9.963-7.178z"
          />
          <path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
        </svg>
      ),
    });
  }

  if (items.length === 0) {
    return (
      <span
        className="inline-flex items-center gap-1.5 rounded-lg border border-zinc-200 bg-zinc-100 px-2.5 py-1 text-xs font-medium text-zinc-500 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-400"
        title="No follow-up actions"
      >
        <svg {...iconSvg} aria-hidden>
          <path strokeLinecap="round" strokeLinejoin="round" d="M18 12H6" />
        </svg>
      </span>
    );
  }

  return (
    <div className="flex flex-wrap items-center gap-1.5">
      {items.map((it) => (
        <span
          key={it.key}
          title={it.title}
          className={`inline-flex items-center justify-center rounded-lg border p-1.5 shadow-sm ${it.className}`}
        >
          {it.node}
        </span>
      ))}
    </div>
  );
}

type AgentMeta = {
  display_name: string;
  description: string;
  last_updated: string;
  active_rules: string;
};

function formatEastern(
  utcString: string | null | undefined,
  opts?: { dateOnly?: boolean }
): string {
  if (!utcString) return "—";
  const normalized = utcString.includes("T") ? utcString : utcString.replace(" ", "T");
  const d = new Date(normalized);
  if (Number.isNaN(d.getTime())) return utcString;

  if (opts?.dateOnly) {
    return new Intl.DateTimeFormat("en-US", {
      timeZone: EASTERN_TZ,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(d);
  }

  return new Intl.DateTimeFormat("en-US", {
    timeZone: EASTERN_TZ,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZoneName: "short",
  }).format(d);
}

const SOURCE_LABELS: Record<string, string> = {
  decision_agent: "Decision Agent",
  daily_summary_report: "Daily Summary Agent",
  ship_tracking: "Shipment Agent",
  json_report: "JSON Agent",
  health_report: "Health Agent",
};

type AgentFilterKey = "decision_agent" | "daily_summary_report" | "ship_tracking" | "json_report" | "health_report";

/** Sub-agent entries may include optional structured metrics (e.g. daily_summary_report). */
function agentEntryMetrics(entry: unknown): Record<string, unknown>[] {
  if (entry == null || typeof entry !== "object") return [];
  const o = entry as Record<string, unknown>;
  const raw = o.metrics ?? o.trigger_metrics;
  if (!Array.isArray(raw)) return [];
  return raw.filter((m): m is Record<string, unknown> => m != null && typeof m === "object");
}

function formatAgentMetricValue(m: Record<string, unknown>): string {
  const v = m.value;
  const unit = m.unit != null ? String(m.unit) : "";
  if (v == null || v === "") return unit ? `— ${unit}`.trim() : "—";
  if (typeof v === "number" && Number.isFinite(v)) {
    const s = unit === "%" ? v.toFixed(2) : String(v);
    return unit ? `${s} ${unit}` : s;
  }
  return unit ? `${String(v)} ${unit}` : String(v);
}

function metricTriggered(m: Record<string, unknown>): boolean {
  return m.triggered === true || m.triggered === "true";
}

function consolidatedReportYmd(record: ConsolidatedFlaggedRecord): string {
  const normalized = record.created_at.includes("T") ? record.created_at : record.created_at.replace(" ", "T");
  const d = new Date(normalized);
  return Number.isNaN(d.getTime()) ? record.created_at.slice(0, 10) : easternDateYmd(d);
}

/** Top-level metrics array or nested `{ metrics: [...] }` from consolidated_flagged_supplier_list. */
function consolidatedMetricsRows(record: ConsolidatedFlaggedRecord): Record<string, unknown>[] {
  const m = record.metrics as unknown;
  if (Array.isArray(m)) {
    return m.filter((x): x is Record<string, unknown> => x != null && typeof x === "object");
  }
  if (m && typeof m === "object" && Array.isArray((m as Record<string, unknown>).metrics)) {
    return ((m as Record<string, unknown>).metrics as unknown[]).filter(
      (x): x is Record<string, unknown> => x != null && typeof x === "object"
    );
  }
  return [];
}

const FIELD =
  "border border-gray-300 dark:border-zinc-600 rounded text-sm text-gray-900 dark:text-zinc-100 bg-white dark:bg-zinc-800 placeholder:text-gray-600 dark:placeholder:text-zinc-400";
const FIELD_FULL = `w-full px-2 py-1.5 ${FIELD}`;
const FIELD_NARROW = `w-14 px-2 py-1.5 ${FIELD}`;
const LABEL = "block text-xs text-gray-500 dark:text-zinc-400 mb-1";

type DateFilterMode = "all" | "single" | "range";

/** Inclusive Eastern calendar bounds for a start..end YYYY-MM-DD range (order-independent). */
function easternYmdRangeToUtcBounds(startYmd: string, endYmd: string): { startIso: string; endIso: string } {
  const lo = easternYmdToUtcRange(startYmd);
  const hi = easternYmdToUtcRange(endYmd);
  if (lo.startIso <= hi.startIso) return { startIso: lo.startIso, endIso: hi.endIso };
  return { startIso: hi.startIso, endIso: lo.endIso };
}

type AppRole = "viewer" | "reviewer" | "admin";

/** Normalize DB role strings (trim, case, common aliases) so admin UI is not lost to "Admin" vs "admin". */
function normalizeAppRole(role: unknown): AppRole {
  const r = String(role ?? "")
    .trim()
    .toLowerCase();
  if (r === "admin" || r === "administrator" || r === "super_admin" || r === "superadmin") return "admin";
  if (r === "reviewer") return "reviewer";
  if (r === "viewer") return "viewer";
  return "viewer";
}

function sameReviewerId(a: string | undefined | null, b: string | undefined | null): boolean {
  if (a == null || b == null) return false;
  return String(a).trim().toLowerCase() === String(b).trim().toLowerCase();
}

const NO_PERMISSION = "You do not have permission to perform this action.";

function mapSupabasePermissionError(err: { message?: string; code?: string } | null | undefined): string {
  if (!err?.message && !err?.code) return NO_PERMISSION;
  const m = (err.message ?? "").toLowerCase();
  if (
    err.code === "42501" ||
    m.includes("permission denied") ||
    m.includes("row-level security") ||
    m.includes("rls") ||
    m.includes("violates row-level security") ||
    m.includes("policy")
  ) {
    return NO_PERMISSION;
  }
  return err.message ?? NO_PERMISSION;
}

type TableSortColumn = "date" | "supplier" | "key" | "score" | "flagged_by";

type TableSortState =
  | { mode: "default" }
  | { mode: "column"; column: TableSortColumn; ascending: boolean; step: 1 | 2 };

type SummaryQuickFilter = "all" | "critical" | "flagged" | "unflagged" | "pending_review" | "reviewed";

const DECISION_TABLE_ORDER_COLUMN: Record<TableSortColumn, string> = {
  date: "report_date",
  supplier: "supplier_name",
  key: "supplier_key",
  score: "final_score",
  // In decision_agent_daily_report this is effectively constant in UI.
  flagged_by: "created_at",
};

const CONSOLIDATED_TABLE_ORDER_COLUMN: Record<TableSortColumn, string> = {
  date: "created_at",
  supplier: "supplier_name",
  key: "supplier_key",
  score: "overall_risk_score",
  flagged_by: "source",
};

function primaryAscending(column: TableSortColumn): boolean {
  return column === "supplier" || column === "key" || column === "flagged_by";
}

/** Active column + direction for UI and query (default = score desc). */
function sortStateToQuery(s: TableSortState): { column: TableSortColumn; ascending: boolean } {
  if (s.mode === "default") return { column: "score", ascending: false };
  return { column: s.column, ascending: s.ascending };
}

function TableSortHeader({
  label,
  column,
  activeColumn,
  ascending,
  onRequestSort,
}: {
  label: string;
  column: TableSortColumn;
  activeColumn: TableSortColumn;
  ascending: boolean;
  onRequestSort: (column: TableSortColumn) => void;
}) {
  const active = activeColumn === column;
  return (
    <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-zinc-400">
      <button
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          onRequestSort(column);
        }}
        className="inline-flex items-center gap-0.5 rounded -mx-1 px-1 py-0.5 text-left hover:bg-gray-200/80 dark:hover:bg-zinc-700/80"
        aria-label={`Sort by ${label}, ${active ? (ascending ? "ascending" : "descending") : "not sorted"}`}
        aria-sort={active ? (ascending ? "ascending" : "descending") : "none"}
      >
        <span>{label}</span>
        <span
          className={`text-[10px] tabular-nums ${
            active ? "text-gray-800 dark:text-zinc-200" : "text-gray-400 dark:text-zinc-500"
          }`}
          aria-hidden
        >
          {active ? (ascending ? "↑" : "↓") : "↕"}
        </span>
      </button>
    </th>
  );
}

export default function DashboardPage() {
  const supabase = getSupabaseBrowser();
  const router = useRouter();

  const [user, setUser] = useState<any>(null);
  const [decisionRecords, setDecisionRecords] = useState<DecisionAgentDailyRecord[]>([]);
  const [consolidatedRecords, setConsolidatedRecords] = useState<ConsolidatedFlaggedRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [agentMeta, setAgentMeta] = useState<Record<string, AgentMeta>>({});

  const [dateMode, setDateMode] = useState<DateFilterMode>("all");
  const [dateSingleYmd, setDateSingleYmd] = useState(() => easternDateYmd());
  const [dateRangeStartYmd, setDateRangeStartYmd] = useState(() => easternDateYmd());
  const [dateRangeEndYmd, setDateRangeEndYmd] = useState(() => easternDateYmd());
  const [sourceFilter, setSourceFilter] = useState<AgentFilterKey>("decision_agent");
  const [searchTerm, setSearchTerm] = useState("");
  const [scoreMin, setScoreMin] = useState(1);
  const [scoreMax, setScoreMax] = useState(10);
  const [tableSortState, setTableSortState] = useState<TableSortState>({ mode: "default" });
  const [summaryQuickFilter, setSummaryQuickFilter] = useState<SummaryQuickFilter>("all");

  const [selectedDecisionRecord, setSelectedDecisionRecord] = useState<DecisionAgentDailyRecord | null>(null);
  const [selectedConsolidatedRecord, setSelectedConsolidatedRecord] = useState<ConsolidatedFlaggedRecord | null>(null);
  const [riskHistory, setRiskHistory] = useState<any[]>([]);
  const [reviewComment, setReviewComment] = useState("");
  const [reviewVerdict, setReviewVerdict] = useState<ReviewVerdict>(VERDICT_TRUE);
  const [reviewSuspended, setReviewSuspended] = useState(false);
  const [reviewEmailed, setReviewEmailed] = useState(false);
  const [reviewMonitored, setReviewMonitored] = useState(false);
  const [reviewError, setReviewError] = useState("");
  const [reviewAgentLabel, setReviewAgentLabel] = useState<string>("Decision Agent");
  const [supplierReviews, setSupplierReviews] = useState<SupplierReview[]>([]);
  const [editingReviewId, setEditingReviewId] = useState<string | null>(null);
  const [agentDetailKey, setAgentDetailKey] = useState<string | null>(null);
  const [hoveredAgent, setHoveredAgent] = useState<string | null>(null);
  const [appRole, setAppRole] = useState<AppRole | null>(null);

  const canReview = appRole === "reviewer" || appRole === "admin";
  const isAdmin = appRole === "admin";
  const isDecisionView = sourceFilter === "decision_agent";

  useEffect(() => {
    supabase.auth.getUser().then(({ data }) => {
      if (!data.user) router.push("/login");
      else setUser(data.user);
    });
    loadAgentMeta();
  }, []);

  useEffect(() => {
    if (!user) {
      setAppRole(null);
      return;
    }
    let cancelled = false;
    (async () => {
      const { data, error } = await supabase.from("profiles").select("role").eq("id", user.id).maybeSingle();
      if (cancelled) return;
      if (error) {
        setAppRole("viewer");
        return;
      }
      if (!data?.role) {
        setAppRole("viewer");
        return;
      }
      setAppRole(normalizeAppRole(data.role));
    })();
    return () => {
      cancelled = true;
    };
  }, [user]);

  useEffect(() => {
    if (user) loadRecords();
  }, [user, dateMode, dateSingleYmd, dateRangeStartYmd, dateRangeEndYmd, sourceFilter, searchTerm, scoreMin, scoreMax, tableSortState]);

  async function loadAgentMeta() {
    const { data } = await supabase
      .from("app_settings")
      .select("value")
      .eq("key", "agent_metadata")
      .single();
    if (data?.value) setAgentMeta(data.value as Record<string, AgentMeta>);
  }

  const loadRecords = useCallback(async () => {
    setLoading(true);
    if (sourceFilter === "decision_agent") {
      let query = supabase
        .from("decision_agent_daily_report")
        .select("*")
        .gte("final_score", scoreMin)
        .lte("final_score", scoreMax);
      if (dateMode === "single") {
        query = query.eq("report_date", dateSingleYmd);
      } else if (dateMode === "range") {
        const lo = dateRangeStartYmd <= dateRangeEndYmd ? dateRangeStartYmd : dateRangeEndYmd;
        const hi = dateRangeStartYmd <= dateRangeEndYmd ? dateRangeEndYmd : dateRangeStartYmd;
        query = query.gte("report_date", lo).lte("report_date", hi);
      }
      if (searchTerm) query = query.or(`supplier_name.ilike.%${searchTerm}%,supplier_key.ilike.%${searchTerm}%`);
      const { column: orderCol, ascending: orderAsc } = sortStateToQuery(tableSortState);
      query = query.order(DECISION_TABLE_ORDER_COLUMN[orderCol], { ascending: orderAsc });

      const { data, error } = await query.limit(200);
      if (error) console.error("Load error:", error);
      setDecisionRecords((data as DecisionAgentDailyRecord[]) ?? []);
      setConsolidatedRecords([]);
      setLoading(false);
      return;
    }

    let query = supabase
      .from("consolidated_flagged_supplier_list")
      .select("*")
      .eq("source", sourceFilter)
      .gte("overall_risk_score", scoreMin)
      .lte("overall_risk_score", scoreMax);
    if (dateMode === "single") {
      const { startIso, endIso } = easternYmdToUtcRange(dateSingleYmd);
      query = query.gte("created_at", startIso).lte("created_at", endIso);
    } else if (dateMode === "range") {
      const { startIso, endIso } = easternYmdRangeToUtcBounds(dateRangeStartYmd, dateRangeEndYmd);
      query = query.gte("created_at", startIso).lte("created_at", endIso);
    }
    if (searchTerm) query = query.or(`supplier_name.ilike.%${searchTerm}%,supplier_key.ilike.%${searchTerm}%`);
    const { column: orderCol, ascending: orderAsc } = sortStateToQuery(tableSortState);
    query = query.order(CONSOLIDATED_TABLE_ORDER_COLUMN[orderCol], { ascending: orderAsc });

    const { data, error } = await query.limit(200);
    if (error) console.error("Load error:", error);
    setConsolidatedRecords((data as ConsolidatedFlaggedRecord[]) ?? []);
    setDecisionRecords([]);
    setLoading(false);
  }, [
    dateMode,
    dateSingleYmd,
    dateRangeStartYmd,
    dateRangeEndYmd,
    sourceFilter,
    searchTerm,
    scoreMin,
    scoreMax,
    tableSortState,
  ]);

  function requestTableSort(column: TableSortColumn) {
    setTableSortState((prev) => {
      if (prev.mode === "default") {
        if (column === "score") {
          return { mode: "column", column: "score", ascending: true, step: 1 };
        }
        return { mode: "column", column, ascending: primaryAscending(column), step: 1 };
      }
      if (prev.column !== column) {
        return { mode: "column", column, ascending: primaryAscending(column), step: 1 };
      }
      if (prev.step === 1) {
        return { mode: "column", column: prev.column, ascending: !prev.ascending, step: 2 };
      }
      return { mode: "default" };
    });
  }

  const displayDecisionRecords = useMemo(() => {
    const anyFlagged = (r: DecisionAgentDailyRecord) => {
      const a = r.agent_scores as any;
      if (!a || typeof a !== "object") return false;
      return Object.values(a).some((v: any) => Boolean(v?.flagged));
    };
    switch (summaryQuickFilter) {
      case "critical":
        return decisionRecords.filter((r) => (r.final_score ?? 0) >= 8);
      case "flagged":
        return decisionRecords.filter(anyFlagged);
      case "unflagged":
        return decisionRecords.filter((r) => !anyFlagged(r));
      default:
        return decisionRecords;
    }
  }, [decisionRecords, summaryQuickFilter]);

  const displayConsolidatedRecords = useMemo(() => {
    switch (summaryQuickFilter) {
      case "critical":
        return consolidatedRecords.filter((r) => r.overall_risk_score >= 8);
      case "pending_review":
        return consolidatedRecords.filter((r) => r.status === "pending_review");
      case "reviewed":
        return consolidatedRecords.filter((r) => r.status === "reviewed");
      default:
        return consolidatedRecords;
    }
  }, [consolidatedRecords, summaryQuickFilter]);

  function onSummaryCardClick(filter: SummaryQuickFilter) {
    if (filter === "all") {
      setSummaryQuickFilter("all");
      return;
    }
    setSummaryQuickFilter((prev) => (prev === filter ? "all" : filter));
  }

  function resetReviewForm() {
    setReviewComment("");
    setReviewVerdict(VERDICT_TRUE);
    setReviewSuspended(false);
    setReviewEmailed(false);
    setReviewMonitored(false);
    setReviewError("");
    setEditingReviewId(null);
  }

  function closeDetail() {
    setSelectedDecisionRecord(null);
    setSelectedConsolidatedRecord(null);
    setSupplierReviews([]);
    resetReviewForm();
    setAgentDetailKey(null);
  }

  async function loadReviewsForDecisionRecord(record: DecisionAgentDailyRecord) {
    const supplierKey = record.supplier_key ?? "";
    const reportDate = record.report_date ?? "";
    if (!supplierKey || !reportDate) {
      setSupplierReviews([]);
      return;
    }
    const { data, error } = await supabase
      .from("supplier_reviews")
      .select("*")
      .eq("supplier_key", supplierKey)
      .eq("report_date", reportDate)
      .order("created_at", { ascending: true });
    if (error) {
      console.error("Load reviews error:", error);
      setSupplierReviews([]);
      return;
    }
    setSupplierReviews((data as SupplierReview[]) ?? []);
  }

  async function loadReviewsForConsolidatedRecord(record: ConsolidatedFlaggedRecord) {
    const { data, error } = await supabase
      .from("supplier_reviews")
      .select("*")
      .eq("flagged_record_id", record.id)
      .order("created_at", { ascending: true });
    if (error) {
      console.error("Load reviews error:", error);
      setSupplierReviews([]);
      return;
    }
    setSupplierReviews((data as SupplierReview[]) ?? []);
  }

  async function openDecisionDetail(record: DecisionAgentDailyRecord) {
    setSelectedDecisionRecord(record);
    setSelectedConsolidatedRecord(null);
    setSupplierReviews([]);
    resetReviewForm();
    setReviewAgentLabel("Decision Agent");

    await loadReviewsForDecisionRecord(record);
    setRiskHistory([]);
  }

  async function openConsolidatedDetail(record: ConsolidatedFlaggedRecord) {
    setSelectedConsolidatedRecord(record);
    setSelectedDecisionRecord(null);
    setSupplierReviews([]);
    resetReviewForm();
    setReviewAgentLabel(SOURCE_LABELS[record.source] ?? record.source);
    setAgentDetailKey(null);
    setRiskHistory([]);
    await loadReviewsForConsolidatedRecord(record);
  }

  function validateReviewFields() {
    const anyFollowUp = reviewSuspended || reviewEmailed || reviewMonitored;
    if (reviewVerdict === VERDICT_TRUE && !anyFollowUp) {
      setReviewError("For True Positive, select at least one: Suspended, Emailed, or Monitored.");
      return false;
    }
    if (reviewVerdict === VERDICT_FALSE && anyFollowUp) {
      setReviewError("False Positive cannot include follow-up actions.");
      return false;
    }
    return true;
  }

  function startEditReview(r: SupplierReview) {
    setReviewError("");
    if (!canReview) {
      setReviewError(NO_PERMISSION);
      return;
    }
    if (!isAdmin && !sameReviewerId(r.reviewer_id, user?.id)) {
      setReviewError("You do not have permission to edit another user's review.");
      return;
    }
    setEditingReviewId(r.id);
    setReviewVerdict(normalizeVerdictFromDb(r.verdict));
    setReviewComment(r.comment ?? "");
    setReviewSuspended(r.suspended);
    setReviewEmailed(r.emailed);
    setReviewMonitored(r.monitored);
    setReviewAgentLabel(r.source?.trim() ? r.source : "Decision Agent");
  }

  async function saveReviewEdit() {
    if (!user || !editingReviewId) return;
    const decisionRec = selectedDecisionRecord;
    const consRec = selectedConsolidatedRecord;
    if (!decisionRec && !consRec) return;
    setReviewError("");
    if (!canReview) {
      setReviewError(NO_PERMISSION);
      return;
    }
    if (!validateReviewFields()) return;

    const supplierKey = decisionRec ? (decisionRec.supplier_key ?? "") : consRec!.supplier_key;
    const reportDate = decisionRec ? (decisionRec.report_date ?? "") : consolidatedReportYmd(consRec!);
    if (!supplierKey || !reportDate) {
      setReviewError("Missing supplier_key or report_date for this record.");
      return;
    }

    let q = supabase
      .from("supplier_reviews")
      .update({
        verdict: reviewVerdict,
        comment: reviewComment,
        source: reviewAgentLabel,
        supplier_key: supplierKey,
        report_date: reportDate,
        suspended: reviewSuspended,
        emailed: reviewEmailed,
        monitored: reviewMonitored,
        updated_at: new Date().toISOString(),
      })
      .eq("id", editingReviewId);
    if (!isAdmin) q = q.eq("reviewer_id", user.id);
    const { data: updatedRows, error } = await q.select("id");

    if (error) {
      setReviewError(mapSupabasePermissionError(error));
      return;
    }
    if (!updatedRows?.length) {
      setReviewError(NO_PERMISSION);
      return;
    }

    if (decisionRec) await loadReviewsForDecisionRecord(decisionRec);
    else await loadReviewsForConsolidatedRecord(consRec!);
    resetReviewForm();
  }

  async function deleteReview(review: SupplierReview) {
    if (!user || (!selectedDecisionRecord && !selectedConsolidatedRecord)) return;
    const decisionRec = selectedDecisionRecord;
    const consRec = selectedConsolidatedRecord;
    setReviewError("");
    if (!canReview) {
      setReviewError(NO_PERMISSION);
      return;
    }
    if (!isAdmin && !sameReviewerId(review.reviewer_id, user.id)) {
      setReviewError("You do not have permission to delete another user's review.");
      return;
    }
    if (!window.confirm("Delete this review? This cannot be undone.")) return;
    let dq = supabase.from("supplier_reviews").delete().eq("id", review.id);
    if (!isAdmin) dq = dq.eq("reviewer_id", user.id);
    const { error } = await dq;
    if (error) {
      setReviewError(mapSupabasePermissionError(error));
      return;
    }
    if (editingReviewId === review.id) resetReviewForm();
    if (decisionRec) await loadReviewsForDecisionRecord(decisionRec);
    else if (consRec) await loadReviewsForConsolidatedRecord(consRec);
    loadRecords();
  }

  async function submitReview() {
    if (!user || editingReviewId) return;
    const decisionRec = selectedDecisionRecord;
    const consRec = selectedConsolidatedRecord;
    if (!decisionRec && !consRec) return;

    setReviewError("");
    if (!canReview) {
      setReviewError(NO_PERMISSION);
      return;
    }
    if (consRec) {
      if (supplierReviews.some((r) => sameReviewerId(r.reviewer_id, user.id))) {
        setReviewError("You already submitted a review for this flag. Edit or delete it first.");
        return;
      }
    } else if (
      supplierReviews.some((r) => sameReviewerId(r.reviewer_id, user.id) && (r.source ?? "").trim() === reviewAgentLabel)
    ) {
      setReviewError("You already have a review for this agent. Edit or delete it first.");
      return;
    }
    if (!validateReviewFields()) return;

    const supplierKey = decisionRec ? (decisionRec.supplier_key ?? "") : consRec!.supplier_key;
    const reportDate = decisionRec ? (decisionRec.report_date ?? "") : consolidatedReportYmd(consRec!);
    if (!supplierKey || !reportDate) {
      setReviewError("Missing supplier_key or report_date for this record.");
      return;
    }

    const insertRow: Record<string, unknown> = {
      supplier_key: supplierKey,
      report_date: reportDate,
      reviewer_id: user.id,
      reviewer_email: user.email,
      verdict: reviewVerdict,
      comment: reviewComment,
      source: reviewAgentLabel,
      suspended: reviewSuspended,
      emailed: reviewEmailed,
      monitored: reviewMonitored,
    };
    if (consRec) insertRow.flagged_record_id = consRec.id;

    const { error: insertError } = await supabase.from("supplier_reviews").insert(insertRow);

    if (insertError) {
      setReviewError(mapSupabasePermissionError(insertError));
      return;
    }

    if (decisionRec) await loadReviewsForDecisionRecord(decisionRec);
    else await loadReviewsForConsolidatedRecord(consRec!);
    resetReviewForm();
    loadRecords();
  }

  async function exportCSV() {
    if (isDecisionView) {
      const headers = [
        "Report Date",
        "Supplier Key",
        "Supplier Name",
        "Risk Score",
        "Decision Agent Score",
        "Resonance Count",
        "Reason",
        "reviewed_by",
        "reviewed_date",
        "verdict",
        "comment",
        "suspended",
        "emailed",
        "monitored",
      ];

      const keys = Array.from(
        new Set(displayDecisionRecords.map((r) => String(r.supplier_key ?? "")).filter(Boolean))
      );
      const dates = Array.from(new Set(displayDecisionRecords.map((r) => String(r.report_date ?? "")).filter(Boolean)));
      let allReviews: SupplierReview[] = [];
      if (keys.length > 0 && dates.length > 0) {
        // PostgREST can't do composite IN (supplier_key, report_date), so we overfetch by supplier_key and date bounds
        // then filter in-memory.
        const lo = dates.slice().sort()[0];
        const hi = dates.slice().sort().slice(-1)[0];
        let q = supabase.from("supplier_reviews").select("*").in("supplier_key", keys);
        q = q.gte("report_date", lo).lte("report_date", hi);
        const { data, error } = await q;
        if (error) {
          console.error("Export reviews error:", error);
          window.alert("Could not load reviews for export. Please try again.");
          return;
        }
        allReviews = (data as SupplierReview[]) ?? [];
      }

      const wantedPairs = new Set(
        displayDecisionRecords.map((r) => `${r.supplier_key ?? ""}__${r.report_date ?? ""}`)
      );
      const byPair = new Map<string, SupplierReview[]>();
      for (const rev of allReviews) {
        const pairKey = `${String((rev as any).supplier_key ?? "")}__${String((rev as any).report_date ?? "")}`;
        if (!wantedPairs.has(pairKey)) continue;
        const list = byPair.get(pairKey);
        if (list) list.push(rev);
        else byPair.set(pairKey, [rev]);
      }
      for (const list of byPair.values()) {
        list.sort((a, b) => a.created_at.localeCompare(b.created_at));
      }

      const rows: unknown[][] = [];
      for (const r of displayDecisionRecords) {
        const reportDate = r.report_date ?? "";
        const base = [
          reportDate,
          r.supplier_key ?? "",
          r.supplier_name ?? "",
          r.final_score ?? "",
          r.final_score ?? "",
          r.resonance_count ?? "",
          r.reason ?? "",
        ];
        const pairKey = `${r.supplier_key ?? ""}__${r.report_date ?? ""}`;
        const reviews = byPair.get(pairKey) ?? [];
        if (reviews.length === 0) {
          rows.push([...base, "", "", "", "", "false", "false", "false"]);
        } else {
          for (const rev of reviews) {
            rows.push([
              ...base,
              rev.reviewer_email,
              formatEastern(rev.created_at),
              normalizeVerdictFromDb(rev.verdict),
              rev.comment?.trim() ? rev.comment : "",
              rev.suspended ? "true" : "false",
              rev.emailed ? "true" : "false",
              rev.monitored ? "true" : "false",
            ]);
          }
        }
      }

      const csv = [headers, ...rows].map((row) => row.map(csvEscapeCell).join(",")).join("\n");
      const blob = new Blob([csv], { type: "text/csv" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      const dateSuffix =
        dateMode === "all"
          ? "all_dates"
          : dateMode === "single"
            ? dateSingleYmd
            : `${dateRangeStartYmd}_to_${dateRangeEndYmd}`;
      a.download = `flagged_suppliers_${dateSuffix}.csv`;
      a.click();
      URL.revokeObjectURL(url);
      return;
    }

    // Consolidated export (one row per review; falls back to a single empty-review row).
    const headers = [
      "Report Date",
      "Supplier Key",
      "Supplier Name",
      "Risk Score",
      "Source",
      "Status",
      "Reasons",
      "reviewed_by",
      "reviewed_date",
      "verdict",
      "comment",
      "suspended",
      "emailed",
      "monitored",
    ];

    const ids = displayConsolidatedRecords.map((r) => r.id);
    let allReviews: SupplierReview[] = [];
    if (ids.length > 0) {
      const { data, error } = await supabase.from("supplier_reviews").select("*").in("flagged_record_id", ids);
      if (error) {
        console.error("Export reviews error:", error);
        window.alert("Could not load reviews for export. Please try again.");
        return;
      }
      allReviews = (data as SupplierReview[]) ?? [];
    }
    const byFlag = new Map<number, SupplierReview[]>();
    for (const rev of allReviews) {
      const fid = Number((rev as any).flagged_record_id);
      if (!Number.isFinite(fid)) continue;
      const list = byFlag.get(fid);
      if (list) list.push(rev);
      else byFlag.set(fid, [rev]);
    }
    for (const list of byFlag.values()) list.sort((a, b) => a.created_at.localeCompare(b.created_at));

    const rows: unknown[][] = [];
    for (const r of displayConsolidatedRecords) {
      const normalized = r.created_at.includes("T") ? r.created_at : r.created_at.replace(" ", "T");
      const d = new Date(normalized);
      const reportDate = Number.isNaN(d.getTime()) ? r.created_at.slice(0, 10) : easternDateYmd(d);
      const base = [
        reportDate,
        r.supplier_key,
        r.supplier_name,
        r.overall_risk_score,
        SOURCE_LABELS[r.source] ?? r.source,
        r.status,
        Array.isArray(r.reasons) ? r.reasons.join("; ") : "",
      ];
      const reviews = byFlag.get(r.id) ?? [];
      if (reviews.length === 0) {
        rows.push([...base, "", "", "", "", "false", "false", "false"]);
      } else {
        for (const rev of reviews) {
          rows.push([
            ...base,
            rev.reviewer_email,
            formatEastern(rev.created_at),
            normalizeVerdictFromDb(rev.verdict),
            rev.comment?.trim() ? rev.comment : "",
            rev.suspended ? "true" : "false",
            rev.emailed ? "true" : "false",
            rev.monitored ? "true" : "false",
          ]);
        }
      }
    }

    const csv = [headers, ...rows].map((row) => row.map(csvEscapeCell).join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    const dateSuffix =
      dateMode === "all"
        ? "all_dates"
        : dateMode === "single"
          ? dateSingleYmd
          : `${dateRangeStartYmd}_to_${dateRangeEndYmd}`;
    a.download = `flagged_suppliers_consolidated_${sourceFilter}_${dateSuffix}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  async function handleLogout() {
    await supabase.auth.signOut();
    router.push("/login");
  }

  const hasMyReview = useMemo(() => {
    if (!user?.id) return false;
    if (selectedConsolidatedRecord) {
      return supplierReviews.some((r) => sameReviewerId(r.reviewer_id, user.id));
    }
    return supplierReviews.some(
      (r) => sameReviewerId(r.reviewer_id, user.id) && (r.source ?? "").trim() === reviewAgentLabel.trim()
    );
  }, [user, supplierReviews, reviewAgentLabel, selectedConsolidatedRecord]);

  if (!user || appRole === null) return null;

  const sortQuery = sortStateToQuery(tableSortState);

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-zinc-950">
      <header className="bg-white dark:bg-zinc-900 border-b border-gray-200 dark:border-zinc-800 px-6 py-4 flex justify-between items-center gap-4">
        <h1 className="text-xl font-bold text-gray-900 dark:text-zinc-100">Payability Risk Dashboard</h1>
        <div className="flex items-center gap-4 flex-wrap justify-end">
          <ThemeToggle />
          <span className="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-zinc-400 px-2 py-0.5 rounded border border-gray-200 dark:border-zinc-600">
            {appRole}
          </span>
          <span className="text-sm text-gray-500 dark:text-zinc-400">{user.email}</span>
          <button onClick={handleLogout} className="text-sm text-red-600 dark:text-red-400 hover:underline">
            Sign Out
          </button>
        </div>
      </header>

      <div className="p-6 max-w-7xl mx-auto">
        {!canReview && (
          <div className="mb-4 rounded-lg border border-amber-200 dark:border-amber-900/50 bg-amber-50 dark:bg-amber-950/30 px-4 py-3 text-sm text-amber-900 dark:text-amber-200">
            View-only access: you can browse data and export CSV, but cannot submit, edit, or delete reviews.
          </div>
        )}
        {/* Filters */}
        <div className="bg-white dark:bg-zinc-900 rounded-lg shadow border border-gray-200 dark:border-zinc-800 p-4 mb-6 grid grid-cols-2 md:grid-cols-5 gap-3">
          <div>
            <label className={LABEL}>Date</label>
            <select
              value={dateMode}
              onChange={(e) => {
                const v = e.target.value as DateFilterMode;
                const prev = dateMode;
                setDateMode(v);
                if (v === "single" && prev === "all") setDateSingleYmd(easternDateYmd());
                if (v === "single" && prev === "range") {
                  setDateSingleYmd(
                    dateRangeStartYmd <= dateRangeEndYmd ? dateRangeStartYmd : dateRangeEndYmd
                  );
                }
                if (v === "range") {
                  if (prev === "all") {
                    const t = easternDateYmd();
                    setDateRangeStartYmd(t);
                    setDateRangeEndYmd(t);
                  } else if (prev === "single") {
                    setDateRangeStartYmd(dateSingleYmd);
                    setDateRangeEndYmd(dateSingleYmd);
                  }
                }
              }}
              className={FIELD_FULL}
            >
              <option value="all">All dates</option>
              <option value="single">Single day</option>
              <option value="range">Date range</option>
            </select>
            {dateMode === "single" && (
              <input
                type="date"
                value={dateSingleYmd}
                onChange={(e) => setDateSingleYmd(e.target.value)}
                className={`${FIELD_FULL} mt-1.5`}
              />
            )}
            {dateMode === "range" && (
              <div className="mt-1.5 space-y-1.5">
                <div>
                  <span className="block text-[10px] text-gray-500 dark:text-zinc-500 mb-0.5">From</span>
                  <input
                    type="date"
                    value={dateRangeStartYmd}
                    onChange={(e) => setDateRangeStartYmd(e.target.value)}
                    className={FIELD_FULL}
                  />
                </div>
                <div>
                  <span className="block text-[10px] text-gray-500 dark:text-zinc-500 mb-0.5">To</span>
                  <input
                    type="date"
                    value={dateRangeEndYmd}
                    onChange={(e) => setDateRangeEndYmd(e.target.value)}
                    className={FIELD_FULL}
                  />
                </div>
              </div>
            )}
          </div>
          <div>
            <label className={LABEL}>Agent</label>
            <select value={sourceFilter} onChange={(e) => setSourceFilter(e.target.value as AgentFilterKey)}
              className={FIELD_FULL}>
              <option value="decision_agent">Decision Agent</option>
              <option value="health_report">Health Agent</option>
              <option value="json_report">JSON Agent</option>
              <option value="ship_tracking">Shipment Agent</option>
              <option value="daily_summary_report">Daily Summary Agent</option>
            </select>
          </div>
          <div>
            <label className={LABEL}>Search</label>
            <input type="text" placeholder="Name or key..." value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className={FIELD_FULL} />
          </div>
          <div>
            <label className={LABEL}>Score Range</label>
            <div className="flex gap-1 items-center text-gray-900 dark:text-zinc-100">
              <input type="number" min={1} max={10} value={scoreMin}
                onChange={(e) => setScoreMin(Number(e.target.value))}
                className={FIELD_NARROW} />
              <span className="py-1.5">-</span>
              <input type="number" min={1} max={10} value={scoreMax}
                onChange={(e) => setScoreMax(Number(e.target.value))}
                className={FIELD_NARROW} />
            </div>
          </div>
          <div className="flex items-end">
            <button
              type="button"
              onClick={() => void exportCSV()}
              className="w-full px-3 py-1.5 bg-green-600 text-white rounded text-sm hover:bg-green-700"
            >
              Export CSV
            </button>
          </div>
        </div>

        {/* Summary cards — click to filter table; click again to clear (except Total resets to all) */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-6">
          <button
            type="button"
            onClick={() => onSummaryCardClick("all")}
            aria-pressed={summaryQuickFilter === "all"}
            className={`rounded-lg shadow border p-4 text-center cursor-pointer transition w-full bg-white dark:bg-zinc-900 border-gray-200 dark:border-zinc-800 hover:bg-gray-50 dark:hover:bg-zinc-800/80 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 ${
              summaryQuickFilter === "all"
                ? "ring-2 ring-blue-600 ring-offset-2 ring-offset-gray-50 dark:ring-blue-400 dark:ring-offset-zinc-950"
                : ""
            }`}
          >
            <div className="text-2xl font-bold text-gray-900 dark:text-zinc-100">
              {isDecisionView ? decisionRecords.length : consolidatedRecords.length}
            </div>
            <div className="text-xs text-gray-500 dark:text-zinc-400">Flagged Total</div>
          </button>
          <button
            type="button"
            onClick={() => onSummaryCardClick("critical")}
            aria-pressed={summaryQuickFilter === "critical"}
            className={`rounded-lg shadow border p-4 text-center cursor-pointer transition w-full bg-white dark:bg-zinc-900 border-gray-200 dark:border-zinc-800 hover:bg-gray-50 dark:hover:bg-zinc-800/80 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 ${
              summaryQuickFilter === "critical"
                ? "ring-2 ring-blue-600 ring-offset-2 ring-offset-gray-50 dark:ring-blue-400 dark:ring-offset-zinc-950"
                : ""
            }`}
          >
            <div className="text-2xl font-bold text-red-600 dark:text-red-400">
              {isDecisionView
                ? decisionRecords.filter((r) => (r.final_score ?? 0) >= 8).length
                : consolidatedRecords.filter((r) => r.overall_risk_score >= 8).length}
            </div>
            <div className="text-xs text-gray-500 dark:text-zinc-400">Critical (8-10)</div>
          </button>
          <button
            type="button"
            onClick={() => onSummaryCardClick(isDecisionView ? "flagged" : "pending_review")}
            aria-pressed={summaryQuickFilter === (isDecisionView ? "flagged" : "pending_review")}
            className={`rounded-lg shadow border p-4 text-center cursor-pointer transition w-full bg-white dark:bg-zinc-900 border-gray-200 dark:border-zinc-800 hover:bg-gray-50 dark:hover:bg-zinc-800/80 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 ${
              summaryQuickFilter === (isDecisionView ? "flagged" : "pending_review")
                ? "ring-2 ring-blue-600 ring-offset-2 ring-offset-gray-50 dark:ring-blue-400 dark:ring-offset-zinc-950"
                : ""
            }`}
          >
            <div className="text-2xl font-bold text-yellow-600 dark:text-yellow-500">
              {isDecisionView
                ? decisionRecords.filter((r) => {
                    const a = r.agent_scores as any;
                    if (!a || typeof a !== "object") return false;
                    return Object.values(a).some((v: any) => Boolean(v?.flagged));
                  }).length
                : consolidatedRecords.filter((r) => r.status === "pending_review").length}
            </div>
            <div className="text-xs text-gray-500 dark:text-zinc-400">
              {isDecisionView ? "Flagged (any agent)" : "Pending Review"}
            </div>
          </button>
          <button
            type="button"
            onClick={() => onSummaryCardClick(isDecisionView ? "unflagged" : "reviewed")}
            aria-pressed={summaryQuickFilter === (isDecisionView ? "unflagged" : "reviewed")}
            className={`rounded-lg shadow border p-4 text-center cursor-pointer transition w-full bg-white dark:bg-zinc-900 border-gray-200 dark:border-zinc-800 hover:bg-gray-50 dark:hover:bg-zinc-800/80 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 ${
              summaryQuickFilter === (isDecisionView ? "unflagged" : "reviewed")
                ? "ring-2 ring-blue-600 ring-offset-2 ring-offset-gray-50 dark:ring-blue-400 dark:ring-offset-zinc-950"
                : ""
            }`}
          >
            <div className="text-2xl font-bold text-green-600 dark:text-green-400">
              {isDecisionView
                ? decisionRecords.filter((r) => {
                    const a = r.agent_scores as any;
                    if (!a || typeof a !== "object") return true;
                    return !Object.values(a).some((v: any) => Boolean(v?.flagged));
                  }).length
                : consolidatedRecords.filter((r) => r.status === "reviewed").length}
            </div>
            <div className="text-xs text-gray-500 dark:text-zinc-400">
              {isDecisionView ? "Unflagged" : "Reviewed"}
            </div>
          </button>
        </div>

        {/* Table */}
        <div className="bg-white dark:bg-zinc-900 rounded-lg shadow border border-gray-200 dark:border-zinc-800 overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 dark:bg-zinc-800 border-b border-gray-200 dark:border-zinc-700">
              <tr>
                <TableSortHeader
                  label="Date"
                  column="date"
                  activeColumn={sortQuery.column}
                  ascending={sortQuery.ascending}
                  onRequestSort={requestTableSort}
                />
                <TableSortHeader
                  label="Supplier"
                  column="supplier"
                  activeColumn={sortQuery.column}
                  ascending={sortQuery.ascending}
                  onRequestSort={requestTableSort}
                />
                <TableSortHeader
                  label="Key"
                  column="key"
                  activeColumn={sortQuery.column}
                  ascending={sortQuery.ascending}
                  onRequestSort={requestTableSort}
                />
                <TableSortHeader
                  label="Score"
                  column="score"
                  activeColumn={sortQuery.column}
                  ascending={sortQuery.ascending}
                  onRequestSort={requestTableSort}
                />
                <TableSortHeader
                  label="Flagged By"
                  column="flagged_by"
                  activeColumn={sortQuery.column}
                  ascending={sortQuery.ascending}
                  onRequestSort={requestTableSort}
                />
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-zinc-400">Reason</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-zinc-400">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-zinc-700">
              {loading ? (
                <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400 dark:text-zinc-500">Loading...</td></tr>
              ) : (isDecisionView ? decisionRecords.length === 0 : consolidatedRecords.length === 0) ? (
                <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400 dark:text-zinc-500">No records found</td></tr>
              ) : (isDecisionView ? displayDecisionRecords.length === 0 : displayConsolidatedRecords.length === 0) ? (
                <tr>
                  <td colSpan={7} className="px-4 py-8 text-center text-gray-400 dark:text-zinc-500">
                    No rows match this summary filter. Click the same card again or choose &quot;Flagged Total&quot; to show all.
                  </td>
                </tr>
              ) : (
                (isDecisionView ? displayDecisionRecords : displayConsolidatedRecords).map((r) => (
                  isDecisionView ? (
                    <tr key={(r as DecisionAgentDailyRecord).id} onClick={() => openDecisionDetail(r as DecisionAgentDailyRecord)} className="hover:bg-blue-50 dark:hover:bg-zinc-800 cursor-pointer">
                      <td className="px-4 py-3 text-gray-600 dark:text-zinc-300">{(r as DecisionAgentDailyRecord).report_date ?? "—"}</td>
                      <td className="px-4 py-3 font-medium text-gray-900 dark:text-zinc-100">{(r as DecisionAgentDailyRecord).supplier_name ?? "—"}</td>
                      <td className="px-4 py-3 text-gray-500 dark:text-zinc-400 font-mono text-xs">
                        {String((r as DecisionAgentDailyRecord).supplier_key ?? "").slice(0, 8)}{(r as DecisionAgentDailyRecord).supplier_key ? "..." : ""}
                      </td>
                      <td className="px-4 py-3">
                        <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-bold ${
                          (((r as DecisionAgentDailyRecord).final_score ?? 0) >= 8) ? "bg-red-100 text-red-800 dark:bg-red-950/50 dark:text-red-300" :
                          (((r as DecisionAgentDailyRecord).final_score ?? 0) >= 5) ? "bg-yellow-100 text-yellow-800 dark:bg-yellow-950/40 dark:text-yellow-300" :
                          "bg-green-100 text-green-800 dark:bg-green-950/50 dark:text-green-300"
                        }`}>{(r as DecisionAgentDailyRecord).final_score ?? "—"}</span>
                      </td>
                      <td className="px-4 py-3">
                        <span className="text-gray-700 dark:text-zinc-200 text-xs font-medium">Decision Agent</span>
                      </td>
                      <td className="px-4 py-3 text-gray-600 dark:text-zinc-300 text-xs max-w-xs truncate">
                        {(r as DecisionAgentDailyRecord).reason ?? ""}
                      </td>
                      <td className="px-4 py-3">
                        <span className={`inline-flex px-2 py-0.5 rounded text-xs ${
                          (((r as DecisionAgentDailyRecord).final_score ?? 0) >= 8)
                            ? "bg-red-100 text-red-700 dark:bg-red-950/50 dark:text-red-300"
                            : "bg-gray-100 text-gray-700 dark:bg-zinc-800 dark:text-zinc-300"
                        }`}>{((r as DecisionAgentDailyRecord).final_score ?? 0) >= 8 ? "Critical" : "—"}</span>
                      </td>
                    </tr>
                  ) : (
                    <tr key={(r as ConsolidatedFlaggedRecord).id} onClick={() => openConsolidatedDetail(r as ConsolidatedFlaggedRecord)} className="hover:bg-blue-50 dark:hover:bg-zinc-800 cursor-pointer">
                      <td className="px-4 py-3 text-gray-600 dark:text-zinc-300">{formatEastern((r as ConsolidatedFlaggedRecord).created_at)}</td>
                      <td className="px-4 py-3 font-medium text-gray-900 dark:text-zinc-100">{(r as ConsolidatedFlaggedRecord).supplier_name}</td>
                      <td className="px-4 py-3 text-gray-500 dark:text-zinc-400 font-mono text-xs">{(r as ConsolidatedFlaggedRecord).supplier_key.slice(0, 8)}...</td>
                      <td className="px-4 py-3">
                        <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-bold ${
                          (r as ConsolidatedFlaggedRecord).overall_risk_score >= 8 ? "bg-red-100 text-red-800 dark:bg-red-950/50 dark:text-red-300" :
                          (r as ConsolidatedFlaggedRecord).overall_risk_score >= 5 ? "bg-yellow-100 text-yellow-800 dark:bg-yellow-950/40 dark:text-yellow-300" :
                          "bg-green-100 text-green-800 dark:bg-green-950/50 dark:text-green-300"
                        }`}>{(r as ConsolidatedFlaggedRecord).overall_risk_score}</span>
                      </td>
                      <td className="px-4 py-3 relative">
                        <span
                          className="text-blue-600 dark:text-blue-400 underline cursor-help"
                          onMouseEnter={() => setHoveredAgent((r as ConsolidatedFlaggedRecord).source)}
                          onMouseLeave={() => setHoveredAgent(null)}
                        >
                          {SOURCE_LABELS[(r as ConsolidatedFlaggedRecord).source] ?? (r as ConsolidatedFlaggedRecord).source}
                        </span>
                        {hoveredAgent === (r as ConsolidatedFlaggedRecord).source && agentMeta[(r as ConsolidatedFlaggedRecord).source] && (
                          <div className="absolute z-50 bg-gray-900 text-white p-3 rounded-lg text-xs w-72 left-0 top-full mt-1 shadow-lg">
                            <div className="font-bold mb-1">{agentMeta[(r as ConsolidatedFlaggedRecord).source].display_name}</div>
                            <div className="mb-1">{agentMeta[(r as ConsolidatedFlaggedRecord).source].description}</div>
                            <div className="text-gray-300">Rules: {agentMeta[(r as ConsolidatedFlaggedRecord).source].active_rules}</div>
                            <div className="text-gray-400 mt-1">Updated: {formatEastern(agentMeta[(r as ConsolidatedFlaggedRecord).source].last_updated)}</div>
                          </div>
                        )}
                      </td>
                      <td className="px-4 py-3 text-gray-600 dark:text-zinc-300 text-xs max-w-xs truncate">
                        {Array.isArray((r as ConsolidatedFlaggedRecord).reasons) ? (r as ConsolidatedFlaggedRecord).reasons[0] : ""}
                      </td>
                      <td className="px-4 py-3">
                        <span className={`inline-flex px-2 py-0.5 rounded text-xs ${
                          (r as ConsolidatedFlaggedRecord).status === "reviewed"
                            ? "bg-green-100 text-green-700 dark:bg-green-950/50 dark:text-green-300"
                            : "bg-orange-100 text-orange-700 dark:bg-orange-950/40 dark:text-orange-300"
                        }`}>{(r as ConsolidatedFlaggedRecord).status === "reviewed" ? "Reviewed" : "Pending"}</span>
                      </td>
                    </tr>
                  )
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Detail Modal (Decision daily report or single-agent consolidated flag) */}
      {(selectedDecisionRecord || selectedConsolidatedRecord) && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-white dark:bg-zinc-900 rounded-lg shadow-xl w-full max-w-3xl max-h-[90vh] overflow-y-auto border border-gray-200 dark:border-zinc-800">
            <div className="p-6">
              <div className="-mx-6 -mt-6 px-6 py-4 sticky top-0 z-10 bg-white/95 dark:bg-zinc-900/95 backdrop-blur border-b border-gray-200 dark:border-zinc-800">
                <div className="flex justify-between items-start gap-4">
                  <div className="min-w-0">
                    <h2 className="text-lg font-bold text-gray-900 dark:text-zinc-100 truncate">
                      {selectedDecisionRecord
                        ? (selectedDecisionRecord.supplier_name ?? "—")
                        : selectedConsolidatedRecord!.supplier_name}
                    </h2>
                    <p className="text-sm text-gray-500 dark:text-zinc-400 font-mono truncate">
                      {selectedDecisionRecord
                        ? (selectedDecisionRecord.supplier_key ?? "—")
                        : selectedConsolidatedRecord!.supplier_key}
                    </p>
                    <div className="mt-1 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-gray-500 dark:text-zinc-400">
                      <span>
                        Date:{" "}
                        <span className="font-medium text-gray-700 dark:text-zinc-200">
                          {selectedDecisionRecord
                            ? (selectedDecisionRecord.report_date ?? "—")
                            : consolidatedReportYmd(selectedConsolidatedRecord!)}
                        </span>
                      </span>
                      <span>
                        Agent:{" "}
                        <span className="font-medium text-gray-700 dark:text-zinc-200">
                          {selectedDecisionRecord
                            ? reviewAgentLabel
                            : SOURCE_LABELS[selectedConsolidatedRecord!.source] ?? selectedConsolidatedRecord!.source}
                        </span>
                      </span>
                    </div>
                  </div>
                  <button
                    type="button"
                    onClick={closeDetail}
                    className="shrink-0 text-gray-400 dark:text-zinc-500 hover:text-gray-600 dark:hover:text-zinc-300 text-2xl leading-none"
                    aria-label="Close"
                  >
                    &times;
                  </button>
                </div>
              </div>

              {selectedDecisionRecord && (
              <>
              {/* Decision Agent Summary */}
              <div className="mb-6 pt-4">
                <h3 className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-2">Decision Agent</h3>
                <div className="flex flex-wrap items-center gap-3">
                  <div className="bg-gray-50 dark:bg-zinc-800 rounded-lg border border-gray-200 dark:border-zinc-700 px-3 py-2">
                    <div className="text-[10px] uppercase tracking-wide text-gray-500 dark:text-zinc-400">Final score</div>
                    <div className="text-lg font-bold text-gray-900 dark:text-zinc-100">{selectedDecisionRecord.final_score ?? "—"}</div>
                  </div>
                  <div className="bg-gray-50 dark:bg-zinc-800 rounded-lg border border-gray-200 dark:border-zinc-700 px-3 py-2">
                    <div className="text-[10px] uppercase tracking-wide text-gray-500 dark:text-zinc-400">Resonance</div>
                    <div className="text-lg font-bold text-gray-900 dark:text-zinc-100">{selectedDecisionRecord.resonance_count ?? "—"}</div>
                  </div>
                </div>
              </div>

              {/* Agent Scores (from agent_scores jsonb) */}
              <div className="mb-6">
                <h3 className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-2">Agent Scores</h3>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
                  {Object.entries(SOURCE_LABELS).map(([agentKey, label]) => {
                    const entry = (selectedDecisionRecord.agent_scores ?? {})[agentKey] as any;
                    const score = entry?.score;
                    const flagged = Boolean(entry?.flagged);
                    return (
                      <button
                        key={agentKey}
                        type="button"
                        onClick={() => {
                          setAgentDetailKey(agentKey);
                          setReviewAgentLabel(label);
                        }}
                        className={`text-left rounded-lg border px-3 py-2 bg-gray-50 dark:bg-zinc-800 hover:bg-gray-100 dark:hover:bg-zinc-700/60 transition ${
                          flagged
                            ? "border-red-300 dark:border-red-700"
                            : "border-gray-200 dark:border-zinc-700"
                        }`}
                        title={entry?.reason ?? label}
                      >
                        <div className="text-[10px] uppercase tracking-wide text-gray-500 dark:text-zinc-400">
                          {label}
                        </div>
                        <div className={`text-lg font-bold ${
                          score == null ? "text-gray-300 dark:text-zinc-600" :
                          Number(score) >= 8 ? "text-red-600 dark:text-red-400" :
                          Number(score) >= 5 ? "text-yellow-600 dark:text-yellow-500" :
                          "text-green-600 dark:text-green-400"
                        }`}>{score ?? "—"}</div>
                        {flagged && (
                          <div className="mt-0.5 text-[10px] font-medium text-red-600 dark:text-red-400">
                            Flagged
                          </div>
                        )}
                      </button>
                    );
                  })}
                </div>
              </div>

              {/* History Summary (from history_summary jsonb) */}
              <div className="mb-6">
                <h3 className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-2">History Summary</h3>
                <div className="space-y-3">
                  {Object.entries(SOURCE_LABELS).map(([agentKey, label]) => {
                    const rows = ((selectedDecisionRecord.history_summary ?? {})[agentKey] ?? []) as any[];
                    if (!Array.isArray(rows) || rows.length === 0) return null;
                    return (
                      <div key={agentKey} className="rounded-lg border border-gray-200 dark:border-zinc-700 bg-gray-50 dark:bg-zinc-800/70 p-3">
                        <div className="text-xs font-semibold text-gray-700 dark:text-zinc-200 mb-2">{label}</div>
                        <div className="space-y-2">
                          {rows.slice(-5).reverse().map((it, idx) => (
                            <div key={idx} className="text-xs text-gray-700 dark:text-zinc-300">
                              <div className="flex flex-wrap items-center gap-2">
                                <span className="font-mono text-gray-500 dark:text-zinc-400">{it?.date ?? "—"}</span>
                                <span className="font-semibold">{it?.score ?? "—"}</span>
                                {it?.flagged ? (
                                  <span className="inline-flex items-center rounded px-1.5 py-0.5 bg-red-100 text-red-700 dark:bg-red-950/40 dark:text-red-300">
                                    Flagged
                                  </span>
                                ) : (
                                  <span className="inline-flex items-center rounded px-1.5 py-0.5 bg-gray-100 text-gray-600 dark:bg-zinc-900/40 dark:text-zinc-400">
                                    —
                                  </span>
                                )}
                              </div>
                              {it?.reason && (
                                <div className="mt-1 text-gray-600 dark:text-zinc-400 leading-snug">
                                  {String(it.reason)}
                                </div>
                              )}
                            </div>
                          ))}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* Agent Detail Popup */}
              {selectedDecisionRecord && agentDetailKey && (
                <div className="fixed inset-0 z-[60] flex items-center justify-center p-4">
                  <button
                    type="button"
                    className="absolute inset-0 bg-black/50"
                    onClick={() => setAgentDetailKey(null)}
                    aria-label="Close agent detail"
                  />
                  <div className="relative w-full max-w-2xl max-h-[85vh] overflow-y-auto rounded-lg border border-gray-200 dark:border-zinc-700 bg-white dark:bg-zinc-900 shadow-xl p-4">
                    <div className="flex items-start justify-between gap-4 mb-3">
                      <div>
                        <div className="text-sm font-bold text-gray-900 dark:text-zinc-100">
                          {SOURCE_LABELS[agentDetailKey] ?? agentDetailKey}
                        </div>
                        <div className="text-xs text-gray-500 dark:text-zinc-400">
                          {selectedDecisionRecord.supplier_key ?? "—"} · {selectedDecisionRecord.report_date ?? "—"}
                        </div>
                      </div>
                      <button
                        type="button"
                        onClick={() => setAgentDetailKey(null)}
                        className="text-gray-400 dark:text-zinc-500 hover:text-gray-600 dark:hover:text-zinc-300 text-2xl leading-none"
                        aria-label="Close"
                      >
                        &times;
                      </button>
                    </div>
                    {(() => {
                      const e = (selectedDecisionRecord.agent_scores ?? {})[agentDetailKey] as any;
                      const metricsRows = agentEntryMetrics(e);
                      const sortedMetrics = [...metricsRows].sort(
                        (a, b) => Number(metricTriggered(b)) - Number(metricTriggered(a))
                      );
                      const hasTriggeredMetric = sortedMetrics.some(metricTriggered);
                      return (
                        <div className="space-y-3 text-sm">
                          <div className="flex flex-wrap gap-3">
                            <div className="rounded border border-gray-200 dark:border-zinc-700 bg-gray-50 dark:bg-zinc-800 px-3 py-2">
                              <div className="text-[10px] uppercase tracking-wide text-gray-500 dark:text-zinc-400">Score</div>
                              <div className="text-lg font-bold text-gray-900 dark:text-zinc-100">{e?.score ?? "—"}</div>
                            </div>
                            <div className="rounded border border-gray-200 dark:border-zinc-700 bg-gray-50 dark:bg-zinc-800 px-3 py-2">
                              <div className="text-[10px] uppercase tracking-wide text-gray-500 dark:text-zinc-400">Flagged</div>
                              <div className="text-lg font-bold text-gray-900 dark:text-zinc-100">{e?.flagged ? "true" : "false"}</div>
                            </div>
                          </div>
                          {sortedMetrics.length > 0 && (
                            <div>
                              <div className="text-xs font-semibold text-gray-700 dark:text-zinc-200 mb-2">
                                {hasTriggeredMetric ? "Trigger metrics" : "Metrics"}
                              </div>
                              <ul className="space-y-2">
                                {sortedMetrics.map((m, idx) => {
                                  const id =
                                    m.metric_id != null
                                      ? String(m.metric_id)
                                      : m.name != null
                                        ? String(m.name)
                                        : `metric-${idx}`;
                                  const trig = metricTriggered(m);
                                  const sev =
                                    m.severity != null && String(m.severity) !== "NONE"
                                      ? String(m.severity)
                                      : null;
                                  const contrib = m.score_contribution;
                                  const contribStr =
                                    typeof contrib === "number" && Number.isFinite(contrib)
                                      ? String(contrib)
                                      : contrib != null
                                        ? String(contrib)
                                        : null;
                                  return (
                                    <li
                                      key={`${id}-${idx}`}
                                      className={`rounded-lg border px-3 py-2 ${
                                        trig
                                          ? "border-red-300 dark:border-red-800 bg-red-50/80 dark:bg-red-950/25"
                                          : "border-gray-200 dark:border-zinc-700 bg-gray-50 dark:bg-zinc-800/60"
                                      }`}
                                    >
                                      <div className="flex flex-wrap items-center gap-2 mb-1">
                                        <span className="font-mono text-xs font-semibold text-gray-900 dark:text-zinc-100">
                                          {id}
                                        </span>
                                        {trig ? (
                                          <span className="inline-flex rounded px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide bg-red-200 text-red-900 dark:bg-red-900/50 dark:text-red-100">
                                            Triggered
                                          </span>
                                        ) : null}
                                        {sev ? (
                                          <span className="text-[10px] text-gray-600 dark:text-zinc-400">{sev}</span>
                                        ) : null}
                                      </div>
                                      <div className="text-xs text-gray-700 dark:text-zinc-300">
                                        <span className="font-medium">Value:</span>{" "}
                                        {formatAgentMetricValue(m)}
                                        {contribStr != null && (
                                          <span className="ml-2 text-gray-500 dark:text-zinc-500">
                                            · contribution {contribStr}
                                          </span>
                                        )}
                                      </div>
                                      {m.explanation != null && String(m.explanation).trim() !== "" && (
                                        <p className="mt-1 text-xs text-gray-600 dark:text-zinc-400 leading-snug">
                                          {String(m.explanation)}
                                        </p>
                                      )}
                                    </li>
                                  );
                                })}
                              </ul>
                            </div>
                          )}
                          <div>
                            <div className="text-xs font-semibold text-gray-700 dark:text-zinc-200 mb-1">Reason</div>
                            <div className="text-sm text-gray-700 dark:text-zinc-300 whitespace-pre-wrap">
                              {e?.reason ? String(e.reason) : "—"}
                            </div>
                          </div>
                        </div>
                      );
                    })()}
                  </div>
                </div>
              )}

              {/* Decision Agent Reason */}
              <div className="mb-6">
                <h3 className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-2">Flag Reason</h3>
                <p className="text-sm text-gray-700 dark:text-zinc-300 whitespace-pre-wrap">
                  {selectedDecisionRecord.reason ?? "—"}
                </p>
              </div>
              </>
              )}

              {selectedConsolidatedRecord && (
              <>
              <div className="mb-6 pt-4">
                <h3 className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-2">
                  {SOURCE_LABELS[selectedConsolidatedRecord.source] ?? selectedConsolidatedRecord.source}
                </h3>
                <div className="flex flex-wrap items-center gap-3">
                  <div className="bg-gray-50 dark:bg-zinc-800 rounded-lg border border-gray-200 dark:border-zinc-700 px-3 py-2">
                    <div className="text-[10px] uppercase tracking-wide text-gray-500 dark:text-zinc-400">Risk score</div>
                    <div className="text-lg font-bold text-gray-900 dark:text-zinc-100">
                      {selectedConsolidatedRecord.overall_risk_score ?? "—"}
                    </div>
                  </div>
                  <div className="bg-gray-50 dark:bg-zinc-800 rounded-lg border border-gray-200 dark:border-zinc-700 px-3 py-2">
                    <div className="text-[10px] uppercase tracking-wide text-gray-500 dark:text-zinc-400">Review status</div>
                    <div className="text-lg font-bold text-gray-900 dark:text-zinc-100 capitalize">
                      {selectedConsolidatedRecord.status?.replace(/_/g, " ") ?? "—"}
                    </div>
                  </div>
                </div>
              </div>

              <div className="mb-6">
                <h3 className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-2">Flag reasons</h3>
                {Array.isArray(selectedConsolidatedRecord.reasons) && selectedConsolidatedRecord.reasons.length > 0 ? (
                  <ul className="list-disc list-inside space-y-1 text-sm text-gray-700 dark:text-zinc-300">
                    {selectedConsolidatedRecord.reasons.map((reason, i) => (
                      <li key={i} className="leading-snug">
                        {reason}
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="text-sm text-gray-500 dark:text-zinc-400">—</p>
                )}
              </div>

              {(() => {
                const metricsRows = consolidatedMetricsRows(selectedConsolidatedRecord);
                const sortedMetrics = [...metricsRows].sort(
                  (a, b) => Number(metricTriggered(b)) - Number(metricTriggered(a))
                );
                const hasTriggeredMetric = sortedMetrics.some(metricTriggered);
                if (sortedMetrics.length === 0) return null;
                return (
                  <div className="mb-6">
                    <div className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-2">
                      {hasTriggeredMetric ? "Trigger metrics" : "Metrics"}
                    </div>
                    <ul className="space-y-2">
                      {sortedMetrics.map((m, idx) => {
                        const id =
                          m.metric_id != null
                            ? String(m.metric_id)
                            : m.name != null
                              ? String(m.name)
                              : `metric-${idx}`;
                        const trig = metricTriggered(m);
                        const sev =
                          m.severity != null && String(m.severity) !== "NONE"
                            ? String(m.severity)
                            : null;
                        const contrib = m.score_contribution;
                        const contribStr =
                          typeof contrib === "number" && Number.isFinite(contrib)
                            ? String(contrib)
                            : contrib != null
                              ? String(contrib)
                              : null;
                        return (
                          <li
                            key={`${id}-${idx}`}
                            className={`rounded-lg border px-3 py-2 text-sm ${
                              trig
                                ? "border-red-300 dark:border-red-800 bg-red-50/80 dark:bg-red-950/25"
                                : "border-gray-200 dark:border-zinc-700 bg-gray-50 dark:bg-zinc-800/60"
                            }`}
                          >
                            <div className="flex flex-wrap items-center gap-2 mb-1">
                              <span className="font-mono text-xs font-semibold text-gray-900 dark:text-zinc-100">
                                {id}
                              </span>
                              {trig ? (
                                <span className="inline-flex rounded px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide bg-red-200 text-red-900 dark:bg-red-900/50 dark:text-red-100">
                                  Triggered
                                </span>
                              ) : null}
                              {sev ? (
                                <span className="text-[10px] text-gray-600 dark:text-zinc-400">{sev}</span>
                              ) : null}
                            </div>
                            <div className="text-xs text-gray-700 dark:text-zinc-300">
                              <span className="font-medium">Value:</span> {formatAgentMetricValue(m)}
                              {contribStr != null && (
                                <span className="ml-2 text-gray-500 dark:text-zinc-500">
                                  · contribution {contribStr}
                                </span>
                              )}
                            </div>
                            {m.explanation != null && String(m.explanation).trim() !== "" && (
                              <p className="mt-1 text-xs text-gray-600 dark:text-zinc-400 leading-snug">
                                {String(m.explanation)}
                              </p>
                            )}
                          </li>
                        );
                      })}
                    </ul>
                  </div>
                );
              })()}
              </>
              )}

              {/* Review Section — reviewers/admins can submit; viewers see history only */}
              <div className="border-t border-gray-200 dark:border-zinc-700 pt-4 space-y-4">
                <p className="text-sm text-gray-600 dark:text-zinc-400">
                  {selectedDecisionRecord
                    ? "Reviews are associated with this supplier and report date, and can target Decision Agent or a specific sub-agent."
                    : "Reviews are stored for this flagged record and listed by reviewer. One review per user per flag."}
                </p>

                <div>
                  <h3 className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-2">Review history</h3>
                  {supplierReviews.length === 0 ? (
                    <p className="text-sm text-gray-500 dark:text-zinc-400">No reviews yet.</p>
                  ) : (
                    <ul className="space-y-2">
                      {supplierReviews.map((r) => (
                        <li
                          key={r.id}
                          className="rounded-lg border border-gray-200 dark:border-zinc-700 bg-gray-50 dark:bg-zinc-800/80 p-3 text-sm"
                        >
                          <div className="flex flex-wrap justify-between gap-2 text-xs text-gray-500 dark:text-zinc-400">
                            <span className="font-medium text-gray-700 dark:text-zinc-300">
                              {r.reviewer_email}
                              {r.source?.trim() && (
                                <span className="ml-2 inline-flex items-center rounded border border-gray-200 dark:border-zinc-600 bg-white/60 dark:bg-zinc-900/40 px-1.5 py-0.5 text-[10px] text-gray-600 dark:text-zinc-300">
                                  {r.source}
                                </span>
                              )}
                            </span>
                            <span className="text-right">
                              <span className="block">{formatEastern(r.created_at)}</span>
                              {r.updated_at &&
                                r.updated_at.slice(0, 19) !== r.created_at.slice(0, 19) && (
                                  <span className="block text-[10px] text-gray-400 dark:text-zinc-500">
                                    Updated {formatEastern(r.updated_at)}
                                  </span>
                                )}
                            </span>
                          </div>
                          <div
                            className="mt-2 flex flex-wrap items-center gap-2"
                            role="group"
                            aria-label={`Verdict ${normalizeVerdictFromDb(r.verdict)}, follow-up ${followUpSummary(r)}`}
                          >
                            <VerdictIconBadge verdict={normalizeVerdictFromDb(r.verdict)} />
                            <FollowUpIconBadges r={r} />
                          </div>
                          <div className="text-gray-700 dark:text-zinc-300 mt-1">
                            <span className="font-medium">Comment:</span> {r.comment?.trim() ? r.comment : "—"}
                          </div>
                          {canReview &&
                            !editingReviewId &&
                            (isAdmin || sameReviewerId(r.reviewer_id, user.id)) && (
                            <div className="mt-2 flex flex-wrap gap-3">
                              <button
                                type="button"
                                onClick={() => startEditReview(r)}
                                className="text-xs font-medium text-blue-600 dark:text-blue-400 hover:underline"
                              >
                                {sameReviewerId(r.reviewer_id, user.id)
                                  ? "Edit my review"
                                  : "Edit review (admin)"}
                              </button>
                              <button
                                type="button"
                                onClick={() => deleteReview(r)}
                                className="text-xs font-medium text-red-600 dark:text-red-400 hover:underline"
                              >
                                {sameReviewerId(r.reviewer_id, user.id)
                                  ? "Delete my review"
                                  : "Delete review (admin)"}
                              </button>
                            </div>
                          )}
                          {editingReviewId === r.id && (
                            <p className="mt-2 text-xs text-blue-600 dark:text-blue-400">Editing below…</p>
                          )}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>

                <div>
                  <h3 className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-2">
                    {editingReviewId ? "Edit review" : "Add a review"}
                  </h3>
                  {!canReview ? (
                    <p className="text-sm text-gray-600 dark:text-zinc-400 mb-3">{NO_PERMISSION}</p>
                  ) : (
                    <>
                  {selectedDecisionRecord ? (
                    <div className="mb-3">
                      <label className={LABEL}>Agent</label>
                      <select
                        value={reviewAgentLabel}
                        onChange={(e) => setReviewAgentLabel(e.target.value)}
                        className={FIELD_FULL}
                      >
                        {Object.entries(SOURCE_LABELS).map(([k, label]) => (
                          <option key={k} value={label}>
                            {label}
                          </option>
                        ))}
                      </select>
                    </div>
                  ) : (
                    <p className="text-sm text-gray-600 dark:text-zinc-400 mb-3">
                      Agent for this review:{" "}
                      <span className="font-medium text-gray-800 dark:text-zinc-200">
                        {SOURCE_LABELS[selectedConsolidatedRecord!.source] ?? selectedConsolidatedRecord!.source}
                      </span>
                    </p>
                  )}
                  {hasMyReview && !editingReviewId && (
                    <p className="text-sm text-gray-600 dark:text-zinc-400 mb-3">
                      {selectedConsolidatedRecord
                        ? "You already submitted a review for this flag. Use Edit or Delete in the list above."
                        : "You already submitted a review for this agent. Use Edit or Delete in the list above."}{" "}
                      Submit is disabled until you delete that review.
                    </p>
                  )}
                  <div className="flex gap-4 mb-3 flex-wrap">
                    <label className="flex items-center gap-2 text-sm text-gray-900 dark:text-zinc-100 cursor-pointer">
                      <input
                        type="radio"
                        name={`verdict-${editingReviewId ?? "new"}`}
                        value={VERDICT_TRUE}
                        checked={reviewVerdict === VERDICT_TRUE}
                        onChange={() => {
                          setReviewVerdict(VERDICT_TRUE);
                          setReviewError("");
                        }}
                      />
                      True Positive
                    </label>
                    <label className="flex items-center gap-2 text-sm text-gray-900 dark:text-zinc-100 cursor-pointer">
                      <input
                        type="radio"
                        name={`verdict-${editingReviewId ?? "new"}`}
                        value={VERDICT_FALSE}
                        checked={reviewVerdict === VERDICT_FALSE}
                        onChange={() => {
                          setReviewVerdict(VERDICT_FALSE);
                          setReviewSuspended(false);
                          setReviewEmailed(false);
                          setReviewMonitored(false);
                          setReviewError("");
                        }}
                      />
                      False Positive
                    </label>
                  </div>
                  <div className="mb-3">
                    <p className="text-xs font-medium text-gray-600 dark:text-zinc-400 mb-2">
                      Follow-up {reviewVerdict === VERDICT_TRUE ? "(select at least one)" : "(not applicable)"}
                    </p>
                    <div className="flex flex-wrap gap-4">
                      {([
                        ["Suspended", reviewSuspended, setReviewSuspended] as const,
                        ["Emailed", reviewEmailed, setReviewEmailed] as const,
                        ["Monitored", reviewMonitored, setReviewMonitored] as const,
                      ]).map(([label, checked, setChecked]) => (
                        <label
                          key={label}
                          className={`flex items-center gap-2 text-sm text-gray-900 dark:text-zinc-100 ${
                            reviewVerdict === VERDICT_FALSE ? "opacity-50 cursor-not-allowed" : "cursor-pointer"
                          }`}
                        >
                          <input
                            type="checkbox"
                            checked={checked}
                            disabled={reviewVerdict === VERDICT_FALSE}
                            onChange={(e) => {
                              setChecked(e.target.checked);
                              setReviewError("");
                            }}
                          />
                          {label}
                        </label>
                      ))}
                    </div>
                  </div>
                  {reviewError && (
                    <p className="text-sm text-red-600 dark:text-red-400 mb-2">{reviewError}</p>
                  )}
                  <textarea
                    value={reviewComment}
                    onChange={(e) => setReviewComment(e.target.value)}
                    placeholder="Leave a comment..."
                    className={`w-full px-3 py-2 mb-3 ${FIELD}`}
                    rows={3}
                  />
                  <div className="flex flex-wrap gap-2">
                    {editingReviewId ? (
                      <>
                        <button
                          type="button"
                          onClick={saveReviewEdit}
                          className="px-4 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700"
                        >
                          Save changes
                        </button>
                        <button
                          type="button"
                          onClick={resetReviewForm}
                          className="px-4 py-2 border border-gray-300 dark:border-zinc-600 rounded text-sm text-gray-800 dark:text-zinc-200 hover:bg-gray-50 dark:hover:bg-zinc-800"
                        >
                          Cancel
                        </button>
                      </>
                    ) : (
                      <button
                        type="button"
                        onClick={submitReview}
                        disabled={hasMyReview}
                        className="px-4 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:bg-blue-600"
                      >
                        Submit review
                      </button>
                    )}
                  </div>
                    </>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}