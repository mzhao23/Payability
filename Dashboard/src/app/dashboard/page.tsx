"use client";
import { useState, useEffect, useCallback, type ReactNode } from "react";
import { getSupabaseBrowser } from "@/lib/supabase-browser";
import { EASTERN_TZ, easternDateYmd, easternYmdToUtcRange } from "@/lib/eastern-date";
import { useRouter } from "next/navigation";
import { ThemeToggle } from "@/components/ThemeToggle";

type FlaggedRecord = {
  id: number;
  supplier_key: string;
  supplier_name: string;
  source: string;
  overall_risk_score: number;
  reasons: string[];
  metrics: any[];
  status: string;
  created_at: string;
  /** After DB migration to uuid[]: array of reviewer user ids; legacy rows may be a single string. */
  reviewed_by: string | string[] | null;
  reviewed_at: string | null;
};

type SupplierReview = {
  id: string;
  created_at: string;
  updated_at: string;
  flagged_record_id: number;
  supplier_key: string;
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
  daily_summary_report: "Daily Summary Agent",
  ship_tracking: "Shipment Agent",
  json_report: "JSON Agent",
  health_report: "Health Agent",
  decision_agent: "Decision Agent",
};

const FIELD =
  "border border-gray-300 dark:border-zinc-600 rounded text-sm text-gray-900 dark:text-zinc-100 bg-white dark:bg-zinc-800 placeholder:text-gray-600 dark:placeholder:text-zinc-400";
const FIELD_FULL = `w-full px-2 py-1.5 ${FIELD}`;
const FIELD_NARROW = `w-14 px-2 py-1.5 ${FIELD}`;
const LABEL = "block text-xs text-gray-500 dark:text-zinc-400 mb-1";

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

const TABLE_ORDER_COLUMN: Record<TableSortColumn, string> = {
  date: "created_at",
  supplier: "supplier_name",
  key: "supplier_key",
  score: "overall_risk_score",
  flagged_by: "source",
};

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
  const [records, setRecords] = useState<FlaggedRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [agentMeta, setAgentMeta] = useState<Record<string, AgentMeta>>({});

  const [dateFilter, setDateFilter] = useState(() => easternDateYmd());
  const [sourceFilter, setSourceFilter] = useState("all");
  const [searchTerm, setSearchTerm] = useState("");
  const [scoreMin, setScoreMin] = useState(1);
  const [scoreMax, setScoreMax] = useState(10);
  const [statusFilter, setStatusFilter] = useState("all");
  const [sortColumn, setSortColumn] = useState<TableSortColumn>("score");
  const [sortAscending, setSortAscending] = useState(false);

  const [selectedRecord, setSelectedRecord] = useState<FlaggedRecord | null>(null);
  const [riskHistory, setRiskHistory] = useState<any[]>([]);
  const [reviewComment, setReviewComment] = useState("");
  const [reviewVerdict, setReviewVerdict] = useState<ReviewVerdict>(VERDICT_TRUE);
  const [reviewSuspended, setReviewSuspended] = useState(false);
  const [reviewEmailed, setReviewEmailed] = useState(false);
  const [reviewMonitored, setReviewMonitored] = useState(false);
  const [reviewError, setReviewError] = useState("");
  const [supplierReviews, setSupplierReviews] = useState<SupplierReview[]>([]);
  const [editingReviewId, setEditingReviewId] = useState<string | null>(null);
  const [hoveredAgent, setHoveredAgent] = useState<string | null>(null);
  const [appRole, setAppRole] = useState<AppRole | null>(null);

  const canReview = appRole === "reviewer" || appRole === "admin";
  const isAdmin = appRole === "admin";

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
  }, [user, dateFilter, sourceFilter, searchTerm, scoreMin, scoreMax, statusFilter, sortColumn, sortAscending]);

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
    const { startIso, endIso } = easternYmdToUtcRange(dateFilter);
    let query = supabase
      .from("consolidated_flagged_supplier_list")
      .select("*")
      .gte("created_at", startIso)
      .lte("created_at", endIso)
      .gte("overall_risk_score", scoreMin)
      .lte("overall_risk_score", scoreMax)
      .order(TABLE_ORDER_COLUMN[sortColumn], { ascending: sortAscending });

    if (sourceFilter !== "all") query = query.eq("source", sourceFilter);
    if (statusFilter !== "all") query = query.eq("status", statusFilter);
    if (searchTerm) query = query.or(`supplier_name.ilike.%${searchTerm}%,supplier_key.ilike.%${searchTerm}%`);

    const { data, error } = await query.limit(200);
    if (error) console.error("Load error:", error);
    setRecords((data as FlaggedRecord[]) ?? []);
    setLoading(false);
  }, [dateFilter, sourceFilter, searchTerm, scoreMin, scoreMax, statusFilter, sortColumn, sortAscending]);

  function requestTableSort(column: TableSortColumn) {
    if (column === sortColumn) {
      setSortAscending((a) => !a);
    } else {
      setSortColumn(column);
      const defaultAsc = column === "supplier" || column === "key" || column === "flagged_by";
      setSortAscending(defaultAsc);
    }
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
    setSelectedRecord(null);
    setSupplierReviews([]);
    resetReviewForm();
  }

  async function loadReviewsForFlag(flaggedRecordId: number) {
    const { data, error } = await supabase
      .from("supplier_reviews")
      .select("*")
      .eq("flagged_record_id", flaggedRecordId)
      .order("created_at", { ascending: true });
    if (error) {
      console.error("Load reviews error:", error);
      setSupplierReviews([]);
      return;
    }
    setSupplierReviews((data as SupplierReview[]) ?? []);
  }

  /** Set consolidated row from actual supplier_reviews (distinct reviewer_id). Requires reviewed_by uuid[]. */
  async function syncConsolidatedReviewersFromReviews(flaggedRecordId: number) {
    const { data: rows, error } = await supabase
      .from("supplier_reviews")
      .select("reviewer_id")
      .eq("flagged_record_id", flaggedRecordId);
    if (error) {
      console.error("Sync reviewers error:", error);
      return;
    }
    const ids = Array.from(new Set((rows ?? []).map((r) => String(r.reviewer_id)).filter(Boolean)));
    if (ids.length === 0) {
      const { error: uerr } = await supabase
        .from("consolidated_flagged_supplier_list")
        .update({
          status: "pending_review",
          reviewed_by: null,
          reviewed_at: null,
        })
        .eq("id", flaggedRecordId);
      if (uerr) console.error("Consolidated update error:", uerr);
    } else {
      const { error: uerr } = await supabase
        .from("consolidated_flagged_supplier_list")
        .update({
          status: "reviewed",
          reviewed_by: ids,
          reviewed_at: new Date().toISOString(),
        })
        .eq("id", flaggedRecordId);
      if (uerr) console.error("Consolidated update error:", uerr);
    }

    const { data: updatedRow, error: refetchError } = await supabase
      .from("consolidated_flagged_supplier_list")
      .select("*")
      .eq("id", flaggedRecordId)
      .single();
    if (!refetchError && updatedRow) {
      setSelectedRecord((prev) => (prev?.id === flaggedRecordId ? (updatedRow as FlaggedRecord) : prev));
    }
  }

  async function openDetail(record: FlaggedRecord) {
    setSelectedRecord(record);
    setSupplierReviews([]);
    resetReviewForm();

    await loadReviewsForFlag(record.id);

    const { data } = await supabase
      .from("consolidated_flagged_supplier_list")
      .select("overall_risk_score, source, created_at")
      .eq("supplier_key", record.supplier_key)
      .order("created_at", { ascending: true })
      .limit(50);
    setRiskHistory(data ?? []);
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
  }

  async function saveReviewEdit() {
    if (!selectedRecord || !user || !editingReviewId) return;
    setReviewError("");
    if (!canReview) {
      setReviewError(NO_PERMISSION);
      return;
    }
    if (!validateReviewFields()) return;

    let q = supabase
      .from("supplier_reviews")
      .update({
        verdict: reviewVerdict,
        comment: reviewComment,
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

    await loadReviewsForFlag(selectedRecord.id);
    resetReviewForm();
  }

  async function deleteReview(review: SupplierReview) {
    if (!selectedRecord || !user) return;
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
    await loadReviewsForFlag(selectedRecord.id);
    await syncConsolidatedReviewersFromReviews(selectedRecord.id);
    loadRecords();
  }

  async function submitReview() {
    if (!selectedRecord || !user || editingReviewId) return;

    setReviewError("");
    if (!canReview) {
      setReviewError(NO_PERMISSION);
      return;
    }
    if (supplierReviews.some((r) => sameReviewerId(r.reviewer_id, user.id))) {
      setReviewError("You already have a review for this flag. Edit or delete it first.");
      return;
    }
    if (!validateReviewFields()) return;

    const { error: insertError } = await supabase.from("supplier_reviews").insert({
      flagged_record_id: selectedRecord.id,
      supplier_key: selectedRecord.supplier_key,
      reviewer_id: user.id,
      reviewer_email: user.email,
      verdict: reviewVerdict,
      comment: reviewComment,
      source: selectedRecord.source,
      suspended: reviewSuspended,
      emailed: reviewEmailed,
      monitored: reviewMonitored,
    });

    if (insertError) {
      setReviewError(mapSupabasePermissionError(insertError));
      return;
    }

    await syncConsolidatedReviewersFromReviews(selectedRecord.id);

    await loadReviewsForFlag(selectedRecord.id);
    resetReviewForm();
    loadRecords();
  }

  async function exportCSV() {
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

    const ids = records.map((r) => r.id);
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
      const fid = rev.flagged_record_id;
      const list = byFlag.get(fid);
      if (list) list.push(rev);
      else byFlag.set(fid, [rev]);
    }
    for (const list of byFlag.values()) {
      list.sort((a, b) => a.created_at.localeCompare(b.created_at));
    }

    const rows: unknown[][] = [];
    for (const r of records) {
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
    a.download = `flagged_suppliers_${dateFilter}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  async function handleLogout() {
    await supabase.auth.signOut();
    router.push("/login");
  }

  if (!user || appRole === null) return null;

  const hasMyReview = supplierReviews.some((r) => sameReviewerId(r.reviewer_id, user.id));

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
        <div className="bg-white dark:bg-zinc-900 rounded-lg shadow border border-gray-200 dark:border-zinc-800 p-4 mb-6 grid grid-cols-2 md:grid-cols-6 gap-3">
          <div>
            <label className={LABEL}>Date</label>
            <input type="date" value={dateFilter} onChange={(e) => setDateFilter(e.target.value)}
              className={FIELD_FULL} />
          </div>
          <div>
            <label className={LABEL}>Agent</label>
            <select value={sourceFilter} onChange={(e) => setSourceFilter(e.target.value)}
              className={FIELD_FULL}>
              <option value="all">All Agents</option>
              {Object.entries(SOURCE_LABELS).map(([key, label]) => (
                <option key={key} value={key}>{label}</option>
              ))}
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
          <div>
            <label className={LABEL}>Status</label>
            <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}
              className={FIELD_FULL}>
              <option value="all">All</option>
              <option value="pending_review">Pending Review</option>
              <option value="reviewed">Reviewed</option>
            </select>
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

        {/* Summary cards */}
        <div className="grid grid-cols-4 gap-4 mb-6">
          <div className="bg-white dark:bg-zinc-900 rounded-lg shadow border border-gray-200 dark:border-zinc-800 p-4 text-center">
            <div className="text-2xl font-bold text-gray-900 dark:text-zinc-100">{records.length}</div>
            <div className="text-xs text-gray-500 dark:text-zinc-400">Flagged Total</div>
          </div>
          <div className="bg-white dark:bg-zinc-900 rounded-lg shadow border border-gray-200 dark:border-zinc-800 p-4 text-center">
            <div className="text-2xl font-bold text-red-600 dark:text-red-400">
              {records.filter((r) => r.overall_risk_score >= 8).length}
            </div>
            <div className="text-xs text-gray-500 dark:text-zinc-400">Critical (8-10)</div>
          </div>
          <div className="bg-white dark:bg-zinc-900 rounded-lg shadow border border-gray-200 dark:border-zinc-800 p-4 text-center">
            <div className="text-2xl font-bold text-yellow-600 dark:text-yellow-500">
              {records.filter((r) => r.status === "pending_review").length}
            </div>
            <div className="text-xs text-gray-500 dark:text-zinc-400">Pending Review</div>
          </div>
          <div className="bg-white dark:bg-zinc-900 rounded-lg shadow border border-gray-200 dark:border-zinc-800 p-4 text-center">
            <div className="text-2xl font-bold text-green-600 dark:text-green-400">
              {records.filter((r) => r.status === "reviewed").length}
            </div>
            <div className="text-xs text-gray-500 dark:text-zinc-400">Reviewed</div>
          </div>
        </div>

        {/* Table */}
        <div className="bg-white dark:bg-zinc-900 rounded-lg shadow border border-gray-200 dark:border-zinc-800 overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 dark:bg-zinc-800 border-b border-gray-200 dark:border-zinc-700">
              <tr>
                <TableSortHeader
                  label="Date"
                  column="date"
                  activeColumn={sortColumn}
                  ascending={sortAscending}
                  onRequestSort={requestTableSort}
                />
                <TableSortHeader
                  label="Supplier"
                  column="supplier"
                  activeColumn={sortColumn}
                  ascending={sortAscending}
                  onRequestSort={requestTableSort}
                />
                <TableSortHeader
                  label="Key"
                  column="key"
                  activeColumn={sortColumn}
                  ascending={sortAscending}
                  onRequestSort={requestTableSort}
                />
                <TableSortHeader
                  label="Score"
                  column="score"
                  activeColumn={sortColumn}
                  ascending={sortAscending}
                  onRequestSort={requestTableSort}
                />
                <TableSortHeader
                  label="Flagged By"
                  column="flagged_by"
                  activeColumn={sortColumn}
                  ascending={sortAscending}
                  onRequestSort={requestTableSort}
                />
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-zinc-400">Reason</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-zinc-400">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-zinc-700">
              {loading ? (
                <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400 dark:text-zinc-500">Loading...</td></tr>
              ) : records.length === 0 ? (
                <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400 dark:text-zinc-500">No records found</td></tr>
              ) : (
                records.map((r) => (
                  <tr key={r.id} onClick={() => openDetail(r)} className="hover:bg-blue-50 dark:hover:bg-zinc-800 cursor-pointer">
                    <td className="px-4 py-3 text-gray-600 dark:text-zinc-300">{formatEastern(r.created_at)}</td>
                    <td className="px-4 py-3 font-medium text-gray-900 dark:text-zinc-100">{r.supplier_name}</td>
                    <td className="px-4 py-3 text-gray-500 dark:text-zinc-400 font-mono text-xs">{r.supplier_key.slice(0, 8)}...</td>
                    <td className="px-4 py-3">
                      <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-bold ${
                        r.overall_risk_score >= 8 ? "bg-red-100 text-red-800 dark:bg-red-950/50 dark:text-red-300" :
                        r.overall_risk_score >= 5 ? "bg-yellow-100 text-yellow-800 dark:bg-yellow-950/40 dark:text-yellow-300" :
                        "bg-green-100 text-green-800 dark:bg-green-950/50 dark:text-green-300"
                      }`}>{r.overall_risk_score}</span>
                    </td>
                    <td className="px-4 py-3 relative">
                      <span className="text-blue-600 dark:text-blue-400 underline cursor-help"
                        onMouseEnter={() => setHoveredAgent(r.source)}
                        onMouseLeave={() => setHoveredAgent(null)}>
                        {SOURCE_LABELS[r.source] ?? r.source}
                      </span>
                      {hoveredAgent === r.source && agentMeta[r.source] && (
                        <div className="absolute z-50 bg-gray-900 text-white p-3 rounded-lg text-xs w-72 left-0 top-full mt-1 shadow-lg">
                          <div className="font-bold mb-1">{agentMeta[r.source].display_name}</div>
                          <div className="mb-1">{agentMeta[r.source].description}</div>
                          <div className="text-gray-300">Rules: {agentMeta[r.source].active_rules}</div>
                          <div className="text-gray-400 mt-1">Updated: {formatEastern(agentMeta[r.source].last_updated)}</div>
                        </div>
                      )}
                    </td>
                    <td className="px-4 py-3 text-gray-600 dark:text-zinc-300 text-xs max-w-xs truncate">
                      {Array.isArray(r.reasons) ? r.reasons[0] : ""}
                    </td>
                    <td className="px-4 py-3">
                      <span className={`inline-flex px-2 py-0.5 rounded text-xs ${
                        r.status === "reviewed"
                          ? "bg-green-100 text-green-700 dark:bg-green-950/50 dark:text-green-300"
                          : "bg-orange-100 text-orange-700 dark:bg-orange-950/40 dark:text-orange-300"
                      }`}>{r.status === "reviewed" ? "Reviewed" : "Pending"}</span>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Detail Modal */}
      {selectedRecord && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-white dark:bg-zinc-900 rounded-lg shadow-xl w-full max-w-3xl max-h-[90vh] overflow-y-auto border border-gray-200 dark:border-zinc-800">
            <div className="p-6">
              <div className="flex justify-between items-start mb-4">
                <div>
                  <h2 className="text-lg font-bold text-gray-900 dark:text-zinc-100">{selectedRecord.supplier_name}</h2>
                  <p className="text-sm text-gray-500 dark:text-zinc-400 font-mono">{selectedRecord.supplier_key}</p>
                </div>
                <button type="button" onClick={closeDetail} className="text-gray-400 dark:text-zinc-500 hover:text-gray-600 dark:hover:text-zinc-300 text-2xl leading-none">&times;</button>
              </div>

              {/* Risk Score Trend */}
              <div className="mb-6">
                <h3 className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-2">Risk Score Trend</h3>
                <div className="flex items-end gap-1 h-24 bg-gray-50 dark:bg-zinc-800 rounded p-2">
                  {riskHistory.map((h: any, i: number) => (
                    <div key={i} className="flex flex-col items-center flex-1">
                      <div className={`w-full rounded-t ${
                        h.overall_risk_score >= 8 ? "bg-red-500" :
                        h.overall_risk_score >= 5 ? "bg-yellow-500" : "bg-green-500"
                      }`} style={{ height: `${(h.overall_risk_score / 10) * 80}px` }}
                      title={`${h.source}: ${h.overall_risk_score} (${formatEastern(h.created_at, { dateOnly: true })})`} />
                      <span className="text-[9px] text-gray-400 dark:text-zinc-500 mt-1">{formatEastern(h.created_at, { dateOnly: true })}</span>
                    </div>
                  ))}
                  {riskHistory.length === 0 && <span className="text-gray-400 dark:text-zinc-500 text-xs m-auto">No history</span>}
                </div>
              </div>

              {/* Agent Scores Breakdown */}
              <div className="mb-6">
                <h3 className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-2">Agent Scores</h3>
                <div className="grid grid-cols-4 gap-2">
                  {Object.entries(SOURCE_LABELS).map(([src, label]) => {
                    const latest = riskHistory.filter((h: any) => h.source === src).slice(-1)[0];
                    return (
                      <div key={src} className="bg-gray-50 dark:bg-zinc-800 rounded p-2 text-center">
                        <div className="text-xs text-gray-500 dark:text-zinc-400">{label}</div>
                        <div className={`text-lg font-bold ${
                          latest?.overall_risk_score >= 8 ? "text-red-600 dark:text-red-400" :
                          latest?.overall_risk_score >= 5 ? "text-yellow-600 dark:text-yellow-500" :
                          latest ? "text-green-600 dark:text-green-400" : "text-gray-300 dark:text-zinc-600"
                        }`}>{latest?.overall_risk_score ?? "—"}</div>
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* Triggered Metrics */}
              <div className="mb-6">
                <h3 className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-2">Triggered Metrics</h3>
                <div className="space-y-2">
                  {(Array.isArray(selectedRecord.metrics) ? selectedRecord.metrics : []).map((m: any, i: number) => (
                    <div key={i} className="bg-gray-50 dark:bg-zinc-800 rounded p-2 flex justify-between text-sm">
                      <span className="font-mono text-gray-700 dark:text-zinc-300">{m.metric_id}</span>
                      <span className="text-gray-900 dark:text-zinc-100 font-medium">
                        {m.value != null
                          ? m.unit != null && String(m.unit).trim() !== ""
                            ? `${m.value} ${m.unit}`
                            : `${m.value}`
                          : "N/A"}
                      </span>
                    </div>
                  ))}
                </div>
              </div>

              {/* Full Reasons */}
              <div className="mb-6">
                <h3 className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-2">Flag Reasons</h3>
                <ul className="text-sm text-gray-600 dark:text-zinc-300 space-y-1">
                  {(Array.isArray(selectedRecord.reasons) ? selectedRecord.reasons : []).map((r: string, i: number) => (
                    <li key={i} className="flex gap-2">
                      <span className="text-red-500 dark:text-red-400 flex-shrink-0">•</span>
                      <span>{r}</span>
                    </li>
                  ))}
                </ul>
              </div>

              {/* Review Section — reviewers/admins can submit; viewers see history only */}
              <div className="border-t border-gray-200 dark:border-zinc-700 pt-4 space-y-4">
                {selectedRecord.status === "reviewed" ? (
                  <p className="text-sm text-green-600 dark:text-green-400">
                    Flag marked reviewed (last update {formatEastern(selectedRecord.reviewed_at)})
                  </p>
                ) : (
                  <p className="text-sm text-orange-600 dark:text-orange-400">
                    Pending review — other reviewers can add their review below (one review per person).
                  </p>
                )}

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
                            <span className="font-medium text-gray-700 dark:text-zinc-300">{r.reviewer_email}</span>
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
                  {hasMyReview && !editingReviewId && (
                    <p className="text-sm text-gray-600 dark:text-zinc-400 mb-3">
                      You already submitted a review for this flag. Use <strong>Edit</strong> or <strong>Delete</strong>{" "}
                      in the list above to change it. Submit is disabled until you delete that review.
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