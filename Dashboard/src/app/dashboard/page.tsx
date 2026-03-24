"use client";
import { useState, useEffect, useCallback } from "react";
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
  verdict: "correct_flag" | "incorrect_flag";
  comment: string | null;
  source: string;
  suspended: boolean;
  emailed: boolean;
  monitored: boolean;
};

function verdictLabel(v: "correct_flag" | "incorrect_flag") {
  return v === "correct_flag" ? "True Positive" : "False Positive";
}

function followUpSummary(r: SupplierReview) {
  const parts: string[] = [];
  if (r.suspended) parts.push("Suspended");
  if (r.emailed) parts.push("Emailed");
  if (r.monitored) parts.push("Monitored");
  return parts.length ? parts.join(", ") : "—";
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

  const [selectedRecord, setSelectedRecord] = useState<FlaggedRecord | null>(null);
  const [riskHistory, setRiskHistory] = useState<any[]>([]);
  const [reviewComment, setReviewComment] = useState("");
  const [reviewVerdict, setReviewVerdict] = useState<"correct_flag" | "incorrect_flag">("correct_flag");
  const [reviewSuspended, setReviewSuspended] = useState(false);
  const [reviewEmailed, setReviewEmailed] = useState(false);
  const [reviewMonitored, setReviewMonitored] = useState(false);
  const [reviewError, setReviewError] = useState("");
  const [supplierReviews, setSupplierReviews] = useState<SupplierReview[]>([]);
  const [editingReviewId, setEditingReviewId] = useState<string | null>(null);
  const [hoveredAgent, setHoveredAgent] = useState<string | null>(null);

  useEffect(() => {
    supabase.auth.getUser().then(({ data }) => {
      if (!data.user) router.push("/login");
      else setUser(data.user);
    });
    loadAgentMeta();
  }, []);

  useEffect(() => {
    if (user) loadRecords();
  }, [user, dateFilter, sourceFilter, searchTerm, scoreMin, scoreMax, statusFilter]);

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
      .order("overall_risk_score", { ascending: false });

    if (sourceFilter !== "all") query = query.eq("source", sourceFilter);
    if (statusFilter !== "all") query = query.eq("status", statusFilter);
    if (searchTerm) query = query.or(`supplier_name.ilike.%${searchTerm}%,supplier_key.ilike.%${searchTerm}%`);

    const { data, error } = await query.limit(200);
    if (error) console.error("Load error:", error);
    setRecords((data as FlaggedRecord[]) ?? []);
    setLoading(false);
  }, [dateFilter, sourceFilter, searchTerm, scoreMin, scoreMax, statusFilter]);

  function resetReviewForm() {
    setReviewComment("");
    setReviewVerdict("correct_flag");
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
    if (reviewVerdict === "correct_flag" && !anyFollowUp) {
      setReviewError("For True Positive, select at least one: Suspended, Emailed, or Monitored.");
      return false;
    }
    if (reviewVerdict === "incorrect_flag" && anyFollowUp) {
      setReviewError("False Positive cannot include follow-up actions.");
      return false;
    }
    return true;
  }

  function startEditReview(r: SupplierReview) {
    setEditingReviewId(r.id);
    setReviewVerdict(r.verdict);
    setReviewComment(r.comment ?? "");
    setReviewSuspended(r.suspended);
    setReviewEmailed(r.emailed);
    setReviewMonitored(r.monitored);
    setReviewError("");
  }

  async function saveReviewEdit() {
    if (!selectedRecord || !user || !editingReviewId) return;
    setReviewError("");
    if (!validateReviewFields()) return;

    const { data: updatedRows, error } = await supabase
      .from("supplier_reviews")
      .update({
        verdict: reviewVerdict,
        comment: reviewComment,
        suspended: reviewSuspended,
        emailed: reviewEmailed,
        monitored: reviewMonitored,
        updated_at: new Date().toISOString(),
      })
      .eq("id", editingReviewId)
      .eq("reviewer_id", user.id)
      .select("id");

    if (error) {
      setReviewError(error.message);
      return;
    }
    if (!updatedRows?.length) {
      setReviewError("Update failed (no row updated). Check permissions or try again.");
      return;
    }

    await loadReviewsForFlag(selectedRecord.id);
    resetReviewForm();
  }

  async function deleteReview(review: SupplierReview) {
    if (!selectedRecord || !user || review.reviewer_id !== user.id) return;
    if (!window.confirm("Delete this review? This cannot be undone.")) return;
    setReviewError("");
    const { error } = await supabase
      .from("supplier_reviews")
      .delete()
      .eq("id", review.id)
      .eq("reviewer_id", user.id);
    if (error) {
      setReviewError(error.message);
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
    if (supplierReviews.some((r) => r.reviewer_id === user.id)) {
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
      setReviewError(insertError.message);
      return;
    }

    await syncConsolidatedReviewersFromReviews(selectedRecord.id);

    await loadReviewsForFlag(selectedRecord.id);
    resetReviewForm();
    loadRecords();
  }

  function exportCSV() {
    const headers = ["Report Date", "Supplier Key", "Supplier Name", "Risk Score", "Source", "Status", "Reasons"];
    const rows = records.map((r) => {
      const normalized = r.created_at.includes("T") ? r.created_at : r.created_at.replace(" ", "T");
      const d = new Date(normalized);
      const reportDate = Number.isNaN(d.getTime()) ? r.created_at.slice(0, 10) : easternDateYmd(d);
      return [
        reportDate,
        r.supplier_key,
        r.supplier_name,
        r.overall_risk_score,
        SOURCE_LABELS[r.source] ?? r.source,
        r.status,
        Array.isArray(r.reasons) ? r.reasons.join("; ") : "",
      ];
    });
    const csv = [headers, ...rows].map((r) => r.map((c) => `"${c}"`).join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `flagged_suppliers_${dateFilter}.csv`;
    a.click();
  }

  async function handleLogout() {
    await supabase.auth.signOut();
    router.push("/login");
  }

  if (!user) return null;

  const hasMyReview = supplierReviews.some((r) => r.reviewer_id === user.id);

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-zinc-950">
      <header className="bg-white dark:bg-zinc-900 border-b border-gray-200 dark:border-zinc-800 px-6 py-4 flex justify-between items-center gap-4">
        <h1 className="text-xl font-bold text-gray-900 dark:text-zinc-100">Payability Risk Dashboard</h1>
        <div className="flex items-center gap-4 flex-wrap justify-end">
          <ThemeToggle />
          <span className="text-sm text-gray-500 dark:text-zinc-400">{user.email}</span>
          <button onClick={handleLogout} className="text-sm text-red-600 dark:text-red-400 hover:underline">
            Sign Out
          </button>
        </div>
      </header>

      <div className="p-6 max-w-7xl mx-auto">
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
            <button onClick={exportCSV}
              className="w-full px-3 py-1.5 bg-green-600 text-white rounded text-sm hover:bg-green-700">
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
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-zinc-400">Date</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-zinc-400">Supplier</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-zinc-400">Key</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-zinc-400">Score</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-zinc-400">Flagged By</th>
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

              {/* Review Section — all users can add reviews; list shows history */}
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
                          <div className="mt-1 text-gray-900 dark:text-zinc-100">
                            <span className="font-medium">Verdict:</span> {verdictLabel(r.verdict)}
                          </div>
                          <div className="text-gray-700 dark:text-zinc-300">
                            <span className="font-medium">Follow-up:</span> {followUpSummary(r)}
                          </div>
                          <div className="text-gray-700 dark:text-zinc-300 mt-1">
                            <span className="font-medium">Comment:</span> {r.comment?.trim() ? r.comment : "—"}
                          </div>
                          {user?.id === r.reviewer_id && !editingReviewId && (
                            <div className="mt-2 flex flex-wrap gap-3">
                              <button
                                type="button"
                                onClick={() => startEditReview(r)}
                                className="text-xs font-medium text-blue-600 dark:text-blue-400 hover:underline"
                              >
                                Edit my review
                              </button>
                              <button
                                type="button"
                                onClick={() => deleteReview(r)}
                                className="text-xs font-medium text-red-600 dark:text-red-400 hover:underline"
                              >
                                Delete my review
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
                    {editingReviewId ? "Edit your review" : "Add a review"}
                  </h3>
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
                        value="correct_flag"
                        checked={reviewVerdict === "correct_flag"}
                        onChange={() => {
                          setReviewVerdict("correct_flag");
                          setReviewError("");
                        }}
                      />
                      True Positive
                    </label>
                    <label className="flex items-center gap-2 text-sm text-gray-900 dark:text-zinc-100 cursor-pointer">
                      <input
                        type="radio"
                        name={`verdict-${editingReviewId ?? "new"}`}
                        value="incorrect_flag"
                        checked={reviewVerdict === "incorrect_flag"}
                        onChange={() => {
                          setReviewVerdict("incorrect_flag");
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
                      Follow-up {reviewVerdict === "correct_flag" ? "(select at least one)" : "(not applicable)"}
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
                            reviewVerdict === "incorrect_flag" ? "opacity-50 cursor-not-allowed" : "cursor-pointer"
                          }`}
                        >
                          <input
                            type="checkbox"
                            checked={checked}
                            disabled={reviewVerdict === "incorrect_flag"}
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
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}