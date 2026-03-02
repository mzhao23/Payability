export const runtime = "nodejs";
export const dynamic = "force-dynamic";

import { NextResponse } from "next/server";
import { getDailyChangeData } from "@/lib/bigquery";
import { flagSuppliers } from "@/lib/risk-engine";
import { generateRiskReport } from "@/lib/ai-report";
import { supabaseAdmin } from "@/lib/supabase-admin";

export async function GET() {
  const start = Date.now();

  try {
    console.log("[cron/daily-risk] start");

    // 1) 拉数据
    const rows = await getDailyChangeData();

    // 2) 风险引擎
    const result = flagSuppliers(rows);

    // 3) AI 报告（限制 20 条）
    const topFlagged = result.flagged.slice(0, 20);
    const report = await generateRiskReport(topFlagged);

    // 4) 写入 agent_runs
    const { data: run, error: runErr } = await supabaseAdmin
      .from("agent_runs")
      .insert({
        report_date: new Date().toISOString(),
        total_suppliers: result.total,
        flagged_count: result.flagged.length,
        ai_report: report,
        debug: {
          rows_length: rows.length,
          execution_time_ms: Date.now() - start,
        },
      })
      .select("id")
      .single();

    if (runErr) throw runErr;

    // 4b) 写入 flagged 明细（top 20）
    if (topFlagged.length > 0) {
      const detailRows = topFlagged.map((s: any) => ({
        run_id: run.id,
        supplier_key: s.supplier_key,
        supplier_name: s.supplier_name,
        metrics: s,
        reasons: s.flag_reasons ?? [],
      }));

      const { error: detailErr } = await supabaseAdmin
        .from("agent_flagged_suppliers")
        .insert(detailRows);

      if (detailErr) throw detailErr;
    }

    console.log("[cron/daily-risk] done", {
      run_id: run.id,
      total: result.total,
      flagged: result.flagged.length,
      ms: Date.now() - start,
    });

    return NextResponse.json({
      ok: true,
      run_id: run.id,
      report_date: new Date().toISOString(),
      summary: {
        total_suppliers: result.total,
        flagged_count: result.flagged.length,
        saved_to_db: true,
        saved_flagged: topFlagged.length,
      },
      debug: {
        rows_length: rows.length,
        execution_time_ms: Date.now() - start,
      },
    });
  } catch (e: any) {
    console.error("[cron/daily-risk] error", e);
    return NextResponse.json(
      { ok: false, error: e?.message ?? String(e) },
      { status: 500 }
    );
  }
}