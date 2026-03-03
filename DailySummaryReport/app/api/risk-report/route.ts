// app/api/risk-report/route.ts
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

import { NextResponse } from "next/server";
import { getDailyChangeData } from "@/lib/bigquery";
import { flagSuppliers } from "@/lib/risk-engine";
import type { DailyChangeRow } from "@/lib/risk-engine";
import { generateRiskReportJSON } from "@/lib/ai-report";

export async function GET() {
  const start = Date.now();
  const reportDateIso = new Date().toISOString();

  try {
    console.log("[risk-report] START", reportDateIso);

    // 1) Query BigQuery
    console.log("[risk-report] Querying BigQuery...");
    const rowsRaw = await getDailyChangeData();
    const rows = (Array.isArray(rowsRaw) ? rowsRaw : []) as DailyChangeRow[];

    console.log("[risk-report] BigQuery done", {
      rows_length: rows.length,
      ms: Date.now() - start,
    });

    // 2) Run risk engine
    console.log("[risk-report] Running risk engine...");
    const result = flagSuppliers(rows);

    console.log("[risk-report] Risk engine done", {
      total: result.total,
      flagged: result.flagged.length,
      unflagged: result.unflagged.length,
      ms: Date.now() - start,
    });

    // 3) Generate AI report JSON (limit top 20)
    const topFlagged = result.flagged.slice(0, 20);

    console.log("[risk-report] Generating AI report JSON...");
    const aiReportJson = await generateRiskReportJSON(topFlagged);

    console.log("[risk-report] AI JSON done", {
      suppliers_reviewed: aiReportJson?.suppliers_reviewed ?? topFlagged.length,
      ms: Date.now() - start,
    });

    // 4) Return structured JSON
    return NextResponse.json({
      success: true,
      report_date: reportDateIso,

      debug: {
        rows_length: rows.length,
        sample_rows: rows.slice(0, 3),
        execution_time_ms: Date.now() - start,
      },

      summary: {
        total_suppliers: result.total,
        flagged_count: result.flagged.length,
        unflagged_count: result.unflagged.length,
      },

      // ✅ structured report for dashboard rendering
      ai_report_json: aiReportJson,

      // keep a small slice for quick inspection
      flagged_details: result.flagged.slice(0, 20),
    });
  } catch (error: any) {
    console.error("[risk-report] ERROR", error);

    return NextResponse.json(
      {
        success: false,
        error: error?.message ?? String(error),
        report_date: reportDateIso,
        debug: { execution_time_ms: Date.now() - start },
      },
      { status: 500 }
    );
  }
}