export const runtime = "nodejs";
export const dynamic = "force-dynamic";

import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase-admin";

export async function GET() {
  try {
    const { data, error } = await supabaseAdmin
      .from("agent_runs")
      .insert({
        report_date: new Date().toISOString(),
        total_suppliers: 1,
        flagged_count: 0,
        ai_report: "test write",
        debug: { hello: "world" },
      })
      .select("id, created_at")
      .single();

    if (error) throw error;

    return NextResponse.json({ ok: true, inserted: data });
  } catch (e: any) {
    return NextResponse.json(
      { ok: false, error: e?.message ?? String(e) },
      { status: 500 }
    );
  }
}