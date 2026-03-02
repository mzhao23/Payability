// app/api/test-supabase/route.ts
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase-admin";

export async function GET() {
  const ts = new Date().toISOString();

  try {
    const sb = supabaseAdmin();

    const { data, error } = await sb
      .from("agent_runs")
      .insert({
        report_date: ts,
        total_suppliers: 1,
        flagged_count: 0,
        ai_report: "test write",
        debug: { hello: "world" },
      })
      .select("id, created_at")
      .single();

    if (error) {
      console.error("[test-supabase] insert error", error);
      throw new Error(`Supabase insert failed: ${error.message}`);
    }

    if (!data?.id) {
      throw new Error("Supabase insert succeeded but returned no id");
    }

    return NextResponse.json({ ok: true, ts, inserted: data });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : String(e);
    console.error("[test-supabase] ERROR", e);

    return NextResponse.json({ ok: false, ts, error: message }, { status: 500 });
  }
}