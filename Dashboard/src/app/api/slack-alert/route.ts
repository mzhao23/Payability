export const runtime = "nodejs";
export const dynamic = "force-dynamic";

import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase-admin";

const SOURCE_LABELS: Record<string, string> = {
  daily_summary_report: "Daily Summary Agent",
  shipment_agent: "Shipment Agent",
  json_agent: "JSON Agent",
  decision_agent: "Decision Agent",
};

export async function GET() {
  try {
    const sb = supabaseAdmin();
    const today = new Date().toISOString().slice(0, 10);

    const { data: config } = await sb
      .from("app_settings")
      .select("value")
      .eq("key", "slack_config")
      .single();

    const slackConfig = config?.value as { webhook_url: string; channel: string; enabled: boolean } | null;

    if (!slackConfig?.enabled || !slackConfig?.webhook_url) {
      return NextResponse.json({ success: false, reason: "Slack not configured or disabled" });
    }

    const { data: flagged } = await sb
      .from("consolidated_flagged_supplier_list")
      .select("supplier_key, supplier_name, overall_risk_score, source, reasons")
      .gte("created_at", `${today}T00:00:00Z`)
      .lte("created_at", `${today}T23:59:59Z`)
      .order("overall_risk_score", { ascending: false })
      .limit(30);

    if (!flagged || flagged.length === 0) {
      return NextResponse.json({ success: true, message: "No flagged suppliers today" });
    }

    const dashboardUrl = process.env.NEXT_PUBLIC_APP_URL || "https://payability-dashboard.vercel.app";

    const blocks = [
      {
        type: "header",
        text: { type: "plain_text", text: `🚨 Risk Alert — ${flagged.length} Suppliers Flagged (${today})` },
      },
      { type: "divider" },
      ...flagged.slice(0, 15).map((s: any) => ({
        type: "section",
        text: {
          type: "mrkdwn",
          text: `*${s.supplier_name}*\nKey: \`${s.supplier_key.slice(0, 8)}...\` | Score: *${s.overall_risk_score}/10* | Agent: ${SOURCE_LABELS[s.source] ?? s.source}\n> ${Array.isArray(s.reasons) ? s.reasons[0]?.slice(0, 150) : "No reason"}`,
        },
      })),
      { type: "divider" },
      {
        type: "actions",
        elements: [{
          type: "button",
          text: { type: "plain_text", text: "Open Dashboard" },
          url: `${dashboardUrl}/dashboard`,
        }],
      },
    ];

    const slackRes = await fetch(slackConfig.webhook_url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ blocks }),
    });

    return NextResponse.json({ success: slackRes.ok, flagged_count: flagged.length });
  } catch (error: any) {
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}