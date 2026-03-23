export const runtime = "nodejs";
export const dynamic = "force-dynamic";

import { NextResponse } from "next/server";

export async function GET() {
  try {
    const baseUrl = process.env.NEXT_PUBLIC_APP_URL || "https://payability-dashboard.vercel.app";

    const slackRes = await fetch(`${baseUrl}/api/slack-alert`);
    const slackData = await slackRes.json();

    return NextResponse.json({
      success: true,
      triggered_at: new Date().toISOString(),
      slack: slackData,
    });
  } catch (error: any) {
    return NextResponse.json({ success: false, error: error.message }, { status: 500 });
  }
}