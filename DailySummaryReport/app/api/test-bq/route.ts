export const runtime = "nodejs";
export const dynamic = "force-dynamic";

import { NextResponse } from "next/server";
import { BigQuery } from "@google-cloud/bigquery";

const bigquery = new BigQuery({
  projectId: process.env.GOOGLE_CLOUD_PROJECT || "bigqueryexport-183608",
});

export async function GET() {
  const query = `
    SELECT
      MAX(xact_post_date) AS latest_date,
      MIN(xact_post_date) AS earliest_date,
      COUNT(*) AS row_count_all,
      COUNT(DISTINCT supplier_key) AS supplier_count_all,

      COUNTIF(xact_post_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) AS row_count_last_30d,
      COUNT(DISTINCT IF(xact_post_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY), supplier_key, NULL)) AS supplier_count_last_30d
    FROM \`bigqueryexport-183608.PayabilitySheets.vm_transaction_summary\`
  `;

  const [rows] = await bigquery.query({ query });
  return NextResponse.json({ ok: true, stats: rows[0] });
}