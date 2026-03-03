import { BigQuery } from "@google-cloud/bigquery";

function getBigQueryClient() {
  const projectId = process.env.GOOGLE_CLOUD_PROJECT || "bigqueryexport-183608";

  // Vercel 环境：用环境变量里的凭证
  if (process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON) {
    const credentials = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON);
    return new BigQuery({ projectId, credentials });
  }

  // 本地环境：自动使用 gcloud auth 的凭证
  return new BigQuery({ projectId });
}

const bigquery = getBigQueryClient();

export async function getDailyChangeData() {
  const query = `
    WITH date_range AS (
      SELECT
        -- ✅ latest_date is the latest date that is NOT in the future
        MAX(IF(xact_post_date <= CURRENT_DATE(), xact_post_date, NULL)) AS latest_date,
        DATE_SUB(MAX(IF(xact_post_date <= CURRENT_DATE(), xact_post_date, NULL)), INTERVAL 7 DAY) AS week_ago_date
      FROM \`bigqueryexport-183608.PayabilitySheets.vm_transaction_summary\`
    ),
    this_week AS (
      SELECT
        supplier_key,
        ANY_VALUE(supplier_name) AS supplier_name,
        SUM(IFNULL(receivable, 0)) AS current_receivable,
        SUM(IFNULL(potential_liability, 0)) AS current_liability,
        SUM(IFNULL(net_earning, 0)) AS current_net_earning,
        SUM(IFNULL(chargeback, 0)) AS current_chargeback,
        SUM(IFNULL(available_balance, 0)) AS current_available_balance,
        SUM(IFNULL(outstanding_bal, 0)) AS current_outstanding_bal
      FROM \`bigqueryexport-183608.PayabilitySheets.vm_transaction_summary\`, date_range
      WHERE xact_post_date > date_range.week_ago_date
        AND xact_post_date <= date_range.latest_date
      GROUP BY supplier_key
    ),
    last_week AS (
      SELECT
        supplier_key,
        SUM(IFNULL(receivable, 0)) AS prev_receivable,
        SUM(IFNULL(potential_liability, 0)) AS prev_liability
      FROM \`bigqueryexport-183608.PayabilitySheets.vm_transaction_summary\`, date_range
      WHERE xact_post_date > DATE_SUB(date_range.week_ago_date, INTERVAL 7 DAY)
        AND xact_post_date <= date_range.week_ago_date
      GROUP BY supplier_key
    )
    SELECT
      t.supplier_key,
      t.supplier_name,
      t.current_receivable AS today_receivable,
      COALESCE(l.prev_receivable, 0) AS prev_receivable,
      t.current_liability AS today_liability,
      COALESCE(l.prev_liability, 0) AS prev_liability,
      t.current_net_earning AS today_net_earning,
      t.current_chargeback AS today_chargeback,
      t.current_available_balance AS today_available_balance,
      t.current_outstanding_bal AS today_outstanding_bal,
      CASE
        WHEN COALESCE(l.prev_receivable, 0) > 0
        THEN ROUND(SAFE_DIVIDE(t.current_receivable - l.prev_receivable, l.prev_receivable) * 100, 2)
        ELSE NULL
      END AS receivable_change_pct,
      CASE
        WHEN COALESCE(l.prev_liability, 0) > 0
        THEN ROUND(SAFE_DIVIDE(t.current_liability - l.prev_liability, l.prev_liability) * 100, 2)
        ELSE NULL
      END AS liability_change_pct,
      IF(l.supplier_key IS NULL, FALSE, TRUE) AS has_prev_week_data
    FROM this_week t
    LEFT JOIN last_week l ON t.supplier_key = l.supplier_key
    ORDER BY t.current_receivable DESC
    LIMIT 2000
  `;

  const [rows] = await bigquery.query({ query });
  return rows;
}