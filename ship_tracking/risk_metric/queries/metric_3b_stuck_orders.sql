-- Metric 3B: FedEx Stuck Orders (2026 only)
-- Since FedEx has 100% pickup coverage, any stuck order = HIGH risk

DECLARE stuck_days INT64 DEFAULT {stuck_days};

WITH deduplicated_labels AS (
  SELECT
    supplier_key,
    order_id,
    init_timestamp,
    pickup_timestamp
  FROM (
    SELECT
      supplier_key,
      order_id,
      init_timestamp,
      pickup_timestamp,
      _sdc_deleted_at,
      ROW_NUMBER() OVER (PARTITION BY sk ORDER BY _sdc_sequence DESC) AS row_num
    FROM `{table}`
    WHERE
      carrier = 'FedEx'
      AND CAST(order_date AS DATE) >= '2026-01-01'
  )
  WHERE
    row_num = 1
    AND _sdc_deleted_at IS NULL
),
supplier_totals AS (
  SELECT
    supplier_key,
    COUNT(DISTINCT order_id) AS total_fedex_orders
  FROM deduplicated_labels
  GROUP BY supplier_key
),
stuck_orders AS (
  SELECT
    supplier_key,
    COUNT(*) AS stuck_order_count
  FROM deduplicated_labels
  WHERE
    (init_timestamp IS NOT NULL AND init_timestamp != '')
    AND (pickup_timestamp IS NULL OR pickup_timestamp = '')
    AND DATE_DIFF(
      CURRENT_DATE(),
      DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', init_timestamp)),
      DAY
    ) > stuck_days
  GROUP BY supplier_key
)
SELECT
  s.supplier_key,
  s.stuck_order_count,
  t.total_fedex_orders,
  ROUND(SAFE_DIVIDE(s.stuck_order_count, t.total_fedex_orders) * 100, 2) AS stuck_rate,
  CASE WHEN s.stuck_order_count > 0 THEN 'HIGH' ELSE 'NORMAL' END AS risk_level
FROM stuck_orders AS s
JOIN supplier_totals AS t ON s.supplier_key = t.supplier_key
ORDER BY stuck_order_count DESC