-- Metric 3A: FedEx Init to Pickup Lag Trend (2026 only)

WITH deduplicated_labels AS (
  SELECT
    order_date,
    supplier_key,
    DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', init_timestamp)) AS init_date,
    DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', pickup_timestamp)) AS pickup_date
  FROM (
    SELECT
      order_date,
      supplier_key,
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
    AND init_timestamp IS NOT NULL AND init_timestamp != ''
    AND pickup_timestamp IS NOT NULL AND pickup_timestamp != ''
),
supplier_stats AS (
  SELECT
    order_date,
    supplier_key,
    AVG(DATE_DIFF(pickup_date, init_date, DAY)) AS avg_pickup_lag,
    MAX(DATE_DIFF(pickup_date, init_date, DAY)) AS max_pickup_lag,
    COUNT(*) AS total_packages,
    'by_supplier' AS result_type
  FROM deduplicated_labels
  GROUP BY order_date, supplier_key
),
carrier_stats AS (
  SELECT
    order_date,
    'ALL_SUPPLIERS' AS supplier_key,
    AVG(DATE_DIFF(pickup_date, init_date, DAY)) AS avg_pickup_lag,
    MAX(DATE_DIFF(pickup_date, init_date, DAY)) AS max_pickup_lag,
    COUNT(*) AS total_packages,
    'by_carrier' AS result_type
  FROM deduplicated_labels
  GROUP BY order_date
),
combined_stats AS (
  SELECT * FROM supplier_stats
  UNION ALL
  SELECT * FROM carrier_stats
)
SELECT
  *,
  AVG(avg_pickup_lag) OVER (
    PARTITION BY supplier_key
    ORDER BY CAST(order_date AS DATE)
    ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
  ) AS rolling_avg_30d,
  avg_pickup_lag - AVG(avg_pickup_lag) OVER (
    PARTITION BY supplier_key
    ORDER BY CAST(order_date AS DATE)
    ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
  ) AS diff
FROM combined_stats
WHERE CAST(order_date AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
ORDER BY order_date DESC, diff DESC