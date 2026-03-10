-- Metric 1: Daily Untracked Order Rate
-- Parameters: ship_sla_days, window_days, table
-- Result A: by supplier + carrier
-- Result B: by carrier only (systemic trend detection)
-- Only includes carriers with reliable init_timestamp coverage:
--   UPS (~93%), FedEx (~94%), USPS (~48% but included for comparison)

DECLARE ship_sla_days INT64 DEFAULT {ship_sla_days};
DECLARE window_days INT64 DEFAULT {window_days};

WITH deduplicated_labels AS (
    SELECT * EXCEPT(row_num),
        CASE
            WHEN carrier = 'FedEx' THEN 'FEDEX'               -- 正常 FedEx，有数据
            WHEN carrier = 'FEDEX' THEN 'FEDEX_UNACTIVATED'   -- 全大写，init/pickup 永远为空
            ELSE UPPER(carrier)
        END AS carrier_normalized
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY sk ORDER BY _sdc_sequence DESC) AS row_num
        FROM `{table}`
    )
    WHERE row_num = 1
      AND _sdc_deleted_at IS NULL
),
filtered_labels AS (
    -- UPS: 93% init coverage
    -- FEDEX: 94% init coverage (carrier = 'FedEx' exact match only)
    -- USPS: 48% init coverage (included for comparison)
    -- FEDEX_UNACTIVATED: 0% coverage, included to detect ghost orders
    SELECT *
    FROM deduplicated_labels
    WHERE carrier_normalized IN ('UPS', 'FEDEX', 'USPS', 'FEDEX_UNACTIVATED')
),
order_level_aggregation AS (
    SELECT
        CAST(order_date AS DATE) AS order_date,
        supplier_key,
        carrier_normalized,
        order_id,
        CASE WHEN COUNTIF(init_timestamp IS NOT NULL AND init_timestamp != '') = 0
             THEN 1 ELSE 0 END AS is_untracked
    FROM filtered_labels
    WHERE order_date IS NOT NULL AND order_date != ''
      AND CAST(order_date AS DATE) <= DATE_SUB(CURRENT_DATE(), INTERVAL ship_sla_days DAY)
    GROUP BY 1, 2, 3, 4
),
daily_supplier_carrier_stats AS (
    SELECT
        order_date, supplier_key, carrier_normalized,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(is_untracked) AS untracked_orders,
        SAFE_DIVIDE(SUM(is_untracked), COUNT(DISTINCT order_id)) AS untracked_rate
    FROM order_level_aggregation
    GROUP BY 1, 2, 3
),
rolling_baseline_supplier AS (
    SELECT *,
        AVG(untracked_rate) OVER (
            PARTITION BY supplier_key, carrier_normalized
            ORDER BY order_date
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS rolling_avg_30d
    FROM daily_supplier_carrier_stats
)
-- Result A: by supplier + carrier
SELECT
    order_date,
    supplier_key,
    carrier_normalized,
    total_orders,
    untracked_orders,
    untracked_rate,
    rolling_avg_30d,
    untracked_rate - COALESCE(rolling_avg_30d, 0) AS diff,
    'by_supplier' AS result_type
FROM rolling_baseline_supplier
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL (window_days + ship_sla_days) DAY)

UNION ALL

-- Result B: by carrier only (systemic trend detection)
-- Use this to check if a supplier's spike is due to broader carrier issues
SELECT
    order_date,
    NULL AS supplier_key,
    carrier_normalized,
    SUM(total_orders) AS total_orders,
    SUM(untracked_orders) AS untracked_orders,
    SAFE_DIVIDE(SUM(untracked_orders), SUM(total_orders)) AS untracked_rate,
    AVG(rolling_avg_30d) AS rolling_avg_30d,
    SAFE_DIVIDE(SUM(untracked_orders), SUM(total_orders)) - AVG(COALESCE(rolling_avg_30d, 0)) AS diff,
    'by_carrier' AS result_type
FROM rolling_baseline_supplier
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL (window_days + ship_sla_days) DAY)
GROUP BY 1, 3

ORDER BY result_type, order_date DESC, diff DESC