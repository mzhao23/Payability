-- Metric 2: Daily Price Escalation Detection
-- Parameters: baseline_days, zscore_threshold, ship_sla_days, table

DECLARE baseline_days INT64 DEFAULT {baseline_days};
DECLARE zscore_threshold FLOAT64 DEFAULT {zscore_threshold};
DECLARE ship_sla_days INT64 DEFAULT {ship_sla_days};

WITH deduplicated_labels AS (
    SELECT * EXCEPT(row_num)
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY sk ORDER BY _sdc_sequence DESC) AS row_num
        FROM `{table}`
    )
    WHERE row_num = 1 AND _sdc_deleted_at IS NULL
),
order_level_numeric AS (
    SELECT
        order_id,
        supplier_key,
        order_date,
        MAX(SAFE_CAST(SPLIT(total_cost, ' ')[OFFSET(0)] AS FLOAT64)) AS order_value
    FROM deduplicated_labels
    WHERE total_cost IS NOT NULL AND total_cost != ''
    GROUP BY order_id, supplier_key, order_date
),
daily_supplier_stats AS (
    SELECT
        CAST(order_date AS DATE) AS order_date,
        supplier_key,
        COUNT(*) AS total_orders,
        SUM(order_value) AS total_revenue,
        AVG(order_value) AS avg_order_value,
        MAX(order_value) AS max_order_value,
        MIN(order_value) AS min_order_value
    FROM order_level_numeric
    WHERE order_date IS NOT NULL AND order_date != ''
    GROUP BY order_date, supplier_key
),
rolling_baseline AS (
    SELECT *,
        AVG(avg_order_value) OVER (
            PARTITION BY supplier_key
            ORDER BY order_date
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS avg_of_avg,
        STDDEV(avg_order_value) OVER (
            PARTITION BY supplier_key
            ORDER BY order_date
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS stddev_of_avg,
        AVG(max_order_value) OVER (
            PARTITION BY supplier_key
            ORDER BY order_date
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS avg_of_max,
        STDDEV(max_order_value) OVER (
            PARTITION BY supplier_key
            ORDER BY order_date
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS stddev_of_max
    FROM daily_supplier_stats
)
SELECT
    order_date,
    supplier_key,
    total_orders,
    total_revenue,
    avg_order_value,
    max_order_value,
    min_order_value,
    avg_of_avg,
    SAFE_DIVIDE((avg_order_value - avg_of_avg), stddev_of_avg) AS zscore,
    SAFE_DIVIDE((max_order_value - avg_of_max), stddev_of_max) AS max_zscore,
    CASE
        WHEN SAFE_DIVIDE((avg_order_value - avg_of_avg), stddev_of_avg) > zscore_threshold
        THEN 'YES' ELSE 'NO'
    END AS high_price_risk
FROM rolling_baseline
WHERE order_date = DATE_SUB(CURRENT_DATE(), INTERVAL ship_sla_days DAY)
ORDER BY zscore DESC