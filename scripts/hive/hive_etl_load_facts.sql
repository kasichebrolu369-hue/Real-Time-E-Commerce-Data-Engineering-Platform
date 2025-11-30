-- ============================================================================
-- HIVE ETL - LOAD FACT TABLES
-- ============================================================================
-- 
-- PURPOSE:
-- Load fact tables from clean zone data
-- 
-- TABLES:
-- - fact_clickstream
-- - fact_transactions
--
-- ============================================================================

USE ecommerce_dwh;

-- ============================================================================
-- ETL 1: LOAD fact_clickstream
-- ============================================================================

/*
 * Load clickstream events from clean zone with partitioning
 * Incremental load based on date partitions
 */

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 2000;
SET hive.exec.max.dynamic.partitions.pernode = 500;

-- Enable bucketing
SET hive.enforce.bucketing = true;

-- Insert clickstream data with transformations
INSERT INTO TABLE fact_clickstream PARTITION(year, month, day)
SELECT 
    c.event_id,
    c.user_id,
    c.product_id,
    c.session_id,
    CAST(c.event_timestamp AS TIMESTAMP) AS event_timestamp,
    c.page_url,
    c.event_type,
    c.device_type,
    c.ip_address,
    c.traffic_source,
    c.referrer,
    c.user_agent,
    c.country,
    c.city,
    c.browser,
    c.os,
    c.hour,
    
    -- Calculate day of week (1=Monday, 7=Sunday)
    CASE DAYOFWEEK(CAST(c.event_timestamp AS TIMESTAMP))
        WHEN 1 THEN 7  -- Sunday
        ELSE DAYOFWEEK(CAST(c.event_timestamp AS TIMESTAMP)) - 1
    END AS day_of_week,
    
    -- Weekend flag
    CASE 
        WHEN DAYOFWEEK(CAST(c.event_timestamp AS TIMESTAMP)) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    
    -- Session duration placeholder (calculate in subsequent processing)
    NULL AS session_duration,
    
    -- Partition columns
    c.year,
    c.month,
    c.day
FROM EXTERNAL TABLE (
    event_id STRING,
    user_id STRING,
    session_id STRING,
    event_timestamp STRING,
    page_url STRING,
    product_id STRING,
    event_type STRING,
    device_type STRING,
    ip_address STRING,
    traffic_source STRING,
    referrer STRING,
    user_agent STRING,
    country STRING,
    city STRING,
    browser STRING,
    os STRING,
    event_date STRING,
    year INT,
    month INT,
    day INT,
    hour INT
) c
STORED AS PARQUET
LOCATION '/ecommerce/clean/clickstream/data/'
WHERE c.event_timestamp IS NOT NULL
  AND c.user_id IS NOT NULL
  AND c.event_type IN ('view', 'add_to_cart', 'remove_from_cart', 'purchase', 'exit');

-- Verify clickstream load
SELECT 
    year,
    month,
    day,
    event_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT session_id) AS unique_sessions
FROM fact_clickstream
WHERE year = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
GROUP BY year, month, day, event_type
ORDER BY year DESC, month DESC, day DESC, event_type;

-- Show partitions
SHOW PARTITIONS fact_clickstream;

-- ============================================================================
-- ETL 2: LOAD fact_transactions
-- ============================================================================

/*
 * Load transaction data from clean zone with enrichment
 * Incremental load based on date partitions
 */

-- Insert transaction data with transformations and enrichment
INSERT INTO TABLE fact_transactions PARTITION(year, month, day)
SELECT 
    t.transaction_id,
    t.user_id,
    t.product_id,
    CAST(t.transaction_ts AS TIMESTAMP) AS transaction_timestamp,
    t.quantity,
    t.unit_price,
    t.subtotal,
    t.discount_percent,
    t.discount_amount,
    t.tax_amount,
    t.shipping_fee,
    t.total_amount,
    t.payment_method,
    t.status,
    t.delivery_pincode,
    t.delivery_city,
    t.delivery_state,
    t.delivery_country,
    CAST(t.expected_delivery_ts AS DATE) AS expected_delivery_date,
    CAST(t.actual_delivery_ts AS DATE) AS actual_delivery_date,
    t.delivery_time_days,
    CAST(t.is_late_delivery AS BOOLEAN) AS is_late_delivery,
    t.customer_email,
    t.customer_phone,
    t.order_source,
    CAST(t.is_first_purchase AS BOOLEAN) AS is_first_purchase,
    t.coupon_code,
    t.estimated_profit,
    t.revenue_bucket,
    CAST(t.is_high_value AS BOOLEAN) AS is_high_value,
    CAST(t.is_bulk_order AS BOOLEAN) AS is_bulk_order,
    CAST(t.is_high_discount AS BOOLEAN) AS is_high_discount,
    t.hour,
    
    -- Calculate day of week
    CASE DAYOFWEEK(CAST(t.transaction_ts AS TIMESTAMP))
        WHEN 1 THEN 7  -- Sunday
        ELSE DAYOFWEEK(CAST(t.transaction_ts AS TIMESTAMP)) - 1
    END AS day_of_week,
    
    -- Weekend flag
    CASE 
        WHEN DAYOFWEEK(CAST(t.transaction_ts AS TIMESTAMP)) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    
    -- Partition columns
    t.year,
    t.month,
    t.day
FROM EXTERNAL TABLE (
    transaction_id STRING,
    user_id STRING,
    product_id STRING,
    category STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    subtotal DECIMAL(12,2),
    discount_percent INT,
    discount_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    shipping_fee DECIMAL(8,2),
    total_amount DECIMAL(12,2),
    payment_method STRING,
    status STRING,
    transaction_ts STRING,
    delivery_pincode STRING,
    delivery_city STRING,
    delivery_state STRING,
    delivery_country STRING,
    expected_delivery_ts STRING,
    actual_delivery_ts STRING,
    delivery_time_days INT,
    is_late_delivery INT,
    customer_email STRING,
    customer_phone STRING,
    order_source STRING,
    is_first_purchase INT,
    coupon_code STRING,
    estimated_profit DECIMAL(10,2),
    revenue_bucket STRING,
    is_high_value INT,
    is_bulk_order INT,
    is_high_discount INT,
    transaction_date STRING,
    year INT,
    month INT,
    day INT,
    hour INT
) t
STORED AS PARQUET
LOCATION '/ecommerce/clean/transactions/data/'
WHERE t.transaction_ts IS NOT NULL
  AND t.user_id IS NOT NULL
  AND t.product_id IS NOT NULL
  AND t.total_amount > 0;

-- Verify transaction load
SELECT 
    year,
    month,
    day,
    status,
    COUNT(*) AS transaction_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    COUNT(DISTINCT user_id) AS unique_customers
FROM fact_transactions
WHERE year = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
GROUP BY year, month, day, status
ORDER BY year DESC, month DESC, day DESC, status;

-- Show partitions
SHOW PARTITIONS fact_transactions;

-- ============================================================================
-- ETL 3: REFRESH AGGREGATE TABLES
-- ============================================================================

/*
 * Populate aggregate tables for fast query performance
 */

-- Populate daily sales summary
INSERT OVERWRITE TABLE agg_daily_sales PARTITION(year, month)
SELECT 
    DATE(transaction_timestamp) AS sale_date,
    COUNT(*) AS total_transactions,
    SUM(total_amount) AS total_revenue,
    SUM(quantity) AS total_quantity,
    AVG(total_amount) AS avg_order_value,
    COUNT(DISTINCT user_id) AS unique_customers,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_orders,
    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
    SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS refunded_orders,
    SUM(CASE WHEN is_first_purchase = TRUE THEN 1 ELSE 0 END) AS new_customers,
    SUM(CASE WHEN is_first_purchase = FALSE THEN 1 ELSE 0 END) AS returning_customers,
    SUM(CASE WHEN order_source IN ('mobile_app', 'mobile_web') THEN 1 ELSE 0 END) AS mobile_orders,
    SUM(CASE WHEN order_source = 'web' THEN 1 ELSE 0 END) AS desktop_orders,
    
    -- Get top category (using window function)
    FIRST_VALUE(p.category) OVER (
        PARTITION BY DATE(t.transaction_timestamp) 
        ORDER BY COUNT(*) DESC
    ) AS top_category,
    
    -- Get top product
    FIRST_VALUE(t.product_id) OVER (
        PARTITION BY DATE(t.transaction_timestamp) 
        ORDER BY COUNT(*) DESC
    ) AS top_product,
    
    -- Partition columns
    YEAR(transaction_timestamp) AS year,
    MONTH(transaction_timestamp) AS month
FROM fact_transactions t
JOIN dim_products p ON t.product_id = p.product_id AND p.is_current = TRUE
WHERE t.status = 'completed'
GROUP BY DATE(transaction_timestamp), YEAR(transaction_timestamp), MONTH(transaction_timestamp);

-- Verify aggregate load
SELECT 
    year,
    month,
    COUNT(*) AS days,
    SUM(total_revenue) AS monthly_revenue,
    AVG(avg_order_value) AS avg_order_value
FROM agg_daily_sales
GROUP BY year, month
ORDER BY year DESC, month DESC
LIMIT 12;

-- ============================================================================
-- ETL 4: REFRESH USER METRICS
-- ============================================================================

/*
 * Update user behavior metrics aggregate table
 */

INSERT OVERWRITE TABLE agg_user_metrics
SELECT 
    u.user_id,
    
    -- Clickstream metrics
    COUNT(DISTINCT c.session_id) AS total_sessions,
    COUNT(c.event_id) AS total_events,
    SUM(CASE WHEN c.event_type = 'view' THEN 1 ELSE 0 END) AS total_page_views,
    SUM(CASE WHEN c.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS total_cart_adds,
    SUM(CASE WHEN c.event_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
    
    -- Conversion rate
    CASE 
        WHEN SUM(CASE WHEN c.event_type = 'view' THEN 1 ELSE 0 END) > 0 
        THEN (SUM(CASE WHEN c.event_type = 'purchase' THEN 1 ELSE 0 END) * 100.0 / 
              SUM(CASE WHEN c.event_type = 'view' THEN 1 ELSE 0 END))
        ELSE 0 
    END AS conversion_rate,
    
    -- Session duration (placeholder - needs more complex calculation)
    NULL AS avg_session_duration,
    
    -- Transaction metrics
    COALESCE(SUM(t.total_amount), 0) AS total_revenue,
    COALESCE(AVG(t.total_amount), 0) AS avg_order_value,
    
    -- Activity metrics
    MAX(DATE(c.event_timestamp)) AS last_activity_date,
    DATEDIFF(CURRENT_DATE, MAX(DATE(c.event_timestamp))) AS days_since_last_activity,
    CASE 
        WHEN DATEDIFF(CURRENT_DATE, MAX(DATE(c.event_timestamp))) > 90 THEN TRUE 
        ELSE FALSE 
    END AS is_churned,
    
    -- Preferences (most common values)
    FIRST_VALUE(c.device_type) OVER (
        PARTITION BY u.user_id 
        ORDER BY COUNT(*) DESC
    ) AS preferred_device,
    
    FIRST_VALUE(p.category) OVER (
        PARTITION BY u.user_id 
        ORDER BY COUNT(*) DESC
    ) AS preferred_category,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM dim_users u
LEFT JOIN fact_clickstream c ON u.user_id = c.user_id
LEFT JOIN fact_transactions t ON u.user_id = t.user_id AND t.status = 'completed'
LEFT JOIN dim_products p ON t.product_id = p.product_id AND p.is_current = TRUE
WHERE u.is_current = TRUE
GROUP BY u.user_id;

-- Verify user metrics
SELECT 
    is_churned,
    COUNT(*) AS user_count,
    AVG(total_revenue) AS avg_revenue,
    AVG(conversion_rate) AS avg_conversion_rate
FROM agg_user_metrics
GROUP BY is_churned;

-- ============================================================================
-- ETL 5: REFRESH MATERIALIZED VIEWS
-- ============================================================================

-- Rebuild materialized view
ALTER MATERIALIZED VIEW mv_daily_revenue_by_category REBUILD;

-- Verify materialized view
SELECT * FROM mv_daily_revenue_by_category
ORDER BY sale_date DESC, total_revenue DESC
LIMIT 20;

/*
 * ============================================================================
 * DATA QUALITY CHECKS
 * ============================================================================
 */

-- Check for orphan records in fact_transactions
SELECT COUNT(*) AS orphan_transaction_count
FROM fact_transactions t
LEFT JOIN dim_users u ON t.user_id = u.user_id AND u.is_current = TRUE
LEFT JOIN dim_products p ON t.product_id = p.product_id AND p.is_current = TRUE
WHERE u.user_id IS NULL OR p.product_id IS NULL;

-- Check for NULL values in critical fields
SELECT 
    'fact_clickstream' AS table_name,
    SUM(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END) AS null_event_id,
    SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_id,
    SUM(CASE WHEN event_timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamp
FROM fact_clickstream
UNION ALL
SELECT 
    'fact_transactions' AS table_name,
    SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) AS null_transaction_id,
    SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_id,
    SUM(CASE WHEN transaction_timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamp
FROM fact_transactions;

/*
 * ============================================================================
 * EXECUTION NOTES:
 * ============================================================================
 * 
 * 1. Run this script after dimension load:
 *    hive -f hive_etl_load_facts.sql
 * 
 * 2. For incremental loads, add date filters:
 *    WHERE year = ${hiveconf:target_year}
 *      AND month = ${hiveconf:target_month}
 *      AND day = ${hiveconf:target_day}
 * 
 * 3. Schedule via Airflow for daily execution
 * 
 * 4. Monitor:
 *    - Partition creation
 *    - Record counts
 *    - Data quality checks
 *    - Aggregate table refresh
 * 
 * ============================================================================
 */
