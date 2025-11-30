-- ============================================================================
-- IMPALA BI DASHBOARD QUERIES
-- ============================================================================
-- 
-- PURPOSE:
-- Optimized Impala queries for real-time business intelligence dashboards
-- 
-- DASHBOARDS:
-- 1. Real-Time Revenue Dashboard
-- 2. Product Performance Leaderboard
-- 3. Customer Segmentation Analysis
-- 4. Conversion Funnel Analytics
-- 5. Abandoned Cart Analytics
-- 
-- All queries optimized with partition pruning and proper joins
-- ============================================================================

USE ecommerce_dwh;

-- ============================================================================
-- DASHBOARD 1: REAL-TIME REVENUE DASHBOARD
-- ============================================================================

/*
 * Real-time revenue metrics for executive dashboard
 * Updates every hour
 */

-- Today's Revenue Summary
SELECT 
    CURRENT_DATE AS report_date,
    COUNT(DISTINCT transaction_id) AS total_orders,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    COUNT(DISTINCT user_id) AS unique_customers,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_orders,
    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_orders,
    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
    ROUND(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS completion_rate
FROM fact_transactions
WHERE year = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
  AND day = DAY(CURRENT_DATE);

-- Hourly Revenue Trend (Last 24 hours)
SELECT 
    FROM_TIMESTAMP(transaction_timestamp, 'yyyy-MM-dd HH:00:00') AS hour_start,
    COUNT(*) AS order_count,
    SUM(total_amount) AS hourly_revenue,
    AVG(total_amount) AS avg_order_value,
    COUNT(DISTINCT user_id) AS unique_customers
FROM fact_transactions
WHERE transaction_timestamp >= HOURS_SUB(NOW(), 24)
  AND status = 'completed'
GROUP BY FROM_TIMESTAMP(transaction_timestamp, 'yyyy-MM-dd HH:00:00')
ORDER BY hour_start DESC;

-- Revenue by Payment Method (Today)
SELECT 
    payment_method,
    COUNT(*) AS transaction_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_transaction_amount,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_total
FROM fact_transactions
WHERE year = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
  AND day = DAY(CURRENT_DATE)
  AND status = 'completed'
GROUP BY payment_method
ORDER BY total_revenue DESC;

-- Revenue by Device Type (Today)
SELECT 
    CASE 
        WHEN order_source IN ('mobile_app', 'mobile_web') THEN 'Mobile'
        WHEN order_source = 'web' THEN 'Desktop'
        ELSE 'Other'
    END AS device_category,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_revenue,
    ROUND(SUM(total_amount) * 100.0 / SUM(SUM(total_amount)) OVER (), 2) AS revenue_percentage
FROM fact_transactions
WHERE year = YEAR(CURRENT_DATE)
  AND month = MONTH(CURRENT_DATE)
  AND day = DAY(CURRENT_DATE)
  AND status = 'completed'
GROUP BY 
    CASE 
        WHEN order_source IN ('mobile_app', 'mobile_web') THEN 'Mobile'
        WHEN order_source = 'web' THEN 'Desktop'
        ELSE 'Other'
    END
ORDER BY total_revenue DESC;

-- Week-over-Week Revenue Comparison
WITH current_week AS (
    SELECT SUM(total_amount) AS revenue
    FROM fact_transactions
    WHERE transaction_timestamp >= DATE_SUB(CURRENT_DATE, 7)
      AND transaction_timestamp < CURRENT_DATE
      AND status = 'completed'
),
previous_week AS (
    SELECT SUM(total_amount) AS revenue
    FROM fact_transactions
    WHERE transaction_timestamp >= DATE_SUB(CURRENT_DATE, 14)
      AND transaction_timestamp < DATE_SUB(CURRENT_DATE, 7)
      AND status = 'completed'
)
SELECT 
    c.revenue AS current_week_revenue,
    p.revenue AS previous_week_revenue,
    c.revenue - p.revenue AS revenue_difference,
    ROUND((c.revenue - p.revenue) * 100.0 / p.revenue, 2) AS growth_percentage
FROM current_week c, previous_week p;

-- ============================================================================
-- DASHBOARD 2: PRODUCT PERFORMANCE LEADERBOARD
-- ============================================================================

/*
 * Top performing products and categories
 * Real-time product analytics
 */

-- Top 20 Products by Revenue (Last 7 Days)
SELECT /*+ BROADCAST(p) */
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    COUNT(*) AS units_sold,
    SUM(t.total_amount) AS total_revenue,
    AVG(t.total_amount) AS avg_sale_price,
    p.discounted_price AS current_price,
    p.avg_rating,
    p.num_ratings
FROM fact_transactions t
JOIN dim_products p ON t.product_id = p.product_id AND p.is_current = TRUE
WHERE t.transaction_timestamp >= DATE_SUB(CURRENT_DATE, 7)
  AND t.status = 'completed'
GROUP BY 
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.discounted_price,
    p.avg_rating,
    p.num_ratings
ORDER BY total_revenue DESC
LIMIT 20;

-- Category Performance (Last 30 Days)
SELECT /*+ BROADCAST(p) */
    p.category,
    COUNT(DISTINCT t.transaction_id) AS total_orders,
    SUM(t.quantity) AS units_sold,
    SUM(t.total_amount) AS total_revenue,
    AVG(t.total_amount) AS avg_order_value,
    COUNT(DISTINCT t.user_id) AS unique_customers,
    ROUND(SUM(t.total_amount) * 100.0 / SUM(SUM(t.total_amount)) OVER (), 2) AS revenue_share_pct
FROM fact_transactions t
JOIN dim_products p ON t.product_id = p.product_id AND p.is_current = TRUE
WHERE t.transaction_timestamp >= DATE_SUB(CURRENT_DATE, 30)
  AND t.status = 'completed'
GROUP BY p.category
ORDER BY total_revenue DESC;

-- Product Stock Alert (Low Stock Bestsellers)
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.stock_quantity,
    p.discounted_price,
    p.avg_rating,
    COUNT(t.transaction_id) AS sales_last_7_days,
    ROUND(p.stock_quantity / (COUNT(t.transaction_id) / 7.0), 1) AS days_of_inventory
FROM dim_products p
LEFT JOIN fact_transactions t 
    ON p.product_id = t.product_id 
    AND t.transaction_timestamp >= DATE_SUB(CURRENT_DATE, 7)
    AND t.status = 'completed'
WHERE p.is_current = TRUE
  AND p.in_stock = TRUE
  AND p.stock_quantity < 50
  AND p.is_bestseller = TRUE
GROUP BY 
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.stock_quantity,
    p.discounted_price,
    p.avg_rating
HAVING COUNT(t.transaction_id) > 10
ORDER BY days_of_inventory ASC
LIMIT 50;

-- Trending Products (Fastest Growing Sales)
WITH last_week AS (
    SELECT 
        product_id,
        COUNT(*) AS sales_count
    FROM fact_transactions
    WHERE transaction_timestamp >= DATE_SUB(CURRENT_DATE, 7)
      AND transaction_timestamp < CURRENT_DATE
      AND status = 'completed'
    GROUP BY product_id
),
previous_week AS (
    SELECT 
        product_id,
        COUNT(*) AS sales_count
    FROM fact_transactions
    WHERE transaction_timestamp >= DATE_SUB(CURRENT_DATE, 14)
      AND transaction_timestamp < DATE_SUB(CURRENT_DATE, 7)
      AND status = 'completed'
    GROUP BY product_id
)
SELECT /*+ BROADCAST(p) */
    p.product_id,
    p.product_name,
    p.category,
    COALESCE(lw.sales_count, 0) AS last_week_sales,
    COALESCE(pw.sales_count, 0) AS previous_week_sales,
    COALESCE(lw.sales_count, 0) - COALESCE(pw.sales_count, 0) AS sales_growth,
    ROUND((COALESCE(lw.sales_count, 0) - COALESCE(pw.sales_count, 0)) * 100.0 / 
          NULLIF(pw.sales_count, 0), 2) AS growth_percentage
FROM dim_products p
LEFT JOIN last_week lw ON p.product_id = lw.product_id
LEFT JOIN previous_week pw ON p.product_id = pw.product_id
WHERE p.is_current = TRUE
  AND COALESCE(pw.sales_count, 0) > 0
ORDER BY growth_percentage DESC
LIMIT 20;

-- ============================================================================
-- DASHBOARD 3: CUSTOMER SEGMENTATION ANALYSIS
-- ============================================================================

/*
 * Customer behavior and segmentation metrics
 */

-- Customer Segment Performance
SELECT /*+ BROADCAST(u) */
    u.customer_segment,
    COUNT(DISTINCT u.user_id) AS total_customers,
    COUNT(t.transaction_id) AS total_transactions,
    SUM(t.total_amount) AS total_revenue,
    AVG(t.total_amount) AS avg_order_value,
    SUM(t.total_amount) / COUNT(DISTINCT u.user_id) AS revenue_per_customer,
    ROUND(SUM(t.total_amount) * 100.0 / SUM(SUM(t.total_amount)) OVER (), 2) AS revenue_share_pct
FROM dim_users u
LEFT JOIN fact_transactions t 
    ON u.user_id = t.user_id 
    AND t.transaction_timestamp >= DATE_SUB(CURRENT_DATE, 30)
    AND t.status = 'completed'
WHERE u.is_current = TRUE
  AND u.account_status = 'active'
GROUP BY u.customer_segment
ORDER BY total_revenue DESC;

-- New vs Returning Customer Analysis (Last 30 Days)
SELECT 
    CASE 
        WHEN is_first_purchase = TRUE THEN 'New Customer'
        ELSE 'Returning Customer'
    END AS customer_type,
    COUNT(*) AS transaction_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    COUNT(DISTINCT user_id) AS unique_customers
FROM fact_transactions
WHERE transaction_timestamp >= DATE_SUB(CURRENT_DATE, 30)
  AND status = 'completed'
GROUP BY 
    CASE 
        WHEN is_first_purchase = TRUE THEN 'New Customer'
        ELSE 'Returning Customer'
    END;

-- RFM Analysis (Recency, Frequency, Monetary)
SELECT 
    user_id,
    DATEDIFF(CURRENT_DATE, MAX(DATE(transaction_timestamp))) AS recency_days,
    COUNT(*) AS frequency,
    SUM(total_amount) AS monetary_value,
    AVG(total_amount) AS avg_order_value,
    -- RFM Score (simplified)
    CASE 
        WHEN DATEDIFF(CURRENT_DATE, MAX(DATE(transaction_timestamp))) <= 30 THEN 4
        WHEN DATEDIFF(CURRENT_DATE, MAX(DATE(transaction_timestamp))) <= 60 THEN 3
        WHEN DATEDIFF(CURRENT_DATE, MAX(DATE(transaction_timestamp))) <= 90 THEN 2
        ELSE 1
    END AS recency_score,
    CASE 
        WHEN COUNT(*) >= 20 THEN 4
        WHEN COUNT(*) >= 10 THEN 3
        WHEN COUNT(*) >= 5 THEN 2
        ELSE 1
    END AS frequency_score,
    CASE 
        WHEN SUM(total_amount) >= 50000 THEN 4
        WHEN SUM(total_amount) >= 20000 THEN 3
        WHEN SUM(total_amount) >= 5000 THEN 2
        ELSE 1
    END AS monetary_score
FROM fact_transactions
WHERE status = 'completed'
  AND transaction_timestamp >= DATE_SUB(CURRENT_DATE, 365)
GROUP BY user_id
ORDER BY monetary_value DESC
LIMIT 1000;

-- Geographic Revenue Distribution
SELECT /*+ BROADCAST(u) */
    u.country,
    u.state,
    COUNT(DISTINCT u.user_id) AS total_customers,
    COUNT(t.transaction_id) AS total_orders,
    SUM(t.total_amount) AS total_revenue,
    AVG(t.total_amount) AS avg_order_value
FROM dim_users u
JOIN fact_transactions t 
    ON u.user_id = t.user_id
WHERE u.is_current = TRUE
  AND t.transaction_timestamp >= DATE_SUB(CURRENT_DATE, 30)
  AND t.status = 'completed'
GROUP BY u.country, u.state
ORDER BY total_revenue DESC
LIMIT 50;

-- ============================================================================
-- DASHBOARD 4: CONVERSION FUNNEL ANALYTICS
-- ============================================================================

/*
 * User journey and conversion analysis
 */

-- Daily Conversion Funnel (Last 7 Days)
WITH funnel_events AS (
    SELECT 
        DATE(event_timestamp) AS event_date,
        user_id,
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS added_to_cart,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM fact_clickstream
    WHERE event_timestamp >= DATE_SUB(CURRENT_DATE, 7)
    GROUP BY DATE(event_timestamp), user_id
)
SELECT 
    event_date,
    SUM(viewed) AS total_viewers,
    SUM(added_to_cart) AS total_cart_adds,
    SUM(purchased) AS total_purchases,
    ROUND(SUM(added_to_cart) * 100.0 / NULLIF(SUM(viewed), 0), 2) AS view_to_cart_rate,
    ROUND(SUM(purchased) * 100.0 / NULLIF(SUM(added_to_cart), 0), 2) AS cart_to_purchase_rate,
    ROUND(SUM(purchased) * 100.0 / NULLIF(SUM(viewed), 0), 2) AS overall_conversion_rate
FROM funnel_events
GROUP BY event_date
ORDER BY event_date DESC;

-- Conversion Rate by Traffic Source
WITH source_metrics AS (
    SELECT 
        c.traffic_source,
        COUNT(DISTINCT c.user_id) AS total_visitors,
        SUM(CASE WHEN c.event_type = 'view' THEN 1 ELSE 0 END) AS total_views,
        COUNT(DISTINCT CASE WHEN t.transaction_id IS NOT NULL THEN c.user_id END) AS converted_users,
        COUNT(DISTINCT t.transaction_id) AS total_purchases
    FROM fact_clickstream c
    LEFT JOIN fact_transactions t 
        ON c.user_id = t.user_id 
        AND DATE(c.event_timestamp) = DATE(t.transaction_timestamp)
        AND t.status = 'completed'
    WHERE c.event_timestamp >= DATE_SUB(CURRENT_DATE, 30)
    GROUP BY c.traffic_source
)
SELECT 
    traffic_source,
    total_visitors,
    total_views,
    converted_users,
    total_purchases,
    ROUND(converted_users * 100.0 / total_visitors, 2) AS conversion_rate,
    ROUND(total_purchases * 1.0 / total_visitors, 2) AS purchases_per_visitor
FROM source_metrics
ORDER BY conversion_rate DESC;

-- Conversion Rate by Device Type
SELECT 
    c.device_type,
    COUNT(DISTINCT c.session_id) AS total_sessions,
    COUNT(DISTINCT CASE WHEN c.event_type = 'view' THEN c.session_id END) AS view_sessions,
    COUNT(DISTINCT CASE WHEN c.event_type = 'add_to_cart' THEN c.session_id END) AS cart_sessions,
    COUNT(DISTINCT CASE WHEN c.event_type = 'purchase' THEN c.session_id END) AS purchase_sessions,
    ROUND(COUNT(DISTINCT CASE WHEN c.event_type = 'purchase' THEN c.session_id END) * 100.0 / 
          COUNT(DISTINCT c.session_id), 2) AS conversion_rate
FROM fact_clickstream c
WHERE c.event_timestamp >= DATE_SUB(CURRENT_DATE, 30)
GROUP BY c.device_type
ORDER BY conversion_rate DESC;

-- ============================================================================
-- DASHBOARD 5: ABANDONED CART ANALYTICS
-- ============================================================================

/*
 * Identify and analyze cart abandonment
 */

-- Cart Abandonment Rate (Last 7 Days)
WITH cart_sessions AS (
    SELECT 
        DATE(event_timestamp) AS cart_date,
        session_id,
        user_id,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS had_cart,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS completed_purchase
    FROM fact_clickstream
    WHERE event_timestamp >= DATE_SUB(CURRENT_DATE, 7)
    GROUP BY DATE(event_timestamp), session_id, user_id
)
SELECT 
    cart_date,
    SUM(had_cart) AS total_carts,
    SUM(completed_purchase) AS completed_purchases,
    SUM(had_cart) - SUM(completed_purchase) AS abandoned_carts,
    ROUND((SUM(had_cart) - SUM(completed_purchase)) * 100.0 / NULLIF(SUM(had_cart), 0), 2) AS abandonment_rate
FROM cart_sessions
WHERE had_cart = 1
GROUP BY cart_date
ORDER BY cart_date DESC;

-- Users with Abandoned Carts (Recovery Opportunity)
WITH recent_cart_users AS (
    SELECT DISTINCT
        c.user_id,
        MAX(c.event_timestamp) AS last_cart_timestamp
    FROM fact_clickstream c
    WHERE c.event_type = 'add_to_cart'
      AND c.event_timestamp >= DATE_SUB(CURRENT_DATE, 3)
    GROUP BY c.user_id
),
recent_purchasers AS (
    SELECT DISTINCT user_id
    FROM fact_transactions
    WHERE transaction_timestamp >= DATE_SUB(CURRENT_DATE, 3)
)
SELECT /*+ BROADCAST(u) */
    u.user_id,
    u.full_name,
    u.email,
    u.customer_segment,
    rcu.last_cart_timestamp,
    DATEDIFF(CURRENT_DATE, DATE(rcu.last_cart_timestamp)) AS days_since_cart,
    u.lifetime_value
FROM recent_cart_users rcu
JOIN dim_users u ON rcu.user_id = u.user_id AND u.is_current = TRUE
LEFT JOIN recent_purchasers rp ON rcu.user_id = rp.user_id
WHERE rp.user_id IS NULL  -- Didn't purchase
  AND u.email_subscribed = TRUE
ORDER BY u.lifetime_value DESC, days_since_cart ASC
LIMIT 1000;

-- Products Most Often Abandoned
WITH abandoned_products AS (
    SELECT 
        c.product_id,
        c.session_id
    FROM fact_clickstream c
    WHERE c.event_type = 'add_to_cart'
      AND c.event_timestamp >= DATE_SUB(CURRENT_DATE, 30)
      AND NOT EXISTS (
          SELECT 1 
          FROM fact_clickstream c2
          WHERE c2.session_id = c.session_id
            AND c2.event_type = 'purchase'
      )
)
SELECT /*+ BROADCAST(p) */
    p.product_id,
    p.product_name,
    p.category,
    p.discounted_price,
    COUNT(*) AS abandonment_count,
    p.avg_rating,
    p.stock_quantity
FROM abandoned_products ap
JOIN dim_products p ON ap.product_id = p.product_id AND p.is_current = TRUE
GROUP BY 
    p.product_id,
    p.product_name,
    p.category,
    p.discounted_price,
    p.avg_rating,
    p.stock_quantity
ORDER BY abandonment_count DESC
LIMIT 50;

/*
 * ============================================================================
 * EXECUTION NOTES:
 * ============================================================================
 * 
 * 1. Run queries via impala-shell:
 *    impala-shell -i <host> -f impala_bi_queries.sql
 * 
 * 2. Or execute individual queries in BI tools:
 *    - Tableau
 *    - Power BI
 *    - Apache Superset
 *    - Looker
 * 
 * 3. Performance Tips:
 *    - Queries use partition pruning
 *    - BROADCAST hints for small dimension tables
 *    - All queries tested with EXPLAIN
 *    - Results cacheable for repeated queries
 * 
 * 4. Refresh Schedule:
 *    - Real-time metrics: Every 5-15 minutes
 *    - Daily aggregates: Once per day
 *    - Historical trends: Once per hour
 * 
 * ============================================================================
 */
