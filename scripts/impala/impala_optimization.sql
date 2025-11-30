-- ============================================================================
-- IMPALA OPTIMIZATION & MAINTENANCE SCRIPTS
-- ============================================================================
-- 
-- PURPOSE:
-- Optimize Impala tables for better query performance
-- 
-- OPERATIONS:
-- 1. REFRESH metadata after Hive loads
-- 2. COMPUTE STATS for query optimization
-- 3. INVALIDATE METADATA when needed
-- 
-- NOTE: Run these after ETL loads complete
-- ============================================================================

-- Connect to Impala (run via impala-shell)
-- impala-shell -i <impala-daemon-host>

USE ecommerce_dwh;

-- ============================================================================
-- STEP 1: REFRESH TABLE METADATA
-- ============================================================================

/*
 * REFRESH updates Impala's metadata cache with new partitions/files
 * Run after Hive inserts or HDFS file additions
 */

-- Refresh dimension tables
REFRESH dim_users;
REFRESH dim_products;

-- Refresh fact tables
REFRESH fact_clickstream;
REFRESH fact_transactions;

-- Refresh aggregate tables
REFRESH agg_daily_sales;
REFRESH agg_user_metrics;

-- Refresh materialized views
REFRESH mv_daily_revenue_by_category;

PRINT('✓ All tables refreshed');

-- ============================================================================
-- STEP 2: INVALIDATE METADATA (Full Refresh)
-- ============================================================================

/*
 * INVALIDATE METADATA forces complete metadata reload
 * Use when:
 * - Table structure changes
 * - New tables created
 * - Major data updates
 * 
 * WARNING: Expensive operation, use sparingly
 */

-- Invalidate specific table (safer)
INVALIDATE METADATA dim_users;
INVALIDATE METADATA fact_transactions;

-- Invalidate all metadata (use only when necessary)
-- INVALIDATE METADATA;

-- ============================================================================
-- STEP 3: COMPUTE TABLE STATISTICS
-- ============================================================================

/*
 * COMPUTE STATS collects table and column statistics
 * Critical for query optimization and join ordering
 * Run after major data loads
 */

-- Compute stats for dimension tables
COMPUTE STATS dim_users;
COMPUTE STATS dim_products;

PRINT('✓ Dimension table stats computed');

-- Compute stats for fact tables (with all partitions)
-- Note: This can be time-consuming for large tables
COMPUTE STATS fact_clickstream;
COMPUTE STATS fact_transactions;

PRINT('✓ Fact table stats computed');

-- Compute stats for specific partitions only (faster)
-- COMPUTE INCREMENTAL STATS fact_clickstream 
-- PARTITION (year=2025, month=11, day=30);

-- COMPUTE INCREMENTAL STATS fact_transactions 
-- PARTITION (year=2025, month=11, day=30);

-- Compute stats for aggregate tables
COMPUTE STATS agg_daily_sales;
COMPUTE STATS agg_user_metrics;

PRINT('✓ Aggregate table stats computed');

-- ============================================================================
-- STEP 4: SHOW STATISTICS
-- ============================================================================

/*
 * View statistics to verify computation
 */

-- Show table statistics
SHOW TABLE STATS fact_transactions;
SHOW TABLE STATS fact_clickstream;

-- Show column statistics
SHOW COLUMN STATS dim_users;
SHOW COLUMN STATS dim_products;

-- ============================================================================
-- STEP 5: COMPUTE INCREMENTAL STATS (Recommended for Partitioned Tables)
-- ============================================================================

/*
 * COMPUTE INCREMENTAL STATS only processes new/changed partitions
 * Much faster than full COMPUTE STATS
 * Best practice for partitioned fact tables
 */

-- Enable incremental stats
SET COMPUTE_STATS_MODE=INCREMENTAL;

-- Compute incremental stats for today's partition
COMPUTE INCREMENTAL STATS fact_clickstream 
PARTITION (
    year=${hiveconf:year}, 
    month=${hiveconf:month}, 
    day=${hiveconf:day}
);

COMPUTE INCREMENTAL STATS fact_transactions 
PARTITION (
    year=${hiveconf:year}, 
    month=${hiveconf:month}, 
    day=${hiveconf:day}
);

PRINT('✓ Incremental stats computed for current partitions');

-- ============================================================================
-- STEP 6: DROP STATISTICS (If Needed)
-- ============================================================================

/*
 * Drop stats to recompute from scratch
 * Use when stats become stale or inaccurate
 */

-- Drop stats for specific table
-- DROP STATS dim_users;
-- DROP STATS fact_transactions;

-- Drop incremental stats
-- DROP INCREMENTAL STATS fact_clickstream PARTITION (year=2025, month=11, day=30);

-- ============================================================================
-- STEP 7: QUERY HINTS FOR OPTIMIZATION
-- ============================================================================

/*
 * Example queries with optimization hints
 * Use these patterns in your BI queries
 */

-- Hint 1: Broadcast join for small dimension tables
-- Forces small table to be broadcast to all nodes
SELECT /*+ BROADCAST(u) */
    t.transaction_id,
    u.full_name,
    t.total_amount
FROM fact_transactions t
JOIN dim_users u ON t.user_id = u.user_id
WHERE t.year = 2025 AND t.month = 11
LIMIT 100;

-- Hint 2: Shuffle join for large tables
-- Distributes both tables across cluster
SELECT /*+ SHUFFLE */
    c.event_id,
    t.transaction_id,
    c.event_timestamp
FROM fact_clickstream c
JOIN fact_transactions t 
    ON c.user_id = t.user_id 
    AND c.year = t.year 
    AND c.month = t.month
WHERE c.year = 2025 AND c.month = 11
LIMIT 100;

-- Hint 3: Straight join (preserve join order)
SELECT STRAIGHT_JOIN
    u.full_name,
    p.product_name,
    t.total_amount
FROM dim_users u
JOIN fact_transactions t ON u.user_id = t.user_id
JOIN dim_products p ON t.product_id = p.product_id
WHERE u.customer_segment = 'vip'
  AND t.year = 2025
LIMIT 100;

-- ============================================================================
-- STEP 8: PARTITION PRUNING EXAMPLES
-- ============================================================================

/*
 * Always filter on partition columns for better performance
 */

-- Good: Uses partition pruning
SELECT COUNT(*) 
FROM fact_transactions
WHERE year = 2025 
  AND month = 11 
  AND day = 30
  AND status = 'completed';

-- Bad: Full table scan (missing partition filters)
-- SELECT COUNT(*) 
-- FROM fact_transactions
-- WHERE status = 'completed';

-- Good: Range query with partition filters
SELECT 
    DATE(transaction_timestamp) AS sale_date,
    SUM(total_amount) AS daily_revenue
FROM fact_transactions
WHERE year = 2025 
  AND month BETWEEN 10 AND 11
  AND status = 'completed'
GROUP BY DATE(transaction_timestamp)
ORDER BY sale_date DESC;

-- ============================================================================
-- STEP 9: VERIFY QUERY PERFORMANCE
-- ============================================================================

/*
 * Use EXPLAIN to analyze query plans
 */

-- Explain query plan
EXPLAIN 
SELECT 
    u.customer_segment,
    COUNT(*) AS transaction_count,
    SUM(t.total_amount) AS total_revenue
FROM fact_transactions t
JOIN dim_users u ON t.user_id = u.user_id AND u.is_current = TRUE
WHERE t.year = 2025 
  AND t.month = 11
  AND t.status = 'completed'
GROUP BY u.customer_segment;

-- Explain with profile (more detailed)
-- EXPLAIN PROFILE
-- SELECT ...

-- ============================================================================
-- STEP 10: AUTOMATED OPTIMIZATION SCRIPT
-- ============================================================================

/*
 * Shell script to run all optimizations
 * Save as: impala_optimize_all.sh
 */

/*
#!/bin/bash

# Impala Optimization Script
echo "Starting Impala optimization..."

impala-shell -i localhost -q "USE ecommerce_dwh;"
impala-shell -i localhost -q "REFRESH dim_users;"
impala-shell -i localhost -q "REFRESH dim_products;"
impala-shell -i localhost -q "REFRESH fact_clickstream;"
impala-shell -i localhost -q "REFRESH fact_transactions;"
impala-shell -i localhost -q "REFRESH agg_daily_sales;"
impala-shell -i localhost -q "REFRESH agg_user_metrics;"

echo "Computing statistics..."
impala-shell -i localhost -q "COMPUTE STATS dim_users;"
impala-shell -i localhost -q "COMPUTE STATS dim_products;"
impala-shell -i localhost -q "COMPUTE STATS fact_clickstream;"
impala-shell -i localhost -q "COMPUTE STATS fact_transactions;"
impala-shell -i localhost -q "COMPUTE STATS agg_daily_sales;"
impala-shell -i localhost -q "COMPUTE STATS agg_user_metrics;"

echo "✓ Impala optimization complete!"
*/

/*
 * ============================================================================
 * EXECUTION NOTES:
 * ============================================================================
 * 
 * 1. Run optimization after ETL:
 *    impala-shell -i <host> -f impala_optimization.sql
 * 
 * 2. Schedule via Airflow after Hive ETL completes
 * 
 * 3. Monitor query performance:
 *    - Check Impala Web UI (port 25000)
 *    - Review query profiles
 *    - Analyze slow queries
 * 
 * 4. Best Practices:
 *    - Always REFRESH after Hive loads
 *    - COMPUTE STATS after major data changes
 *    - Use INCREMENTAL STATS for partitioned tables
 *    - Filter on partition columns
 *    - Use appropriate join hints
 * 
 * 5. Performance Tuning:
 *    - Increase executor memory for large joins
 *    - Tune admission control settings
 *    - Enable result caching
 *    - Use Parquet/ORC with compression
 * 
 * ============================================================================
 */
