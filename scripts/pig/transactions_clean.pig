/*
 * Transaction Data Cleaning - Pig ETL Script
 * ===========================================
 * 
 * PURPOSE:
 * Clean and transform raw transaction data from CSV/Parquet format.
 * 
 * OPERATIONS:
 * 1. Load raw transaction data
 * 2. Validate transaction records
 * 3. Clean and normalize data
 * 4. Calculate derived fields
 * 5. Remove duplicates and invalid transactions
 * 6. Aggregate transaction metrics
 * 7. Store cleaned data in Parquet format
 * 
 * INPUT: /ecommerce/raw/transactions/YYYY/MM/DD/*.csv
 * OUTPUT: /ecommerce/clean/transactions/
 * 
 * USAGE:
 * pig -param input_path=/ecommerce/raw/transactions/2025/11/30 \
 *     -param output_path=/ecommerce/clean/transactions/2025/11/30 \
 *     transactions_clean.pig
 */

-- ============================================================================
-- STEP 1: CONFIGURATION
-- ============================================================================

-- Enable compression
SET output.compression.enabled true;
SET output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

-- Set parallelism
SET default_parallel 20;

-- Set date format
SET pig.datetime.default.tz 'UTC';

-- ============================================================================
-- STEP 2: LOAD RAW TRANSACTION DATA
-- ============================================================================

-- Load transactions from CSV files
raw_transactions = LOAD '$input_path/*.csv' 
    USING PigStorage(',') 
    AS (
        transaction_id:chararray,
        user_id:chararray,
        product_id:chararray,
        category:chararray,
        quantity:int,
        unit_price:double,
        subtotal:double,
        discount_percent:int,
        discount_amount:double,
        tax_amount:double,
        shipping_fee:double,
        total_amount:double,
        payment_method:chararray,
        status:chararray,
        transaction_timestamp:chararray,
        delivery_pincode:chararray,
        delivery_city:chararray,
        delivery_state:chararray,
        delivery_country:chararray,
        expected_delivery_date:chararray,
        actual_delivery_date:chararray,
        customer_email:chararray,
        customer_phone:chararray,
        order_source:chararray,
        is_first_purchase:chararray,
        coupon_code:chararray
    );

-- Skip header row (if present)
transactions_no_header = FILTER raw_transactions BY transaction_id != 'transaction_id';

-- Display schema
-- DESCRIBE transactions_no_header;

-- ============================================================================
-- STEP 3: DATA VALIDATION
-- ============================================================================

-- Filter out records with NULL or invalid critical fields
valid_transactions = FILTER transactions_no_header BY 
    transaction_id IS NOT NULL AND
    user_id IS NOT NULL AND
    product_id IS NOT NULL AND
    transaction_timestamp IS NOT NULL AND
    total_amount IS NOT NULL AND
    total_amount > 0 AND
    quantity IS NOT NULL AND
    quantity > 0 AND
    quantity < 100;  -- Reasonable quantity limit

-- ============================================================================
-- STEP 4: DATA CLEANING AND TRANSFORMATION
-- ============================================================================

-- Clean and enrich transaction data
cleaned_transactions = FOREACH valid_transactions GENERATE
    transaction_id,
    user_id,
    product_id,
    LOWER(TRIM(category)) AS category,
    quantity,
    unit_price,
    subtotal,
    discount_percent,
    discount_amount,
    tax_amount,
    shipping_fee,
    total_amount,
    LOWER(TRIM(payment_method)) AS payment_method,
    LOWER(TRIM(status)) AS status,
    ToDate(transaction_timestamp, 'yyyy-MM-dd HH:mm:ss') AS transaction_ts,
    TRIM(delivery_pincode) AS delivery_pincode,
    TRIM(delivery_city) AS delivery_city,
    TRIM(delivery_state) AS delivery_state,
    TRIM(delivery_country) AS delivery_country,
    (expected_delivery_date IS NOT NULL ? 
        ToDate(expected_delivery_date, 'yyyy-MM-dd') : NULL) AS expected_delivery_ts,
    (actual_delivery_date IS NOT NULL ? 
        ToDate(actual_delivery_date, 'yyyy-MM-dd') : NULL) AS actual_delivery_ts,
    LOWER(TRIM(customer_email)) AS customer_email,
    TRIM(customer_phone) AS customer_phone,
    LOWER(TRIM(order_source)) AS order_source,
    (is_first_purchase == 'True' OR is_first_purchase == 'true' ? 1 : 0) AS is_first_purchase,
    TRIM(coupon_code) AS coupon_code;

-- ============================================================================
-- STEP 5: ADD DERIVED FIELDS
-- ============================================================================

-- Add partitioning fields and business logic fields
enriched_transactions = FOREACH cleaned_transactions GENERATE
    *,
    -- Extract date components for partitioning
    ToString(transaction_ts, 'yyyy-MM-dd') AS transaction_date,
    GetYear(transaction_ts) AS year,
    GetMonth(transaction_ts) AS month,
    GetDay(transaction_ts) AS day,
    GetHour(transaction_ts) AS hour,
    
    -- Calculate profit margin (assuming 30% avg margin)
    ROUND(total_amount * 0.30) AS estimated_profit,
    
    -- Delivery time calculation (if delivered)
    (actual_delivery_ts IS NOT NULL ? 
        DaysBetween(actual_delivery_ts, transaction_ts) : NULL) AS delivery_time_days,
    
    -- Late delivery flag
    (actual_delivery_ts IS NOT NULL AND expected_delivery_ts IS NOT NULL ? 
        (actual_delivery_ts > expected_delivery_ts ? 1 : 0) : 0) AS is_late_delivery,
    
    -- Revenue buckets
    (total_amount < 500 ? 'low' :
     (total_amount < 2000 ? 'medium' :
      (total_amount < 10000 ? 'high' : 'premium'))) AS revenue_bucket;

-- ============================================================================
-- STEP 6: FILTER VALID STATUSES AND PAYMENT METHODS
-- ============================================================================

-- Keep only valid transaction statuses
status_filtered = FILTER enriched_transactions BY 
    status IN ('completed', 'pending', 'cancelled', 'refunded');

-- Keep only valid payment methods
payment_filtered = FILTER status_filtered BY 
    payment_method IN ('credit_card', 'debit_card', 'upi', 'paypal', 'net_banking', 'cod');

-- ============================================================================
-- STEP 7: REMOVE DUPLICATES
-- ============================================================================

-- Group by transaction_id to identify duplicates
grouped_transactions = GROUP payment_filtered BY transaction_id;

-- Keep only the latest transaction (in case of duplicates)
deduped_transactions = FOREACH grouped_transactions {
    sorted = ORDER payment_filtered BY transaction_ts DESC;
    latest = LIMIT sorted 1;
    GENERATE FLATTEN(latest);
}

-- ============================================================================
-- STEP 8: DATA QUALITY CHECKS - DETECT ANOMALIES
-- ============================================================================

-- Flag suspicious transactions (unusually high amounts)
final_transactions = FOREACH deduped_transactions GENERATE
    *,
    (total_amount > 100000 ? 1 : 0) AS is_high_value,
    (quantity > 50 ? 1 : 0) AS is_bulk_order,
    (discount_percent > 50 ? 1 : 0) AS is_high_discount;

-- ============================================================================
-- STEP 9: GENERATE STATISTICS
-- ============================================================================

-- Overall statistics
overall_stats = GROUP final_transactions ALL;
summary_stats = FOREACH overall_stats GENERATE
    COUNT(final_transactions) AS total_transactions,
    SUM(final_transactions.total_amount) AS total_revenue,
    AVG(final_transactions.total_amount) AS avg_order_value,
    SUM(final_transactions.quantity) AS total_items_sold;

STORE summary_stats INTO '$output_path/stats/summary' 
    USING PigStorage(',');

-- Status-wise breakdown
status_stats = GROUP final_transactions BY status;
status_counts = FOREACH status_stats GENERATE
    group AS status,
    COUNT(final_transactions) AS transaction_count,
    SUM(final_transactions.total_amount) AS total_revenue;

STORE status_counts INTO '$output_path/stats/status_breakdown' 
    USING PigStorage(',');

-- Payment method breakdown
payment_stats = GROUP final_transactions BY payment_method;
payment_counts = FOREACH payment_stats GENERATE
    group AS payment_method,
    COUNT(final_transactions) AS transaction_count,
    SUM(final_transactions.total_amount) AS total_revenue;

STORE payment_counts INTO '$output_path/stats/payment_method_breakdown' 
    USING PigStorage(',');

-- Category performance
category_stats = GROUP final_transactions BY category;
category_performance = FOREACH category_stats GENERATE
    group AS category,
    COUNT(final_transactions) AS transaction_count,
    SUM(final_transactions.total_amount) AS total_revenue,
    AVG(final_transactions.total_amount) AS avg_order_value;

STORE category_performance INTO '$output_path/stats/category_performance' 
    USING PigStorage(',');

-- Daily revenue trends
daily_stats = GROUP final_transactions BY transaction_date;
daily_revenue = FOREACH daily_stats GENERATE
    group AS date,
    COUNT(final_transactions) AS transaction_count,
    SUM(final_transactions.total_amount) AS daily_revenue,
    AVG(final_transactions.total_amount) AS avg_order_value;

STORE daily_revenue INTO '$output_path/stats/daily_revenue' 
    USING PigStorage(',');

-- Hourly patterns
hourly_stats = GROUP final_transactions BY hour;
hourly_pattern = FOREACH hourly_stats GENERATE
    group AS hour,
    COUNT(final_transactions) AS transaction_count;

STORE hourly_pattern INTO '$output_path/stats/hourly_pattern' 
    USING PigStorage(',');

-- ============================================================================
-- STEP 10: IDENTIFY TOP CUSTOMERS
-- ============================================================================

-- Group by user to find top customers
user_stats = GROUP final_transactions BY user_id;
user_metrics = FOREACH user_stats GENERATE
    group AS user_id,
    COUNT(final_transactions) AS total_orders,
    SUM(final_transactions.total_amount) AS lifetime_value,
    AVG(final_transactions.total_amount) AS avg_order_value;

-- Sort by lifetime value
top_customers = ORDER user_metrics BY lifetime_value DESC;
top_100_customers = LIMIT top_customers 100;

STORE top_100_customers INTO '$output_path/stats/top_customers' 
    USING PigStorage(',');

-- ============================================================================
-- STEP 11: SAVE CLEANED DATA
-- ============================================================================

-- Store cleaned transaction data in Parquet format
STORE final_transactions INTO '$output_path/data' 
    USING org.apache.parquet.pig.ParquetStorer();

-- Also store as CSV for easy viewing (optional)
STORE final_transactions INTO '$output_path/data_csv' 
    USING PigStorage(',');

-- ============================================================================
-- STEP 12: QUALITY REPORT
-- ============================================================================

quality_report = FOREACH overall_stats GENERATE
    COUNT(final_transactions) AS total_clean_transactions,
    SUM(final_transactions.is_high_value) AS high_value_count,
    SUM(final_transactions.is_bulk_order) AS bulk_order_count,
    SUM(final_transactions.is_high_discount) AS high_discount_count;

STORE quality_report INTO '$output_path/quality_report' 
    USING PigStorage(',');

/*
 * ============================================================================
 * EXECUTION NOTES:
 * ============================================================================
 * 
 * 1. BEFORE RUNNING:
 *    - Ensure transaction CSV/Parquet files exist in input path
 *    - Remove output directory if it exists
 *    - Check HDFS space availability
 * 
 * 2. RUNNING THE SCRIPT:
 *    
 *    Local mode (for testing):
 *    pig -x local -param input_path=../data/raw/transactions/2025/11/30 \
 *        -param output_path=../data/clean/transactions/2025/11/30 \
 *        transactions_clean.pig
 *    
 *    MapReduce mode (production):
 *    pig -x mapreduce -param input_path=/ecommerce/raw/transactions/2025/11/30 \
 *        -param output_path=/ecommerce/clean/transactions/2025/11/30 \
 *        transactions_clean.pig
 * 
 * 3. OUTPUT STRUCTURE:
 *    /ecommerce/clean/transactions/YYYY/MM/DD/
 *        ├── data/                      (cleaned Parquet files)
 *        ├── data_csv/                  (CSV backup)
 *        ├── stats/
 *        │   ├── summary/               (overall statistics)
 *        │   ├── status_breakdown/      (by status)
 *        │   ├── payment_method_breakdown/
 *        │   ├── category_performance/
 *        │   ├── daily_revenue/
 *        │   ├── hourly_pattern/
 *        │   └── top_customers/
 *        └── quality_report/
 * 
 * 4. VALIDATION:
 *    - Compare input vs output record counts
 *    - Check for data loss in critical fields
 *    - Verify revenue calculations
 * 
 * ============================================================================
 */
