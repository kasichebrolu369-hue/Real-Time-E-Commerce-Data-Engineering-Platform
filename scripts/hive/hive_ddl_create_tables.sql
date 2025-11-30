-- ============================================================================
-- HIVE DATA WAREHOUSE - DDL SCRIPTS
-- ============================================================================
-- 
-- PURPOSE:
-- Create Hive database and dimension/fact tables for e-commerce analytics
-- 
-- ARCHITECTURE:
-- - Star Schema design
-- - Dimension tables: Users, Products
-- - Fact tables: Clickstream, Transactions
-- - Partitioning by date for performance
-- - ORC/Parquet formats with compression
-- - Bucketing for join optimization
--
-- ============================================================================

-- ============================================================================
-- STEP 1: CREATE DATABASE
-- ============================================================================

-- Create ecommerce database
CREATE DATABASE IF NOT EXISTS ecommerce_dwh
COMMENT 'E-commerce Data Warehouse'
LOCATION '/ecommerce/warehouse/';

USE ecommerce_dwh;

-- ============================================================================
-- STEP 2: DIMENSION TABLE - USERS
-- ============================================================================

/*
 * dim_users: User/Customer dimension table
 * Type: Slowly Changing Dimension (SCD Type 2)
 * 
 * Contains user profile information, demographics, and customer segments
 */

CREATE EXTERNAL TABLE IF NOT EXISTS dim_users (
    user_id STRING COMMENT 'Unique user identifier',
    first_name STRING COMMENT 'User first name',
    last_name STRING COMMENT 'User last name',
    full_name STRING COMMENT 'Full name',
    gender STRING COMMENT 'User gender',
    date_of_birth DATE COMMENT 'Date of birth',
    age INT COMMENT 'Current age',
    email STRING COMMENT 'Email address (PII)',
    phone STRING COMMENT 'Phone number (PII)',
    address STRING COMMENT 'Street address (PII)',
    city STRING COMMENT 'City',
    state STRING COMMENT 'State/Province',
    country STRING COMMENT 'Country',
    zipcode STRING COMMENT 'Postal code',
    account_created_at TIMESTAMP COMMENT 'Account creation timestamp',
    last_login_at TIMESTAMP COMMENT 'Last login timestamp',
    account_status STRING COMMENT 'Account status: active, inactive, suspended, closed',
    customer_segment STRING COMMENT 'Customer segment: vip, loyal, regular, occasional, new',
    total_purchases INT COMMENT 'Total number of purchases',
    lifetime_value DECIMAL(12,2) COMMENT 'Customer lifetime value',
    average_order_value DECIMAL(10,2) COMMENT 'Average order value',
    preferred_categories STRING COMMENT 'Comma-separated preferred categories',
    email_subscribed BOOLEAN COMMENT 'Email subscription flag',
    sms_subscribed BOOLEAN COMMENT 'SMS subscription flag',
    preferred_language STRING COMMENT 'Preferred language',
    preferred_currency STRING COMMENT 'Preferred currency',
    is_premium_member BOOLEAN COMMENT 'Premium membership flag',
    referral_code STRING COMMENT 'User referral code',
    kyc_verified BOOLEAN COMMENT 'KYC verification status',
    
    -- SCD Type 2 fields
    effective_date DATE COMMENT 'Start date of this record version',
    expiration_date DATE COMMENT 'End date of this record version',
    is_current BOOLEAN COMMENT 'Flag indicating current active record'
)
COMMENT 'User/Customer Dimension Table'
STORED AS ORC
LOCATION '/ecommerce/warehouse/dim_users/'
TBLPROPERTIES (
    'orc.compress'='SNAPPY',
    'orc.stripe.size'='268435456',
    'orc.row.index.stride'='10000',
    'transactional'='false'
);

-- ============================================================================
-- STEP 3: DIMENSION TABLE - PRODUCTS
-- ============================================================================

/*
 * dim_products: Product catalog dimension table
 * Type: Slowly Changing Dimension (SCD Type 2)
 * 
 * Contains product details, pricing, inventory, and ratings
 */

CREATE EXTERNAL TABLE IF NOT EXISTS dim_products (
    product_id STRING COMMENT 'Unique product identifier',
    product_name STRING COMMENT 'Product name',
    category STRING COMMENT 'Main product category',
    subcategory STRING COMMENT 'Product subcategory',
    brand STRING COMMENT 'Product brand',
    cost_price DECIMAL(10,2) COMMENT 'Cost price',
    selling_price DECIMAL(10,2) COMMENT 'Listed selling price',
    discount_percent INT COMMENT 'Current discount percentage',
    discounted_price DECIMAL(10,2) COMMENT 'Final discounted price',
    currency STRING COMMENT 'Currency code',
    stock_quantity INT COMMENT 'Current stock quantity',
    in_stock BOOLEAN COMMENT 'In stock flag',
    low_stock_alert BOOLEAN COMMENT 'Low stock alert flag',
    avg_rating DECIMAL(3,2) COMMENT 'Average product rating',
    num_ratings INT COMMENT 'Number of ratings',
    num_reviews INT COMMENT 'Number of reviews',
    weight_kg DECIMAL(8,2) COMMENT 'Product weight in kg',
    dimensions STRING COMMENT 'Product dimensions',
    condition STRING COMMENT 'Product condition: new, refurbished, used',
    warranty_months INT COMMENT 'Warranty period in months',
    manufacturer STRING COMMENT 'Manufacturer name',
    country_of_origin STRING COMMENT 'Country of origin',
    product_added_date DATE COMMENT 'Date product was added',
    last_updated_date DATE COMMENT 'Last update date',
    is_featured BOOLEAN COMMENT 'Featured product flag',
    is_bestseller BOOLEAN COMMENT 'Bestseller flag',
    free_shipping BOOLEAN COMMENT 'Free shipping eligibility',
    returnable BOOLEAN COMMENT 'Product returnable flag',
    return_window_days INT COMMENT 'Return window in days',
    tags STRING COMMENT 'Product tags for search',
    sku STRING COMMENT 'Stock Keeping Unit',
    barcode STRING COMMENT 'Product barcode',
    supplier_id STRING COMMENT 'Supplier identifier',
    reorder_level INT COMMENT 'Reorder level threshold',
    max_order_quantity INT COMMENT 'Maximum order quantity',
    
    -- SCD Type 2 fields
    effective_date DATE COMMENT 'Start date of this record version',
    expiration_date DATE COMMENT 'End date of this record version',
    is_current BOOLEAN COMMENT 'Flag indicating current active record'
)
COMMENT 'Product Catalog Dimension Table'
PARTITIONED BY (category_partition STRING)
STORED AS ORC
LOCATION '/ecommerce/warehouse/dim_products/'
TBLPROPERTIES (
    'orc.compress'='SNAPPY',
    'orc.stripe.size'='268435456',
    'orc.create.index'='true',
    'transactional'='false'
);

-- ============================================================================
-- STEP 4: FACT TABLE - CLICKSTREAM
-- ============================================================================

/*
 * fact_clickstream: Clickstream events fact table
 * Grain: One record per clickstream event
 * 
 * Contains all user interaction events with the website/app
 */

CREATE EXTERNAL TABLE IF NOT EXISTS fact_clickstream (
    event_id STRING COMMENT 'Unique event identifier',
    user_id STRING COMMENT 'FK to dim_users',
    product_id STRING COMMENT 'FK to dim_products (nullable)',
    session_id STRING COMMENT 'Session identifier',
    event_timestamp TIMESTAMP COMMENT 'Event timestamp',
    page_url STRING COMMENT 'Page URL',
    event_type STRING COMMENT 'Event type: view, add_to_cart, remove_from_cart, purchase, exit',
    device_type STRING COMMENT 'Device type: mobile, desktop, tablet',
    ip_address STRING COMMENT 'User IP address (PII)',
    traffic_source STRING COMMENT 'Traffic source',
    referrer STRING COMMENT 'Referrer URL',
    user_agent STRING COMMENT 'User agent string',
    country STRING COMMENT 'Country from IP',
    city STRING COMMENT 'City from IP',
    browser STRING COMMENT 'Browser type',
    os STRING COMMENT 'Operating system',
    hour INT COMMENT 'Hour of day (0-23)',
    day_of_week INT COMMENT 'Day of week (1-7)',
    is_weekend BOOLEAN COMMENT 'Weekend flag',
    session_duration INT COMMENT 'Session duration in seconds'
)
COMMENT 'Clickstream Events Fact Table'
PARTITIONED BY (
    year INT COMMENT 'Year',
    month INT COMMENT 'Month',
    day INT COMMENT 'Day'
)
CLUSTERED BY (user_id) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION '/ecommerce/warehouse/fact_clickstream/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'parquet.block.size'='268435456',
    'transactional'='false'
);

-- ============================================================================
-- STEP 5: FACT TABLE - TRANSACTIONS
-- ============================================================================

/*
 * fact_transactions: Transaction/Orders fact table
 * Grain: One record per transaction line item
 * 
 * Contains all e-commerce transactions with complete order details
 */

CREATE EXTERNAL TABLE IF NOT EXISTS fact_transactions (
    transaction_id STRING COMMENT 'Unique transaction identifier',
    user_id STRING COMMENT 'FK to dim_users',
    product_id STRING COMMENT 'FK to dim_products',
    transaction_timestamp TIMESTAMP COMMENT 'Transaction timestamp',
    quantity INT COMMENT 'Quantity purchased',
    unit_price DECIMAL(10,2) COMMENT 'Unit price',
    subtotal DECIMAL(12,2) COMMENT 'Subtotal amount',
    discount_percent INT COMMENT 'Discount percentage',
    discount_amount DECIMAL(10,2) COMMENT 'Discount amount',
    tax_amount DECIMAL(10,2) COMMENT 'Tax amount',
    shipping_fee DECIMAL(8,2) COMMENT 'Shipping fee',
    total_amount DECIMAL(12,2) COMMENT 'Total order amount',
    payment_method STRING COMMENT 'Payment method used',
    status STRING COMMENT 'Transaction status: completed, pending, cancelled, refunded',
    delivery_pincode STRING COMMENT 'Delivery postal code',
    delivery_city STRING COMMENT 'Delivery city',
    delivery_state STRING COMMENT 'Delivery state',
    delivery_country STRING COMMENT 'Delivery country',
    expected_delivery_date DATE COMMENT 'Expected delivery date',
    actual_delivery_date DATE COMMENT 'Actual delivery date',
    delivery_time_days INT COMMENT 'Days taken for delivery',
    is_late_delivery BOOLEAN COMMENT 'Late delivery flag',
    customer_email STRING COMMENT 'Customer email (PII)',
    customer_phone STRING COMMENT 'Customer phone (PII)',
    order_source STRING COMMENT 'Order source: web, mobile_app, mobile_web',
    is_first_purchase BOOLEAN COMMENT 'First purchase flag',
    coupon_code STRING COMMENT 'Coupon code used',
    estimated_profit DECIMAL(10,2) COMMENT 'Estimated profit',
    revenue_bucket STRING COMMENT 'Revenue bucket: low, medium, high, premium',
    is_high_value BOOLEAN COMMENT 'High value transaction flag (>100K)',
    is_bulk_order BOOLEAN COMMENT 'Bulk order flag (quantity >50)',
    is_high_discount BOOLEAN COMMENT 'High discount flag (>50%)',
    hour INT COMMENT 'Hour of day',
    day_of_week INT COMMENT 'Day of week',
    is_weekend BOOLEAN COMMENT 'Weekend flag'
)
COMMENT 'Transactions Fact Table'
PARTITIONED BY (
    year INT COMMENT 'Year',
    month INT COMMENT 'Month',
    day INT COMMENT 'Day'
)
CLUSTERED BY (user_id) INTO 32 BUCKETS
STORED AS ORC
LOCATION '/ecommerce/warehouse/fact_transactions/'
TBLPROPERTIES (
    'orc.compress'='ZLIB',
    'orc.stripe.size'='268435456',
    'orc.create.index'='true',
    'transactional'='false'
);

-- ============================================================================
-- STEP 6: AGGREGATE TABLE - DAILY SALES SUMMARY
-- ============================================================================

/*
 * agg_daily_sales: Daily sales aggregation
 * Pre-aggregated table for faster dashboard queries
 */

CREATE TABLE IF NOT EXISTS agg_daily_sales (
    sale_date DATE COMMENT 'Sale date',
    total_transactions INT COMMENT 'Total transactions',
    total_revenue DECIMAL(15,2) COMMENT 'Total revenue',
    total_quantity INT COMMENT 'Total quantity sold',
    avg_order_value DECIMAL(10,2) COMMENT 'Average order value',
    unique_customers INT COMMENT 'Unique customers',
    completed_orders INT COMMENT 'Completed orders',
    cancelled_orders INT COMMENT 'Cancelled orders',
    refunded_orders INT COMMENT 'Refunded orders',
    new_customers INT COMMENT 'New customers',
    returning_customers INT COMMENT 'Returning customers',
    mobile_orders INT COMMENT 'Mobile orders',
    desktop_orders INT COMMENT 'Desktop orders',
    top_category STRING COMMENT 'Top selling category',
    top_product STRING COMMENT 'Top selling product'
)
COMMENT 'Daily Sales Summary Aggregate Table'
PARTITIONED BY (year INT, month INT)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY',
    'transactional'='false'
);

-- ============================================================================
-- STEP 7: AGGREGATE TABLE - USER BEHAVIOR METRICS
-- ============================================================================

/*
 * agg_user_metrics: User behavior aggregation
 * One record per user with comprehensive metrics
 */

CREATE TABLE IF NOT EXISTS agg_user_metrics (
    user_id STRING COMMENT 'User identifier',
    total_sessions INT COMMENT 'Total sessions',
    total_events INT COMMENT 'Total events',
    total_page_views INT COMMENT 'Total page views',
    total_cart_adds INT COMMENT 'Total add to cart events',
    total_purchases INT COMMENT 'Total purchases',
    conversion_rate DECIMAL(5,2) COMMENT 'Conversion rate percentage',
    avg_session_duration INT COMMENT 'Average session duration in seconds',
    total_revenue DECIMAL(15,2) COMMENT 'Total revenue',
    avg_order_value DECIMAL(10,2) COMMENT 'Average order value',
    last_activity_date DATE COMMENT 'Last activity date',
    days_since_last_activity INT COMMENT 'Days since last activity',
    is_churned BOOLEAN COMMENT 'Churn flag (>90 days inactive)',
    preferred_device STRING COMMENT 'Most used device type',
    preferred_category STRING COMMENT 'Most browsed/purchased category',
    last_updated TIMESTAMP COMMENT 'Last metrics update timestamp'
)
COMMENT 'User Behavior Metrics Aggregate Table'
CLUSTERED BY (user_id) INTO 16 BUCKETS
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY',
    'transactional'='true',
    'transactional_properties'='default'
);

-- ============================================================================
-- STEP 8: CREATE VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View: Active users with recent activity
CREATE VIEW IF NOT EXISTS vw_active_users AS
SELECT 
    u.user_id,
    u.full_name,
    u.email,
    u.customer_segment,
    u.lifetime_value,
    u.total_purchases,
    u.last_login_at,
    DATEDIFF(CURRENT_DATE, DATE(u.last_login_at)) AS days_since_last_login
FROM dim_users u
WHERE u.is_current = TRUE
  AND u.account_status = 'active'
  AND DATEDIFF(CURRENT_DATE, DATE(u.last_login_at)) <= 30;

-- View: Products in stock with good ratings
CREATE VIEW IF NOT EXISTS vw_featured_products AS
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.discounted_price,
    p.avg_rating,
    p.num_ratings,
    p.stock_quantity
FROM dim_products p
WHERE p.is_current = TRUE
  AND p.in_stock = TRUE
  AND p.avg_rating >= 4.0
  AND p.num_ratings >= 50
ORDER BY p.avg_rating DESC, p.num_ratings DESC;

-- View: Recent transactions with user and product details
CREATE VIEW IF NOT EXISTS vw_recent_transactions AS
SELECT 
    t.transaction_id,
    t.transaction_timestamp,
    u.full_name AS customer_name,
    u.customer_segment,
    p.product_name,
    p.category,
    p.brand,
    t.quantity,
    t.total_amount,
    t.payment_method,
    t.status
FROM fact_transactions t
JOIN dim_users u ON t.user_id = u.user_id AND u.is_current = TRUE
JOIN dim_products p ON t.product_id = p.product_id AND p.is_current = TRUE
WHERE t.year = YEAR(CURRENT_DATE)
  AND t.month = MONTH(CURRENT_DATE)
ORDER BY t.transaction_timestamp DESC;

-- ============================================================================
-- STEP 9: MATERIALIZED VIEW FOR PERFORMANCE
-- ============================================================================

-- Materialized view for daily revenue by category
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_revenue_by_category
AS
SELECT 
    p.category,
    DATE(t.transaction_timestamp) AS sale_date,
    COUNT(DISTINCT t.transaction_id) AS total_orders,
    SUM(t.total_amount) AS total_revenue,
    AVG(t.total_amount) AS avg_order_value,
    COUNT(DISTINCT t.user_id) AS unique_customers
FROM fact_transactions t
JOIN dim_products p ON t.product_id = p.product_id AND p.is_current = TRUE
WHERE t.status = 'completed'
GROUP BY p.category, DATE(t.transaction_timestamp);

-- ============================================================================
-- SCRIPT COMPLETION
-- ============================================================================

-- Verify table creation
SHOW TABLES;

-- Display table schemas
DESCRIBE FORMATTED dim_users;
DESCRIBE FORMATTED dim_products;
DESCRIBE FORMATTED fact_clickstream;
DESCRIBE FORMATTED fact_transactions;

/*
 * ============================================================================
 * EXECUTION NOTES:
 * ============================================================================
 * 
 * 1. To execute this script:
 *    hive -f hive_ddl_create_tables.sql
 * 
 * 2. Or run interactively in Hive CLI:
 *    hive> source hive_ddl_create_tables.sql;
 * 
 * 3. Verify tables:
 *    hive> USE ecommerce_dwh;
 *    hive> SHOW TABLES;
 *    hive> DESCRIBE FORMATTED fact_transactions;
 * 
 * 4. Check table locations in HDFS:
 *    hadoop fs -ls /ecommerce/warehouse/
 * 
 * ============================================================================
 */
