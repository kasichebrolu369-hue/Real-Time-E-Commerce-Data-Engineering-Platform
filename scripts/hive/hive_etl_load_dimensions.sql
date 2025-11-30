-- ============================================================================
-- HIVE ETL - LOAD DIMENSION TABLES
-- ============================================================================
-- 
-- PURPOSE:
-- Load dimension tables from clean zone data
-- 
-- TABLES:
-- - dim_users
-- - dim_products
--
-- ============================================================================

USE ecommerce_dwh;

-- ============================================================================
-- ETL 1: LOAD dim_users
-- ============================================================================

/*
 * Load user data from clean zone
 * Implements SCD Type 2 logic
 */

-- Step 1: Create staging table for incoming user data
DROP TABLE IF EXISTS stg_users;

CREATE TEMPORARY TABLE stg_users AS
SELECT 
    user_id,
    first_name,
    last_name,
    full_name,
    gender,
    CAST(date_of_birth AS DATE) AS date_of_birth,
    age,
    email,
    phone,
    address,
    city,
    state,
    country,
    zipcode,
    CAST(account_created_at AS TIMESTAMP) AS account_created_at,
    CAST(last_login_at AS TIMESTAMP) AS last_login_at,
    account_status,
    customer_segment,
    total_purchases,
    lifetime_value,
    average_order_value,
    preferred_categories,
    CAST(email_subscribed AS BOOLEAN) AS email_subscribed,
    CAST(sms_subscribed AS BOOLEAN) AS sms_subscribed,
    preferred_language,
    preferred_currency,
    CAST(is_premium_member AS BOOLEAN) AS is_premium_member,
    referral_code,
    CAST(kyc_verified AS BOOLEAN) AS kyc_verified
FROM EXTERNAL TABLE (
    user_id STRING,
    first_name STRING,
    last_name STRING,
    full_name STRING,
    gender STRING,
    date_of_birth STRING,
    age INT,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    zipcode STRING,
    account_created_at STRING,
    last_login_at STRING,
    account_status STRING,
    customer_segment STRING,
    total_purchases INT,
    lifetime_value DECIMAL(12,2),
    average_order_value DECIMAL(10,2),
    preferred_categories STRING,
    email_subscribed STRING,
    sms_subscribed STRING,
    preferred_language STRING,
    preferred_currency STRING,
    is_premium_member STRING,
    referral_code STRING,
    kyc_verified STRING
)
STORED AS PARQUET
LOCATION '/ecommerce/raw/users/';

-- Step 2: Expire old records (SCD Type 2)
-- Mark existing current records as expired if data has changed
UPDATE dim_users du
SET 
    expiration_date = CURRENT_DATE,
    is_current = FALSE
WHERE du.is_current = TRUE
  AND EXISTS (
      SELECT 1 
      FROM stg_users su
      WHERE su.user_id = du.user_id
        AND (
            su.account_status != du.account_status OR
            su.customer_segment != du.customer_segment OR
            su.lifetime_value != du.lifetime_value OR
            su.total_purchases != du.total_purchases
        )
  );

-- Step 3: Insert new records
INSERT INTO TABLE dim_users
SELECT 
    user_id,
    first_name,
    last_name,
    full_name,
    gender,
    date_of_birth,
    age,
    email,
    phone,
    address,
    city,
    state,
    country,
    zipcode,
    account_created_at,
    last_login_at,
    account_status,
    customer_segment,
    total_purchases,
    lifetime_value,
    average_order_value,
    preferred_categories,
    email_subscribed,
    sms_subscribed,
    preferred_language,
    preferred_currency,
    is_premium_member,
    referral_code,
    kyc_verified,
    CURRENT_DATE AS effective_date,
    CAST('9999-12-31' AS DATE) AS expiration_date,
    TRUE AS is_current
FROM stg_users su
WHERE NOT EXISTS (
    SELECT 1 
    FROM dim_users du
    WHERE du.user_id = su.user_id 
      AND du.is_current = TRUE
)
OR EXISTS (
    SELECT 1 
    FROM dim_users du
    WHERE du.user_id = su.user_id 
      AND du.is_current = FALSE
      AND (
          su.account_status != du.account_status OR
          su.customer_segment != du.customer_segment OR
          su.lifetime_value != du.lifetime_value OR
          su.total_purchases != du.total_purchases
      )
);

-- Verify load
SELECT COUNT(*) AS total_users, COUNT(DISTINCT user_id) AS unique_users 
FROM dim_users 
WHERE is_current = TRUE;

-- ============================================================================
-- ETL 2: LOAD dim_products
-- ============================================================================

/*
 * Load product catalog from clean zone
 * Implements SCD Type 2 logic with partitioning by category
 */

-- Step 1: Create staging table for incoming product data
DROP TABLE IF EXISTS stg_products;

CREATE TEMPORARY TABLE stg_products AS
SELECT 
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    cost_price,
    selling_price,
    discount_percent,
    discounted_price,
    currency,
    stock_quantity,
    CAST(in_stock AS BOOLEAN) AS in_stock,
    CAST(low_stock_alert AS BOOLEAN) AS low_stock_alert,
    avg_rating,
    num_ratings,
    num_reviews,
    weight_kg,
    dimensions,
    condition,
    warranty_months,
    manufacturer,
    country_of_origin,
    CAST(product_added_date AS DATE) AS product_added_date,
    CAST(last_updated_date AS DATE) AS last_updated_date,
    CAST(is_featured AS BOOLEAN) AS is_featured,
    CAST(is_bestseller AS BOOLEAN) AS is_bestseller,
    CAST(free_shipping AS BOOLEAN) AS free_shipping,
    CAST(returnable AS BOOLEAN) AS returnable,
    return_window_days,
    tags,
    sku,
    barcode,
    supplier_id,
    reorder_level,
    max_order_quantity
FROM EXTERNAL TABLE (
    product_id STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    brand STRING,
    cost_price DECIMAL(10,2),
    selling_price DECIMAL(10,2),
    discount_percent INT,
    discounted_price DECIMAL(10,2),
    currency STRING,
    stock_quantity INT,
    in_stock STRING,
    low_stock_alert STRING,
    avg_rating DECIMAL(3,2),
    num_ratings INT,
    num_reviews INT,
    weight_kg DECIMAL(8,2),
    dimensions STRING,
    condition STRING,
    warranty_months INT,
    manufacturer STRING,
    country_of_origin STRING,
    product_added_date STRING,
    last_updated_date STRING,
    is_featured STRING,
    is_bestseller STRING,
    free_shipping STRING,
    returnable STRING,
    return_window_days INT,
    tags STRING,
    sku STRING,
    barcode STRING,
    supplier_id STRING,
    reorder_level INT,
    max_order_quantity INT
)
STORED AS PARQUET
LOCATION '/ecommerce/raw/products/';

-- Step 2: Expire old product records (SCD Type 2)
UPDATE dim_products dp
SET 
    expiration_date = CURRENT_DATE,
    is_current = FALSE
WHERE dp.is_current = TRUE
  AND EXISTS (
      SELECT 1 
      FROM stg_products sp
      WHERE sp.product_id = dp.product_id
        AND (
            sp.discounted_price != dp.discounted_price OR
            sp.stock_quantity != dp.stock_quantity OR
            sp.in_stock != dp.in_stock OR
            sp.avg_rating != dp.avg_rating
        )
  );

-- Step 3: Insert new product records with dynamic partitioning
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 1000;
SET hive.exec.max.dynamic.partitions.pernode = 100;

INSERT INTO TABLE dim_products PARTITION(category_partition)
SELECT 
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    cost_price,
    selling_price,
    discount_percent,
    discounted_price,
    currency,
    stock_quantity,
    in_stock,
    low_stock_alert,
    avg_rating,
    num_ratings,
    num_reviews,
    weight_kg,
    dimensions,
    condition,
    warranty_months,
    manufacturer,
    country_of_origin,
    product_added_date,
    last_updated_date,
    is_featured,
    is_bestseller,
    free_shipping,
    returnable,
    return_window_days,
    tags,
    sku,
    barcode,
    supplier_id,
    reorder_level,
    max_order_quantity,
    CURRENT_DATE AS effective_date,
    CAST('9999-12-31' AS DATE) AS expiration_date,
    TRUE AS is_current,
    category AS category_partition  -- Partition column
FROM stg_products sp
WHERE NOT EXISTS (
    SELECT 1 
    FROM dim_products dp
    WHERE dp.product_id = sp.product_id 
      AND dp.is_current = TRUE
)
OR EXISTS (
    SELECT 1 
    FROM dim_products dp
    WHERE dp.product_id = sp.product_id 
      AND dp.is_current = FALSE
      AND (
          sp.discounted_price != dp.discounted_price OR
          sp.stock_quantity != dp.stock_quantity OR
          sp.in_stock != dp.in_stock OR
          sp.avg_rating != dp.avg_rating
      )
);

-- Verify load
SELECT 
    category_partition,
    COUNT(*) AS total_products,
    COUNT(DISTINCT product_id) AS unique_products
FROM dim_products 
WHERE is_current = TRUE
GROUP BY category_partition
ORDER BY total_products DESC;

-- Show partitions
SHOW PARTITIONS dim_products;

/*
 * ============================================================================
 * EXECUTION NOTES:
 * ============================================================================
 * 
 * 1. Run this script after DDL creation:
 *    hive -f hive_etl_load_dimensions.sql
 * 
 * 2. Schedule for daily execution via Airflow
 * 
 * 3. Monitor execution:
 *    - Check record counts
 *    - Verify partition creation
 *    - Review SCD Type 2 logic
 * 
 * ============================================================================
 */
