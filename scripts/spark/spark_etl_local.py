"""
Spark ETL Pipeline - Local Mode (Windows Compatible)
====================================================

PURPOSE:
Process raw data and create analytics-ready datasets using Spark in local mode.
No Hadoop/HDFS required - works with local filesystem.

OPERATIONS:
1. Load raw data (CSV/Parquet) from data/raw/
2. Clean and transform data
3. Join dimension and fact tables
4. Apply business logic and aggregations
5. Write to data/warehouse/ in Parquet format

USAGE:
python spark_etl_local.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import os
import sys

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration for local ETL pipeline"""
    
    # Base directory (project root)
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    
    # Input paths (raw data)
    RAW_USERS_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'users', 'users.csv')
    RAW_PRODUCTS_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'products', 'products.csv')
    RAW_TRANSACTIONS_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'transactions')
    RAW_CLICKSTREAM_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'clickstream')
    
    # Output paths (warehouse)
    WAREHOUSE_DIR = os.path.join(BASE_DIR, 'data', 'warehouse')
    WAREHOUSE_USERS_PATH = os.path.join(WAREHOUSE_DIR, 'dim_users')
    WAREHOUSE_PRODUCTS_PATH = os.path.join(WAREHOUSE_DIR, 'dim_products')
    WAREHOUSE_TRANSACTIONS_PATH = os.path.join(WAREHOUSE_DIR, 'fact_transactions')
    WAREHOUSE_CLICKSTREAM_PATH = os.path.join(WAREHOUSE_DIR, 'fact_clickstream')
    WAREHOUSE_DAILY_SALES_PATH = os.path.join(WAREHOUSE_DIR, 'agg_daily_sales')
    WAREHOUSE_USER_METRICS_PATH = os.path.join(WAREHOUSE_DIR, 'agg_user_metrics')
    
    # Spark Configuration
    APP_NAME = "EcommerceETL-LocalMode"


# ============================================================================
# SPARK SESSION
# ============================================================================

def create_spark_session():
    """Create Spark session for local processing"""
    
    print("="*80)
    print("🚀 SPARK ETL PIPELINE - LOCAL MODE")
    print("="*80)
    print(f"Base Directory: {Config.BASE_DIR}")
    print(f"Warehouse Output: {Config.WAREHOUSE_DIR}")
    print("="*80)
    
    spark = SparkSession.builder \
        .appName(Config.APP_NAME) \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    print(f"✓ Spark Session Created")
    print(f"  Version: {spark.version}")
    print(f"  Master: {spark.sparkContext.master}")
    print(f"  Cores: {spark.sparkContext.defaultParallelism}")
    print()
    
    return spark


# ============================================================================
# DATA LOADING
# ============================================================================

def load_users(spark):
    """Load user dimension data"""
    print("📥 Loading users data...")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(Config.RAW_USERS_PATH)
    
    print(f"   ✓ Loaded {df.count():,} users")
    return df


def load_products(spark):
    """Load product dimension data"""
    print("📥 Loading products data...")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(Config.RAW_PRODUCTS_PATH)
    
    print(f"   ✓ Loaded {df.count():,} products")
    return df


def load_transactions(spark):
    """Load transaction fact data (all CSV files)"""
    print("📥 Loading transactions data...")
    
    # Find all transaction CSV files
    import glob
    csv_files = glob.glob(os.path.join(Config.RAW_TRANSACTIONS_PATH, '**', '*.csv'), recursive=True)
    
    if not csv_files:
        print("   ⚠️  No transaction CSV files found")
        return spark.createDataFrame([], StructType([]))
    
    print(f"   Found {len(csv_files)} transaction batch files")
    
    # Read all CSV files
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(Config.RAW_TRANSACTIONS_PATH + "/**/*.csv")
    
    count = df.count()
    print(f"   ✓ Loaded {count:,} transactions")
    return df


def load_clickstream(spark):
    """Load clickstream fact data (JSON files)"""
    print("📥 Loading clickstream data...")
    
    # Find all clickstream JSON files
    import glob
    json_files = glob.glob(os.path.join(Config.RAW_CLICKSTREAM_PATH, '**', '*.json'), recursive=True)
    
    if not json_files:
        print("   ⚠️  No clickstream JSON files found")
        return spark.createDataFrame([], StructType([]))
    
    print(f"   Found {len(json_files)} clickstream batch files")
    
    # Read all JSON files
    df = spark.read.json(Config.RAW_CLICKSTREAM_PATH + "/**/*.json")
    
    count = df.count()
    print(f"   ✓ Loaded {count:,} clickstream events")
    return df


# ============================================================================
# DATA TRANSFORMATION
# ============================================================================

def transform_users(df_users):
    """Transform and enrich user dimension"""
    print("\n🔄 Transforming users dimension...")
    
    df_transformed = df_users.select(
        col("user_id"),
        col("full_name"),
        col("email"),
        col("phone"),
        col("gender"),
        col("date_of_birth"),
        col("age"),
        col("city"),
        col("state"),
        col("country"),
        col("zipcode"),
        col("account_created_at").cast("timestamp").alias("account_created_at"),
        col("last_login_at").cast("timestamp").alias("last_login_at"),
        col("account_status"),
        col("customer_segment"),
        col("total_purchases"),
        col("lifetime_value"),
        col("average_order_value"),
        col("is_premium_member"),
        col("kyc_verified"),
        current_timestamp().alias("etl_processed_at")
    )
    
    print(f"   ✓ Transformed {df_transformed.count():,} user records")
    return df_transformed


def transform_products(df_products):
    """Transform and enrich product dimension"""
    print("\n🔄 Transforming products dimension...")
    
    df_transformed = df_products.select(
        col("product_id"),
        col("product_name"),
        col("category"),
        col("subcategory"),
        col("brand"),
        col("selling_price"),
        col("cost_price"),
        col("discount_percent"),
        col("discounted_price"),
        col("stock_quantity"),
        col("in_stock"),
        col("avg_rating"),
        col("num_ratings"),
        col("num_reviews"),
        col("is_featured"),
        col("is_bestseller"),
        col("free_shipping"),
        col("returnable"),
        current_timestamp().alias("etl_processed_at")
    )
    
    print(f"   ✓ Transformed {df_transformed.count():,} product records")
    return df_transformed


def transform_transactions(df_transactions, df_users, df_products):
    """Transform and enrich transaction fact table"""
    print("\n🔄 Transforming transactions fact table...")
    
    # Clean and transform
    df_clean = df_transactions \
        .filter(col("status").isNotNull()) \
        .filter(col("total_amount") > 0) \
        .withColumn("transaction_date", to_date(col("transaction_timestamp")))
    
    # Join with dimensions
    df_enriched = df_clean \
        .join(df_users.select("user_id", "customer_segment"), "user_id", "left") \
        .join(df_products.select("product_id", "category", "brand"), "product_id", "left")
    
    # Select final columns
    df_final = df_enriched.select(
        col("transaction_id"),
        col("user_id"),
        col("product_id"),
        col("category"),
        col("brand"),
        col("customer_segment"),
        col("quantity"),
        col("unit_price"),
        col("subtotal"),
        col("discount_amount"),
        col("tax_amount"),
        col("shipping_fee"),
        col("total_amount"),
        col("payment_method"),
        col("status"),
        col("transaction_timestamp").cast("timestamp"),
        col("transaction_date"),
        year(col("transaction_date")).alias("year"),
        month(col("transaction_date")).alias("month"),
        dayofmonth(col("transaction_date")).alias("day"),
        current_timestamp().alias("etl_processed_at")
    )
    
    print(f"   ✓ Transformed {df_final.count():,} transaction records")
    return df_final


def transform_clickstream(df_clickstream, df_users, df_products):
    """Transform and enrich clickstream fact table"""
    print("\n🔄 Transforming clickstream fact table...")
    
    # Clean and transform
    df_clean = df_clickstream \
        .filter(col("event_type").isNotNull()) \
        .withColumn("event_date", to_date(col("event_time")))
    
    # Join with dimensions
    df_enriched = df_clean \
        .join(df_users.select("user_id", "customer_segment"), "user_id", "left") \
        .join(df_products.select("product_id", "category"), "product_id", "left")
    
    # Select final columns
    df_final = df_enriched.select(
        col("event_id"),
        col("user_id"),
        col("session_id"),
        col("product_id"),
        col("category"),
        col("customer_segment"),
        col("event_type"),
        col("device_type"),
        col("browser"),
        col("os"),
        col("country"),
        col("city"),
        col("traffic_source"),
        col("event_time").cast("timestamp"),
        col("event_date"),
        year(col("event_date")).alias("year"),
        month(col("event_date")).alias("month"),
        dayofmonth(col("event_date")).alias("day"),
        current_timestamp().alias("etl_processed_at")
    )
    
    print(f"   ✓ Transformed {df_final.count():,} clickstream events")
    return df_final


# ============================================================================
# AGGREGATIONS
# ============================================================================

def create_daily_sales_aggregate(df_transactions):
    """Create daily sales aggregation"""
    print("\n📊 Creating daily sales aggregates...")
    
    df_agg = df_transactions \
        .groupBy("transaction_date", "category", "customer_segment") \
        .agg(
            count("transaction_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("user_id").alias("unique_customers"),
            sum("quantity").alias("total_items_sold")
        ) \
        .withColumn("etl_processed_at", current_timestamp())
    
    print(f"   ✓ Created {df_agg.count():,} daily aggregate records")
    return df_agg


def create_user_metrics_aggregate(df_transactions, df_clickstream):
    """Create user-level metrics"""
    print("\n📊 Creating user metrics aggregates...")
    
    # Transaction metrics
    txn_metrics = df_transactions \
        .groupBy("user_id") \
        .agg(
            count("transaction_id").alias("total_transactions"),
            sum("total_amount").alias("total_spent"),
            avg("total_amount").alias("avg_transaction_value"),
            max("transaction_timestamp").alias("last_purchase_date")
        )
    
    # Clickstream metrics
    click_metrics = df_clickstream \
        .groupBy("user_id") \
        .agg(
            count("event_id").alias("total_events"),
            countDistinct("session_id").alias("total_sessions"),
            sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("total_add_to_cart")
        )
    
    # Combine
    df_agg = txn_metrics \
        .join(click_metrics, "user_id", "outer") \
        .fillna(0) \
        .withColumn("etl_processed_at", current_timestamp())
    
    print(f"   ✓ Created {df_agg.count():,} user metric records")
    return df_agg


# ============================================================================
# DATA WRITING
# ============================================================================

def write_dimension_table(df, path, name):
    """Write dimension table to warehouse"""
    print(f"\n💾 Writing {name} to warehouse...")
    
    df.write \
        .mode("overwrite") \
        .parquet(path)
    
    print(f"   ✓ Written to: {path}")


def write_fact_table(df, path, name, partition_cols=None):
    """Write fact table to warehouse with partitioning"""
    print(f"\n💾 Writing {name} to warehouse...")
    
    if partition_cols:
        df.write \
            .mode("overwrite") \
            .partitionBy(*partition_cols) \
            .parquet(path)
        print(f"   ✓ Partitioned by: {partition_cols}")
    else:
        df.write \
            .mode("overwrite") \
            .parquet(path)
    
    print(f"   ✓ Written to: {path}")


# ============================================================================
# DATA QUALITY CHECKS
# ============================================================================

def data_quality_checks(spark):
    """Run data quality checks on warehouse"""
    print("\n" + "="*80)
    print("🔍 DATA QUALITY CHECKS")
    print("="*80)
    
    checks = []
    
    # Check 1: Dimension tables exist
    if os.path.exists(Config.WAREHOUSE_USERS_PATH):
        df_users = spark.read.parquet(Config.WAREHOUSE_USERS_PATH)
        user_count = df_users.count()
        checks.append(("dim_users", user_count, "✓"))
        print(f"✓ dim_users: {user_count:,} records")
    else:
        checks.append(("dim_users", 0, "✗"))
        print(f"✗ dim_users: NOT FOUND")
    
    if os.path.exists(Config.WAREHOUSE_PRODUCTS_PATH):
        df_products = spark.read.parquet(Config.WAREHOUSE_PRODUCTS_PATH)
        product_count = df_products.count()
        checks.append(("dim_products", product_count, "✓"))
        print(f"✓ dim_products: {product_count:,} records")
    else:
        checks.append(("dim_products", 0, "✗"))
        print(f"✗ dim_products: NOT FOUND")
    
    # Check 2: Fact tables exist
    if os.path.exists(Config.WAREHOUSE_TRANSACTIONS_PATH):
        df_txn = spark.read.parquet(Config.WAREHOUSE_TRANSACTIONS_PATH)
        txn_count = df_txn.count()
        checks.append(("fact_transactions", txn_count, "✓"))
        print(f"✓ fact_transactions: {txn_count:,} records")
        
        # Revenue check
        total_revenue = df_txn.agg(sum("total_amount")).collect()[0][0]
        print(f"  Total Revenue: ${total_revenue:,.2f}")
    else:
        checks.append(("fact_transactions", 0, "✗"))
        print(f"✗ fact_transactions: NOT FOUND")
    
    if os.path.exists(Config.WAREHOUSE_CLICKSTREAM_PATH):
        df_click = spark.read.parquet(Config.WAREHOUSE_CLICKSTREAM_PATH)
        click_count = df_click.count()
        checks.append(("fact_clickstream", click_count, "✓"))
        print(f"✓ fact_clickstream: {click_count:,} events")
    else:
        checks.append(("fact_clickstream", 0, "✗"))
        print(f"✗ fact_clickstream: NOT FOUND")
    
    # Check 3: Aggregates
    if os.path.exists(Config.WAREHOUSE_DAILY_SALES_PATH):
        df_sales = spark.read.parquet(Config.WAREHOUSE_DAILY_SALES_PATH)
        sales_count = df_sales.count()
        checks.append(("agg_daily_sales", sales_count, "✓"))
        print(f"✓ agg_daily_sales: {sales_count:,} records")
    else:
        checks.append(("agg_daily_sales", 0, "✗"))
        print(f"✗ agg_daily_sales: NOT FOUND")
    
    print("="*80)
    
    return checks


# ============================================================================
# MAIN ETL PIPELINE
# ============================================================================

def main():
    """Main ETL execution"""
    
    start_time = datetime.now()
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Create warehouse directory
        os.makedirs(Config.WAREHOUSE_DIR, exist_ok=True)
        
        print("\n" + "="*80)
        print("STEP 1: LOADING RAW DATA")
        print("="*80)
        
        # Load raw data
        df_users = load_users(spark)
        df_products = load_products(spark)
        df_transactions = load_transactions(spark)
        df_clickstream = load_clickstream(spark)
        
        print("\n" + "="*80)
        print("STEP 2: TRANSFORMING DATA")
        print("="*80)
        
        # Transform dimensions
        df_users_clean = transform_users(df_users)
        df_products_clean = transform_products(df_products)
        
        # Transform facts (with dimension joins)
        df_transactions_clean = transform_transactions(df_transactions, df_users, df_products)
        df_clickstream_clean = transform_clickstream(df_clickstream, df_users, df_products)
        
        # Create aggregates
        df_daily_sales = create_daily_sales_aggregate(df_transactions_clean)
        df_user_metrics = create_user_metrics_aggregate(df_transactions_clean, df_clickstream_clean)
        
        print("\n" + "="*80)
        print("STEP 3: WRITING TO WAREHOUSE")
        print("="*80)
        
        # Write dimensions
        write_dimension_table(df_users_clean, Config.WAREHOUSE_USERS_PATH, "dim_users")
        write_dimension_table(df_products_clean, Config.WAREHOUSE_PRODUCTS_PATH, "dim_products")
        
        # Write facts (partitioned)
        write_fact_table(df_transactions_clean, Config.WAREHOUSE_TRANSACTIONS_PATH, 
                        "fact_transactions", partition_cols=["year", "month"])
        write_fact_table(df_clickstream_clean, Config.WAREHOUSE_CLICKSTREAM_PATH, 
                        "fact_clickstream", partition_cols=["year", "month"])
        
        # Write aggregates
        write_dimension_table(df_daily_sales, Config.WAREHOUSE_DAILY_SALES_PATH, "agg_daily_sales")
        write_dimension_table(df_user_metrics, Config.WAREHOUSE_USER_METRICS_PATH, "agg_user_metrics")
        
        # Data quality checks
        data_quality_checks(spark)
        
        # Success summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*80)
        print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("="*80)
        print(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        print(f"Warehouse Location: {Config.WAREHOUSE_DIR}")
        print("\n🎯 Next Steps:")
        print("  1. Query data: python -c \"from pyspark.sql import SparkSession; ...")
        print("  2. Run ML models: python spark_ml_recommendations.py")
        print("  3. Explore data/warehouse/ directory")
        print("="*80)
        
        spark.stop()
        return 0
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
