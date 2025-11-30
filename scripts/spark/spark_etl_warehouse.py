"""
Spark ETL Pipeline - Clean Zone to Warehouse
=============================================

PURPOSE:
Load cleaned data from HDFS clean zone and join dimension/fact tables
for analytics-ready warehouse layer.

OPERATIONS:
1. Load clean clickstream and transaction data
2. Join with dimension tables (users, products)
3. Apply business logic and transformations
4. Write to warehouse in optimized formats (Parquet/ORC)
5. Partition and optimize for query performance

USAGE:
spark-submit --master yarn \\
    --deploy-mode cluster \\
    --num-executors 10 \\
    --executor-memory 4G \\
    --executor-cores 2 \\
    spark_etl_warehouse.py \\
    --date 2025-11-30
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import argparse
from datetime import datetime, timedelta
import sys

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration class for ETL pipeline"""
    
    # HDFS Paths
    CLEAN_CLICKSTREAM_PATH = "/ecommerce/clean/clickstream/data/"
    CLEAN_TRANSACTIONS_PATH = "/ecommerce/clean/transactions/data/"
    RAW_USERS_PATH = "/ecommerce/raw/users/"
    RAW_PRODUCTS_PATH = "/ecommerce/raw/products/"
    
    WAREHOUSE_CLICKSTREAM_PATH = "/ecommerce/warehouse/fact_clickstream/"
    WAREHOUSE_TRANSACTIONS_PATH = "/ecommerce/warehouse/fact_transactions/"
    WAREHOUSE_USERS_PATH = "/ecommerce/warehouse/dim_users/"
    WAREHOUSE_PRODUCTS_PATH = "/ecommerce/warehouse/dim_products/"
    
    # Spark Configuration
    APP_NAME = "EcommerceETL-WarehouseLoad"
    SHUFFLE_PARTITIONS = 200
    
    # Checkpoint directory for fault tolerance
    CHECKPOINT_DIR = "/ecommerce/checkpoints/etl/"


# ============================================================================
# SPARK SESSION INITIALIZATION
# ============================================================================

def create_spark_session():
    """
    Create and configure Spark session with optimizations
    """
    spark = SparkSession.builder \
        .appName(Config.APP_NAME) \
        .config("spark.sql.shuffle.partitions", Config.SHUFFLE_PARTITIONS) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.orc.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.hive.convertMetastoreParquet", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✓ Spark Session Created: {Config.APP_NAME}")
    print(f"  Spark Version: {spark.version}")
    print(f"  Master: {spark.sparkContext.master}")
    
    return spark


# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

def load_clickstream_data(spark, date_filter=None):
    """
    Load cleaned clickstream data from HDFS
    
    Args:
        spark: SparkSession
        date_filter: Optional date string (YYYY-MM-DD) for filtering
    
    Returns:
        DataFrame with clickstream events
    """
    print("\n" + "="*80)
    print("LOADING CLICKSTREAM DATA")
    print("="*80)
    
    df = spark.read.parquet(Config.CLEAN_CLICKSTREAM_PATH)
    
    if date_filter:
        df = df.filter(col("event_date") == date_filter)
        print(f"  Filtered to date: {date_filter}")
    
    count = df.count()
    print(f"  Records loaded: {count:,}")
    
    return df


def load_transaction_data(spark, date_filter=None):
    """
    Load cleaned transaction data from HDFS
    
    Args:
        spark: SparkSession
        date_filter: Optional date string (YYYY-MM-DD) for filtering
    
    Returns:
        DataFrame with transactions
    """
    print("\n" + "="*80)
    print("LOADING TRANSACTION DATA")
    print("="*80)
    
    df = spark.read.parquet(Config.CLEAN_TRANSACTIONS_PATH)
    
    if date_filter:
        df = df.filter(col("transaction_date") == date_filter)
        print(f"  Filtered to date: {date_filter}")
    
    count = df.count()
    print(f"  Records loaded: {count:,}")
    
    return df


def load_dimension_users(spark):
    """
    Load user dimension data
    
    Returns:
        DataFrame with user profiles
    """
    print("\n" + "="*80)
    print("LOADING USER DIMENSION")
    print("="*80)
    
    # Read from Parquet (could also read from Hive table)
    df = spark.read.parquet(Config.RAW_USERS_PATH)
    
    # Add SCD Type 2 fields
    df = df.withColumn("effective_date", current_date()) \
           .withColumn("expiration_date", lit("9999-12-31").cast("date")) \
           .withColumn("is_current", lit(True))
    
    count = df.count()
    print(f"  Users loaded: {count:,}")
    
    return df


def load_dimension_products(spark):
    """
    Load product dimension data
    
    Returns:
        DataFrame with product catalog
    """
    print("\n" + "="*80)
    print("LOADING PRODUCT DIMENSION")
    print("="*80)
    
    df = spark.read.parquet(Config.RAW_PRODUCTS_PATH)
    
    # Add SCD Type 2 fields
    df = df.withColumn("effective_date", current_date()) \
           .withColumn("expiration_date", lit("9999-12-31").cast("date")) \
           .withColumn("is_current", lit(True))
    
    count = df.count()
    print(f"  Products loaded: {count:,}")
    
    return df


# ============================================================================
# TRANSFORMATION FUNCTIONS
# ============================================================================

def enrich_clickstream_data(spark, clickstream_df, users_df, products_df):
    """
    Enrich clickstream with user and product dimensions
    
    Args:
        spark: SparkSession
        clickstream_df: Clickstream fact data
        users_df: User dimension data
        products_df: Product dimension data
    
    Returns:
        Enriched clickstream DataFrame
    """
    print("\n" + "="*80)
    print("ENRICHING CLICKSTREAM DATA")
    print("="*80)
    
    # Join with users (broadcast join for dimension)
    enriched_df = clickstream_df.join(
        broadcast(users_df.select("user_id", "customer_segment", "country as user_country")),
        on="user_id",
        how="left"
    )
    
    # Join with products (broadcast join)
    enriched_df = enriched_df.join(
        broadcast(products_df.select("product_id", "category", "brand", "discounted_price")),
        on="product_id",
        how="left"
    )
    
    # Add derived columns
    enriched_df = enriched_df.withColumn(
        "session_duration",
        when(col("event_type") == "exit", 
             unix_timestamp(col("event_timestamp")) - lag(unix_timestamp(col("event_timestamp")))
             .over(Window.partitionBy("session_id").orderBy("event_timestamp")))
        .otherwise(None)
    )
    
    # Add time-based features
    enriched_df = enriched_df \
        .withColumn("day_of_week", dayofweek(col("event_timestamp"))) \
        .withColumn("is_weekend", when(dayofweek(col("event_timestamp")).isin([1, 7]), True).otherwise(False)) \
        .withColumn("hour_of_day", hour(col("event_timestamp")))
    
    print(f"  ✓ Clickstream enrichment complete")
    
    return enriched_df


def enrich_transaction_data(spark, transaction_df, users_df, products_df):
    """
    Enrich transactions with user and product dimensions
    
    Args:
        spark: SparkSession
        transaction_df: Transaction fact data
        users_df: User dimension data
        products_df: Product dimension data
    
    Returns:
        Enriched transaction DataFrame
    """
    print("\n" + "="*80)
    print("ENRICHING TRANSACTION DATA")
    print("="*80)
    
    # Join with users
    enriched_df = transaction_df.join(
        broadcast(users_df.select("user_id", "customer_segment", "country as user_country", 
                                  "lifetime_value", "total_purchases")),
        on="user_id",
        how="left"
    )
    
    # Join with products
    enriched_df = enriched_df.join(
        broadcast(products_df.select("product_id", "category", "brand", "avg_rating", 
                                    "manufacturer", "warranty_months")),
        on="product_id",
        how="left"
    )
    
    # Calculate additional metrics
    enriched_df = enriched_df \
        .withColumn("gross_profit", col("total_amount") - (col("unit_price") * col("quantity") * 0.7)) \
        .withColumn("profit_margin", (col("gross_profit") / col("total_amount")) * 100) \
        .withColumn("is_high_value", when(col("total_amount") > 100000, True).otherwise(False)) \
        .withColumn("is_bulk_order", when(col("quantity") > 50, True).otherwise(False))
    
    # Time-based features
    enriched_df = enriched_df \
        .withColumn("day_of_week", dayofweek(col("transaction_ts"))) \
        .withColumn("is_weekend", when(dayofweek(col("transaction_ts")).isin([1, 7]), True).otherwise(False)) \
        .withColumn("hour_of_day", hour(col("transaction_ts")))
    
    # Customer lifetime value bucket
    enriched_df = enriched_df.withColumn(
        "ltv_bucket",
        when(col("lifetime_value") >= 100000, "platinum")
        .when(col("lifetime_value") >= 50000, "gold")
        .when(col("lifetime_value") >= 10000, "silver")
        .otherwise("bronze")
    )
    
    print(f"  ✓ Transaction enrichment complete")
    
    return enriched_df


# ============================================================================
# SAVE FUNCTIONS
# ============================================================================

def save_to_warehouse(df, output_path, partition_cols, file_format="parquet"):
    """
    Save DataFrame to warehouse with partitioning
    
    Args:
        df: DataFrame to save
        output_path: HDFS output path
        partition_cols: List of partition column names
        file_format: File format (parquet, orc, csv)
    """
    print(f"\n  Saving to: {output_path}")
    print(f"  Format: {file_format}")
    print(f"  Partitions: {partition_cols}")
    
    df.write \
        .mode("append") \
        .format(file_format) \
        .partitionBy(*partition_cols) \
        .option("compression", "snappy") \
        .save(output_path)
    
    print(f"  ✓ Data saved successfully")


def save_clickstream_to_warehouse(spark, enriched_clickstream_df):
    """
    Save enriched clickstream to warehouse
    """
    print("\n" + "="*80)
    print("SAVING CLICKSTREAM TO WAREHOUSE")
    print("="*80)
    
    # Select final columns
    final_df = enriched_clickstream_df.select(
        "event_id",
        "user_id",
        "product_id",
        "session_id",
        "event_timestamp",
        "page_url",
        "event_type",
        "device_type",
        "ip_address",
        "traffic_source",
        "referrer",
        "user_agent",
        "country",
        "city",
        "browser",
        "os",
        "hour_of_day",
        "day_of_week",
        "is_weekend",
        "session_duration",
        "customer_segment",
        "category",
        "brand",
        "year",
        "month",
        "day"
    )
    
    save_to_warehouse(
        df=final_df,
        output_path=Config.WAREHOUSE_CLICKSTREAM_PATH,
        partition_cols=["year", "month", "day"],
        file_format="parquet"
    )


def save_transactions_to_warehouse(spark, enriched_transaction_df):
    """
    Save enriched transactions to warehouse
    """
    print("\n" + "="*80)
    print("SAVING TRANSACTIONS TO WAREHOUSE")
    print("="*80)
    
    # Select final columns
    final_df = enriched_transaction_df.select(
        "transaction_id",
        "user_id",
        "product_id",
        "transaction_ts",
        "quantity",
        "unit_price",
        "subtotal",
        "discount_percent",
        "discount_amount",
        "tax_amount",
        "shipping_fee",
        "total_amount",
        "payment_method",
        "status",
        "delivery_pincode",
        "delivery_city",
        "delivery_state",
        "delivery_country",
        "expected_delivery_ts",
        "actual_delivery_ts",
        "delivery_time_days",
        "is_late_delivery",
        "customer_email",
        "customer_phone",
        "order_source",
        "is_first_purchase",
        "coupon_code",
        "estimated_profit",
        "revenue_bucket",
        "is_high_value",
        "is_bulk_order",
        "is_high_discount",
        "hour_of_day",
        "day_of_week",
        "is_weekend",
        "customer_segment",
        "category",
        "brand",
        "gross_profit",
        "profit_margin",
        "ltv_bucket",
        "year",
        "month",
        "day"
    )
    
    save_to_warehouse(
        df=final_df,
        output_path=Config.WAREHOUSE_TRANSACTIONS_PATH,
        partition_cols=["year", "month", "day"],
        file_format="orc"
    )


def save_dimensions_to_warehouse(spark, users_df, products_df):
    """
    Save dimension tables to warehouse
    """
    print("\n" + "="*80)
    print("SAVING DIMENSIONS TO WAREHOUSE")
    print("="*80)
    
    # Save users dimension
    users_df.write \
        .mode("overwrite") \
        .format("orc") \
        .option("compression", "snappy") \
        .save(Config.WAREHOUSE_USERS_PATH)
    
    print(f"  ✓ Users dimension saved: {Config.WAREHOUSE_USERS_PATH}")
    
    # Save products dimension (partitioned by category)
    products_df.write \
        .mode("overwrite") \
        .format("orc") \
        .partitionBy("category") \
        .option("compression", "snappy") \
        .save(Config.WAREHOUSE_PRODUCTS_PATH)
    
    print(f"  ✓ Products dimension saved: {Config.WAREHOUSE_PRODUCTS_PATH}")


# ============================================================================
# DATA QUALITY CHECKS
# ============================================================================

def perform_quality_checks(spark, clickstream_df, transaction_df):
    """
    Perform data quality checks
    """
    print("\n" + "="*80)
    print("DATA QUALITY CHECKS")
    print("="*80)
    
    # Check 1: NULL values in critical fields
    print("\n1. Checking for NULL values...")
    
    clickstream_nulls = clickstream_df.select(
        [count(when(col(c).isNull(), c)).alias(c) for c in ["event_id", "user_id", "event_timestamp"]]
    ).collect()[0]
    
    transaction_nulls = transaction_df.select(
        [count(when(col(c).isNull(), c)).alias(c) for c in ["transaction_id", "user_id", "total_amount"]]
    ).collect()[0]
    
    print(f"   Clickstream NULLs: {dict(clickstream_nulls.asDict())}")
    print(f"   Transaction NULLs: {dict(transaction_nulls.asDict())}")
    
    # Check 2: Duplicate detection
    print("\n2. Checking for duplicates...")
    
    clickstream_total = clickstream_df.count()
    clickstream_unique = clickstream_df.select("event_id").distinct().count()
    print(f"   Clickstream: {clickstream_total:,} total, {clickstream_unique:,} unique events")
    
    transaction_total = transaction_df.count()
    transaction_unique = transaction_df.select("transaction_id").distinct().count()
    print(f"   Transactions: {transaction_total:,} total, {transaction_unique:,} unique transactions")
    
    # Check 3: Value ranges
    print("\n3. Checking value ranges...")
    
    transaction_stats = transaction_df.select(
        min("total_amount").alias("min_amount"),
        max("total_amount").alias("max_amount"),
        avg("total_amount").alias("avg_amount")
    ).collect()[0]
    
    print(f"   Transaction amounts: Min=${transaction_stats['min_amount']:.2f}, "
          f"Max=${transaction_stats['max_amount']:.2f}, Avg=${transaction_stats['avg_amount']:.2f}")
    
    print("\n✓ Quality checks complete")


# ============================================================================
# MAIN ETL PIPELINE
# ============================================================================

def main():
    """
    Main ETL execution function
    """
    # Parse arguments
    parser = argparse.ArgumentParser(description='Spark ETL - Warehouse Load')
    parser.add_argument('--date', type=str, help='Date filter (YYYY-MM-DD)', default=None)
    args = parser.parse_args()
    
    print("\n" + "="*80)
    print("E-COMMERCE SPARK ETL PIPELINE")
    print("="*80)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.date:
        print(f"Date Filter: {args.date}")
    print("="*80)
    
    try:
        # Initialize Spark
        spark = create_spark_session()
        
        # Load data
        clickstream_df = load_clickstream_data(spark, args.date)
        transaction_df = load_transaction_data(spark, args.date)
        users_df = load_dimension_users(spark)
        products_df = load_dimension_products(spark)
        
        # Cache dimension tables for multiple joins
        users_df.cache()
        products_df.cache()
        
        # Perform quality checks
        perform_quality_checks(spark, clickstream_df, transaction_df)
        
        # Transform and enrich
        enriched_clickstream = enrich_clickstream_data(spark, clickstream_df, users_df, products_df)
        enriched_transactions = enrich_transaction_data(spark, transaction_df, users_df, products_df)
        
        # Save to warehouse
        save_dimensions_to_warehouse(spark, users_df, products_df)
        save_clickstream_to_warehouse(spark, enriched_clickstream)
        save_transactions_to_warehouse(spark, enriched_transactions)
        
        print("\n" + "="*80)
        print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("="*80)
        print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
