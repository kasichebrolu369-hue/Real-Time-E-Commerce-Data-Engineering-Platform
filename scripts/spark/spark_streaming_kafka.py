"""
Spark Structured Streaming - Kafka to HDFS
==========================================

PURPOSE:
Real-time streaming pipeline to consume Kafka events and write to HDFS.

FEATURES:
- Consumes from Kafka topic
- Processes events in micro-batches
- Applies transformations and enrichment
- Writes to HDFS with partitioning
- Checkpointing for fault tolerance

USAGE:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
    spark_streaming_kafka.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Streaming configuration"""
    
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    KAFKA_TOPIC = "clickstream-events"
    
    # Streaming Configuration
    TRIGGER_INTERVAL = "10 seconds"  # Micro-batch interval
    CHECKPOINT_LOCATION = "/ecommerce/checkpoints/streaming/"
    
    # Output Configuration
    OUTPUT_PATH = "/ecommerce/raw/clickstream/streaming/"
    OUTPUT_MODE = "append"
    OUTPUT_FORMAT = "parquet"


# ============================================================================
# SCHEMA DEFINITION
# ============================================================================

def get_clickstream_schema():
    """
    Define schema for clickstream events
    
    Returns:
        StructType schema
    """
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("event_timestamp", LongType(), False),
        StructField("page_url", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("event_type", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("traffic_source", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("country", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("os", StringType(), True)
    ])


# ============================================================================
# SPARK SESSION
# ============================================================================

def create_spark_session():
    """
    Create Spark session with streaming configuration
    
    Returns:
        SparkSession
    """
    spark = SparkSession.builder \
        .appName("KafkaClickstreamStreaming") \
        .config("spark.sql.streaming.checkpointLocation", Config.CHECKPOINT_LOCATION) \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("✓ Spark Streaming Session Created")
    logger.info(f"  Spark Version: {spark.version}")
    
    return spark


# ============================================================================
# STREAMING PIPELINE
# ============================================================================

def read_from_kafka(spark):
    """
    Read streaming data from Kafka
    
    Args:
        spark: SparkSession
    
    Returns:
        Streaming DataFrame
    """
    logger.info("Reading from Kafka...")
    logger.info(f"  Bootstrap Servers: {Config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Topic: {Config.KAFKA_TOPIC}")
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", Config.KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    logger.info("✓ Kafka stream connected")
    
    return df


def parse_events(df):
    """
    Parse JSON events from Kafka
    
    Args:
        df: Raw Kafka DataFrame
    
    Returns:
        Parsed DataFrame
    """
    logger.info("Parsing JSON events...")
    
    # Get schema
    schema = get_clickstream_schema()
    
    # Parse JSON from value column
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition"),
        col("offset")
    ).select("data.*", "kafka_timestamp", "partition", "offset")
    
    logger.info("✓ Events parsed")
    
    return parsed_df


def transform_events(df):
    """
    Apply transformations to events
    
    Args:
        df: Parsed DataFrame
    
    Returns:
        Transformed DataFrame
    """
    logger.info("Applying transformations...")
    
    # Convert timestamp from milliseconds to timestamp
    df = df.withColumn(
        "event_timestamp_dt",
        from_unixtime(col("event_timestamp") / 1000).cast("timestamp")
    )
    
    # Add processing time
    df = df.withColumn("processing_time", current_timestamp())
    
    # Extract date components for partitioning
    df = df.withColumn("year", year(col("event_timestamp_dt"))) \
           .withColumn("month", month(col("event_timestamp_dt"))) \
           .withColumn("day", dayofmonth(col("event_timestamp_dt"))) \
           .withColumn("hour", hour(col("event_timestamp_dt")))
    
    # Add derived fields
    df = df.withColumn("is_mobile", 
                       when(col("device_type") == "mobile", True).otherwise(False))
    
    df = df.withColumn("is_purchase", 
                       when(col("event_type") == "purchase", True).otherwise(False))
    
    # Calculate latency (processing time - event time)
    df = df.withColumn(
        "latency_seconds",
        unix_timestamp(col("processing_time")) - 
        unix_timestamp(col("event_timestamp_dt"))
    )
    
    logger.info("✓ Transformations applied")
    
    return df


def write_to_hdfs(df):
    """
    Write streaming data to HDFS with partitioning
    
    Args:
        df: Transformed DataFrame
    
    Returns:
        StreamingQuery
    """
    logger.info("Writing to HDFS...")
    logger.info(f"  Output Path: {Config.OUTPUT_PATH}")
    logger.info(f"  Output Format: {Config.OUTPUT_FORMAT}")
    logger.info(f"  Trigger Interval: {Config.TRIGGER_INTERVAL}")
    
    query = df.writeStream \
        .format(Config.OUTPUT_FORMAT) \
        .outputMode(Config.OUTPUT_MODE) \
        .option("path", Config.OUTPUT_PATH) \
        .option("checkpointLocation", Config.CHECKPOINT_LOCATION) \
        .partitionBy("year", "month", "day", "hour") \
        .trigger(processingTime=Config.TRIGGER_INTERVAL) \
        .start()
    
    logger.info("✓ Streaming query started")
    
    return query


def write_to_console(df):
    """
    Write streaming data to console (for debugging)
    
    Args:
        df: Transformed DataFrame
    
    Returns:
        StreamingQuery
    """
    query = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 20) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return query


# ============================================================================
# AGGREGATIONS (OPTIONAL)
# ============================================================================

def compute_real_time_metrics(df):
    """
    Compute real-time metrics with windowing
    
    Args:
        df: Event DataFrame
    
    Returns:
        Aggregated DataFrame
    """
    logger.info("Computing real-time metrics...")
    
    # Define watermark for late data
    df_with_watermark = df.withWatermark("event_timestamp_dt", "5 minutes")
    
    # Aggregate by 1-minute windows
    metrics = df_with_watermark.groupBy(
        window(col("event_timestamp_dt"), "1 minute"),
        col("event_type"),
        col("device_type")
    ).agg(
        count("*").alias("event_count"),
        countDistinct("user_id").alias("unique_users"),
        countDistinct("session_id").alias("unique_sessions"),
        avg("latency_seconds").alias("avg_latency")
    )
    
    logger.info("✓ Metrics computed")
    
    return metrics


def write_metrics_to_console(metrics_df):
    """
    Write metrics to console for monitoring
    
    Args:
        metrics_df: Metrics DataFrame
    
    Returns:
        StreamingQuery
    """
    query = metrics_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="1 minute") \
        .start()
    
    return query


# ============================================================================
# MAIN STREAMING APPLICATION
# ============================================================================

def main():
    """Main streaming application"""
    
    logger.info("="*80)
    logger.info("SPARK STRUCTURED STREAMING - KAFKA TO HDFS")
    logger.info("="*80)
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Read from Kafka
        raw_stream = read_from_kafka(spark)
        
        # Parse and transform
        parsed_stream = parse_events(raw_stream)
        transformed_stream = transform_events(parsed_stream)
        
        # Write to HDFS
        hdfs_query = write_to_hdfs(transformed_stream)
        
        # Optional: Compute and display metrics
        metrics_stream = compute_real_time_metrics(transformed_stream)
        metrics_query = write_metrics_to_console(metrics_stream)
        
        # Also write sample to console for monitoring
        console_query = write_to_console(
            transformed_stream.select(
                "event_id", "user_id", "event_type", 
                "device_type", "event_timestamp_dt", "latency_seconds"
            )
        )
        
        logger.info("\n" + "="*80)
        logger.info("STREAMING QUERIES ACTIVE")
        logger.info("="*80)
        logger.info("✓ HDFS Writer: Active")
        logger.info("✓ Metrics Monitor: Active")
        logger.info("✓ Console Monitor: Active")
        logger.info("="*80)
        logger.info("\nPress Ctrl+C to stop...")
        
        # Wait for termination
        hdfs_query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("\n⚠️  Keyboard interrupt received, stopping streams...")
        
    except Exception as e:
        logger.error(f"❌ Error in streaming application: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()
        logger.info("✓ Spark session stopped")


if __name__ == "__main__":
    main()
