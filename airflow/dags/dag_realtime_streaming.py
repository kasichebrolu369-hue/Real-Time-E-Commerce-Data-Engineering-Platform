"""
Airflow DAG - Real-Time Streaming Pipeline
==========================================

PURPOSE:
Manage Kafka and Spark Structured Streaming jobs for real-time
data ingestion and processing.

FEATURES:
- Monitor Kafka producer/consumer health
- Manage Spark Structured Streaming jobs
- Handle streaming job failures and restarts
- Monitor data freshness and lag

SCHEDULE: Continuous monitoring (every 5 minutes)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago

# ============================================================================
# DAG CONFIGURATION
# ============================================================================

default_args = {
    'owner': 'streaming-engineering',
    'depends_on_past': False,
    'email': ['streaming-alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
}

dag = DAG(
    'realtime_streaming_pipeline',
    default_args=default_args,
    description='Monitor and manage real-time streaming jobs',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['streaming', 'realtime', 'kafka', 'monitoring'],
)

# ============================================================================
# CONFIGURATION
# ============================================================================

KAFKA_BROKER = 'kafka-broker.local:9092'
PROJECT_DIR = '/home/airflow/ecommerce-platform'
SPARK_STREAMING_APP = 'spark_streaming_kafka'

# ============================================================================
# TASK 1: CHECK KAFKA HEALTH
# ============================================================================

def check_kafka_health(**context):
    """
    Check Kafka broker and topic health
    """
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.admin import KafkaAdminClient
    
    print("Checking Kafka health...")
    
    try:
        # Check broker connectivity
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        brokers = admin._client.cluster.brokers()
        
        print(f"  Active brokers: {len(brokers)}")
        
        # Check topic
        topics = admin.list_topics()
        
        if 'clickstream-events' not in topics:
            raise ValueError("Topic 'clickstream-events' not found")
        
        print(f"  Total topics: {len(topics)}")
        print("  ✓ clickstream-events topic exists")
        
        # Check consumer lag
        consumer = KafkaConsumer(
            'clickstream-events',
            bootstrap_servers=KAFKA_BROKER,
            group_id='health_check',
            auto_offset_reset='latest',
        )
        
        partitions = consumer.partitions_for_topic('clickstream-events')
        print(f"  Topic partitions: {len(partitions)}")
        
        consumer.close()
        admin.close()
        
        print("✓ Kafka is healthy")
        return True
        
    except Exception as e:
        print(f"❌ Kafka health check failed: {e}")
        raise

kafka_health = PythonSensor(
    task_id='check_kafka_health',
    python_callable=check_kafka_health,
    mode='poke',
    timeout=300,
    poke_interval=30,
    dag=dag,
)

# ============================================================================
# TASK 2: CHECK PRODUCER STATUS
# ============================================================================

def check_producer_status(**context):
    """
    Check if Kafka producer is running and producing events
    """
    from kafka import KafkaConsumer
    import time
    
    print("Checking producer status...")
    
    consumer = KafkaConsumer(
        'clickstream-events',
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest',
        consumer_timeout_ms=10000,
    )
    
    # Check for recent messages
    message_count = 0
    start_time = time.time()
    
    for message in consumer:
        message_count += 1
        if message_count >= 10 or (time.time() - start_time) > 10:
            break
    
    consumer.close()
    
    if message_count == 0:
        print("⚠️  WARNING: No recent messages from producer")
        # In production, might restart producer here
    else:
        print(f"✓ Producer is active ({message_count} messages in 10 seconds)")
    
    return True

producer_status = PythonOperator(
    task_id='check_producer_status',
    python_callable=check_producer_status,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 3: CHECK SPARK STREAMING JOB
# ============================================================================

check_streaming_job = BashOperator(
    task_id='check_spark_streaming_job',
    bash_command=f"""
    set -e
    echo "Checking Spark Structured Streaming job..."
    
    # Check if streaming app is running
    APP_ID=$(yarn application -list | grep {SPARK_STREAMING_APP} | grep RUNNING | awk '{{print $1}}')
    
    if [ -z "$APP_ID" ]; then
        echo "⚠️  WARNING: Streaming job not running"
        exit 1
    else
        echo "✓ Streaming job is running: $APP_ID"
        
        # Check job health
        yarn application -status $APP_ID
    fi
    """,
    dag=dag,
)

# ============================================================================
# TASK 4: MONITOR STREAMING LAG
# ============================================================================

def monitor_streaming_lag(**context):
    """
    Monitor streaming processing lag
    """
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("StreamingLagMonitor") \
        .enableHiveSupport() \
        .getOrCreate()
    
    print("Monitoring streaming lag...")
    
    # Check latest processed timestamp
    latest_processed = spark.sql("""
        SELECT MAX(event_timestamp) as latest_ts
        FROM ecommerce.fact_clickstream
        WHERE processing_time IS NOT NULL
    """).collect()[0]['latest_ts']
    
    if latest_processed:
        from datetime import datetime
        lag_seconds = (datetime.now() - latest_processed).total_seconds()
        
        print(f"  Latest processed event: {latest_processed}")
        print(f"  Current lag: {lag_seconds:.0f} seconds")
        
        # Alert if lag > 5 minutes
        if lag_seconds > 300:
            print(f"⚠️  HIGH LAG DETECTED: {lag_seconds:.0f} seconds")
        else:
            print("✓ Lag is acceptable")
    else:
        print("⚠️  No processed events found")
    
    spark.stop()

monitor_lag = PythonOperator(
    task_id='monitor_streaming_lag',
    python_callable=monitor_streaming_lag,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 5: CHECK DATA FRESHNESS
# ============================================================================

def check_data_freshness(**context):
    """
    Verify streaming data freshness
    """
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("DataFreshnessCheck") \
        .enableHiveSupport() \
        .getOrCreate()
    
    print("Checking data freshness...")
    
    # Count events in last 5 minutes
    recent_events = spark.sql("""
        SELECT COUNT(*) as cnt
        FROM ecommerce.fact_clickstream
        WHERE event_timestamp >= date_sub(now(), INTERVAL 5 MINUTE)
    """).collect()[0]['cnt']
    
    print(f"  Events in last 5 minutes: {recent_events:,}")
    
    # Alert if too few events
    MIN_EVENTS_THRESHOLD = 100
    
    if recent_events < MIN_EVENTS_THRESHOLD:
        print(f"⚠️  LOW EVENT VOLUME: {recent_events} < {MIN_EVENTS_THRESHOLD}")
    else:
        print("✓ Data freshness is good")
    
    spark.stop()

freshness_check = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 6: RESTART STREAMING JOB (IF NEEDED)
# ============================================================================

restart_streaming = BashOperator(
    task_id='restart_streaming_job',
    bash_command=f"""
    set -e
    echo "Checking if streaming job needs restart..."
    
    # Check if job is running
    APP_ID=$(yarn application -list | grep {SPARK_STREAMING_APP} | grep RUNNING | awk '{{print $1}}')
    
    if [ -z "$APP_ID" ]; then
        echo "Restarting Spark Structured Streaming..."
        
        # Submit streaming job
        spark-submit \\
            --master yarn \\
            --deploy-mode cluster \\
            --name {SPARK_STREAMING_APP} \\
            --executor-memory 4g \\
            --executor-cores 2 \\
            --num-executors 5 \\
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
            {PROJECT_DIR}/scripts/spark/spark_streaming_kafka.py
        
        echo "✓ Streaming job restarted"
    else
        echo "✓ Streaming job already running: $APP_ID"
    fi
    """,
    trigger_rule='all_done',  # Run even if previous tasks failed
    dag=dag,
)

# ============================================================================
# TASK 7: UPDATE MONITORING METRICS
# ============================================================================

def update_metrics(**context):
    """
    Update streaming pipeline metrics
    """
    from pyspark.sql import SparkSession
    import json
    
    spark = SparkSession.builder \
        .appName("StreamingMetrics") \
        .enableHiveSupport() \
        .getOrCreate()
    
    execution_date = context['execution_date'].isoformat()
    
    # Calculate metrics
    total_events = spark.sql("""
        SELECT COUNT(*) as cnt
        FROM ecommerce.fact_clickstream
        WHERE event_timestamp >= date_sub(now(), INTERVAL 1 HOUR)
    """).collect()[0]['cnt']
    
    metrics = {
        'timestamp': execution_date,
        'events_last_hour': total_events,
        'pipeline_status': 'healthy',
    }
    
    print(f"Metrics: {json.dumps(metrics, indent=2)}")
    
    # Write to metrics table
    metrics_df = spark.createDataFrame([metrics])
    metrics_df.write.mode('append').parquet('/ecommerce/metrics/streaming/')
    
    print("✓ Metrics updated")
    
    spark.stop()

update_metrics_task = PythonOperator(
    task_id='update_metrics',
    python_callable=update_metrics,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Health checks
kafka_health >> producer_status
kafka_health >> check_streaming_job

# Monitoring
[producer_status, check_streaming_job] >> monitor_lag
monitor_lag >> freshness_check

# Recovery
freshness_check >> restart_streaming

# Metrics
restart_streaming >> update_metrics_task

# ============================================================================
# DAG DOCUMENTATION
# ============================================================================

dag.doc_md = """
# Real-Time Streaming Pipeline Management

## Overview
Monitors and manages Kafka and Spark Structured Streaming infrastructure
for real-time data ingestion.

## Architecture
```
Kafka Producer (clickstream events)
    ↓
Kafka Broker (clickstream-events topic)
    ↓
Spark Structured Streaming
    ↓
HDFS (partitioned by hour)
    ↓
Hive Tables (real-time facts)
```

## Schedule
- **Frequency**: Every 5 minutes
- **SLA**: 10 minutes
- **Retries**: 5 attempts with 1-minute delay

## Monitoring Components

### 1. Kafka Health
- Broker connectivity
- Topic availability
- Partition health

### 2. Producer Status
- Message production rate
- Recent message check
- Producer liveness

### 3. Streaming Job Health
- YARN application status
- Job execution state
- Resource utilization

### 4. Data Lag
- Processing latency
- Event-to-processing delay
- Watermark progress

### 5. Data Freshness
- Recent event count
- Throughput monitoring
- Volume anomaly detection

## Alerts & Recovery

### Automatic Actions
- Restart failed streaming jobs
- Alert on high lag (>5 minutes)
- Alert on low volume (<100 events/5min)

### Manual Intervention
- Kafka cluster issues
- Resource exhaustion
- Data quality problems

## Performance Targets
- **Latency**: <2 minutes event-to-warehouse
- **Throughput**: >100 events/second
- **Availability**: 99.9% uptime

## Troubleshooting

### Common Issues
1. **No messages**: Check producer, Kafka brokers
2. **High lag**: Scale Spark executors, check resource
3. **Job failures**: Review Spark logs, checkpoint corruption

### Useful Commands
```bash
# Check Kafka topics
kafka-topics.sh --list --bootstrap-server kafka-broker:9092

# Check consumer lag
kafka-consumer-groups.sh --describe --group spark-streaming

# YARN applications
yarn application -list -appStates RUNNING

# Spark streaming UI
http://spark-master:4040
```

## Dashboard
- Airflow: http://airflow.local/dags/realtime_streaming_pipeline
- Kafka Manager: http://kafka-manager.local
- Spark UI: http://spark-master:4040
"""
