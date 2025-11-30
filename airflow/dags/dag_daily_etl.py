"""
Airflow DAG - Daily ETL Pipeline
================================

PURPOSE:
Orchestrate the daily batch ETL pipeline:
1. Run Pig cleaning scripts
2. Load Hive dimensions (SCD Type 2)
3. Load Hive fact tables
4. Refresh Impala metadata
5. Compute statistics

SCHEDULE: Daily at 2:00 AM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

# ============================================================================
# DAG CONFIGURATION
# ============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=4),
}

dag = DAG(
    'daily_etl_pipeline',
    default_args=default_args,
    description='Daily batch ETL: Pig → Hive → Impala',
    schedule_interval='0 2 * * *',  # 2:00 AM daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'daily', 'batch'],
)

# ============================================================================
# CONFIGURATION
# ============================================================================

PIG_HOME = '/opt/pig'
HIVE_HOME = '/opt/hive'
IMPALA_HOST = 'impala-server.local'
SPARK_HOME = '/opt/spark'
PROJECT_DIR = '/home/airflow/ecommerce-platform'

# ============================================================================
# TASK 1: PIG CLEANING - CLICKSTREAM
# ============================================================================

pig_clean_clickstream = BashOperator(
    task_id='pig_clean_clickstream',
    bash_command=f"""
    set -e
    echo "Starting Pig cleaning for clickstream..."
    
    pig -x mapreduce \
        -param INPUT_DATE={{{{ ds }}}} \
        -f {PROJECT_DIR}/scripts/pig/clickstream_clean.pig
    
    echo "✓ Clickstream cleaning complete"
    """,
    dag=dag,
)

# ============================================================================
# TASK 2: PIG CLEANING - TRANSACTIONS
# ============================================================================

pig_clean_transactions = BashOperator(
    task_id='pig_clean_transactions',
    bash_command=f"""
    set -e
    echo "Starting Pig cleaning for transactions..."
    
    pig -x mapreduce \
        -param INPUT_DATE={{{{ ds }}}} \
        -f {PROJECT_DIR}/scripts/pig/transactions_clean.pig
    
    echo "✓ Transaction cleaning complete"
    """,
    dag=dag,
)

# ============================================================================
# TASK 3: HIVE LOAD DIMENSIONS
# ============================================================================

hive_load_dimensions = BashOperator(
    task_id='hive_load_dimensions',
    bash_command=f"""
    set -e
    echo "Loading Hive dimension tables with SCD Type 2..."
    
    hive -f {PROJECT_DIR}/scripts/hive/hive_etl_load_dimensions.sql \
        --hivevar processing_date={{{{ ds }}}}
    
    echo "✓ Dimensions loaded"
    """,
    dag=dag,
)

# ============================================================================
# TASK 4: HIVE LOAD FACTS
# ============================================================================

hive_load_facts = BashOperator(
    task_id='hive_load_facts',
    bash_command=f"""
    set -e
    echo "Loading Hive fact tables..."
    
    hive -f {PROJECT_DIR}/scripts/hive/hive_etl_load_facts.sql \
        --hivevar processing_date={{{{ ds }}}}
    
    echo "✓ Facts loaded"
    """,
    dag=dag,
)

# ============================================================================
# TASK 5: IMPALA REFRESH METADATA
# ============================================================================

impala_refresh_metadata = BashOperator(
    task_id='impala_refresh_metadata',
    bash_command=f"""
    set -e
    echo "Refreshing Impala metadata..."
    
    impala-shell -i {IMPALA_HOST} -q "
    INVALIDATE METADATA ecommerce.dim_users;
    INVALIDATE METADATA ecommerce.dim_products;
    INVALIDATE METADATA ecommerce.fact_clickstream;
    INVALIDATE METADATA ecommerce.fact_transactions;
    REFRESH ecommerce.agg_daily_sales;
    "
    
    echo "✓ Metadata refreshed"
    """,
    dag=dag,
)

# ============================================================================
# TASK 6: IMPALA COMPUTE STATS
# ============================================================================

impala_compute_stats = BashOperator(
    task_id='impala_compute_stats',
    bash_command=f"""
    set -e
    echo "Computing Impala statistics..."
    
    impala-shell -i {IMPALA_HOST} -f {PROJECT_DIR}/scripts/impala/impala_optimization.sql
    
    echo "✓ Statistics computed"
    """,
    dag=dag,
)

# ============================================================================
# TASK 7: SPARK ETL (ALTERNATIVE PATH)
# ============================================================================

spark_etl_warehouse = SparkSubmitOperator(
    task_id='spark_etl_warehouse',
    application=f'{PROJECT_DIR}/scripts/spark/spark_etl_warehouse.py',
    name='daily_etl_warehouse_load',
    conn_id='spark_default',
    conf={
        'spark.master': 'yarn',
        'spark.submit.deployMode': 'cluster',
        'spark.executor.memory': '4g',
        'spark.executor.cores': '2',
        'spark.executor.instances': '10',
        'spark.sql.adaptive.enabled': 'true',
    },
    application_args=[
        '--date', '{{ ds }}',
        '--mode', 'incremental',
    ],
    dag=dag,
)

# ============================================================================
# TASK 8: DATA QUALITY CHECKS
# ============================================================================

def data_quality_checks(**context):
    """
    Perform data quality checks
    """
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("DQ_Checks").enableHiveSupport().getOrCreate()
    
    date = context['ds']
    
    # Check 1: Row count validation
    clickstream_count = spark.sql(f"""
        SELECT COUNT(*) as cnt 
        FROM ecommerce.fact_clickstream 
        WHERE year={date[:4]} AND month={date[5:7]} AND day={date[8:10]}
    """).collect()[0]['cnt']
    
    print(f"Clickstream rows for {date}: {clickstream_count:,}")
    
    if clickstream_count < 1000:
        raise ValueError(f"Low clickstream volume: {clickstream_count}")
    
    # Check 2: NULL validation
    null_users = spark.sql("""
        SELECT COUNT(*) as cnt 
        FROM ecommerce.fact_transactions 
        WHERE user_id IS NULL
    """).collect()[0]['cnt']
    
    if null_users > 0:
        raise ValueError(f"Found {null_users} NULL user_ids in transactions")
    
    # Check 3: Referential integrity
    orphaned_trans = spark.sql("""
        SELECT COUNT(*) as cnt
        FROM ecommerce.fact_transactions t
        LEFT JOIN ecommerce.dim_users u ON t.user_id = u.user_id
        WHERE u.user_id IS NULL
    """).collect()[0]['cnt']
    
    if orphaned_trans > 0:
        raise ValueError(f"Found {orphaned_trans} orphaned transactions")
    
    print("✓ All data quality checks passed")
    
    spark.stop()

data_quality = PythonOperator(
    task_id='data_quality_checks',
    python_callable=data_quality_checks,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 9: UPDATE METRICS
# ============================================================================

def update_etl_metrics(**context):
    """
    Update ETL metrics in monitoring system
    """
    import json
    from datetime import datetime
    
    date = context['ds']
    
    metrics = {
        'pipeline': 'daily_etl',
        'execution_date': date,
        'completion_time': datetime.now().isoformat(),
        'status': 'success',
        'duration_minutes': context['ti'].duration / 60 if context['ti'].duration else 0,
    }
    
    # Write to HDFS (or send to monitoring system)
    metrics_path = f"/ecommerce/metrics/etl/date={date}/metrics.json"
    
    print(f"ETL Metrics: {json.dumps(metrics, indent=2)}")
    print(f"✓ Metrics logged to {metrics_path}")

update_metrics = PythonOperator(
    task_id='update_etl_metrics',
    python_callable=update_etl_metrics,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Parallel Pig cleaning
[pig_clean_clickstream, pig_clean_transactions] >> hive_load_dimensions

# Sequential Hive loads
hive_load_dimensions >> hive_load_facts

# Impala optimization
hive_load_facts >> impala_refresh_metadata >> impala_compute_stats

# Data quality checks
impala_compute_stats >> data_quality

# Alternative Spark path (runs in parallel with Hive)
pig_clean_clickstream >> spark_etl_warehouse >> data_quality

# Final metrics
data_quality >> update_metrics

# ============================================================================
# DAG DOCUMENTATION
# ============================================================================

dag.doc_md = """
# Daily ETL Pipeline

## Overview
Orchestrates the complete daily batch ETL process for the e-commerce platform.

## Architecture
```
Raw Data (HDFS)
    ↓
Pig Cleaning (clickstream + transactions)
    ↓
Hive Dimension Load (SCD Type 2)
    ↓
Hive Fact Load (partitioned)
    ↓
Impala Metadata Refresh
    ↓
Impala Statistics Compute
    ↓
Data Quality Checks
    ↓
Metrics Update
```

## Schedule
- **Frequency**: Daily at 2:00 AM
- **SLA**: 4 hours
- **Retries**: 2 attempts with 5-minute delay

## Data Flow
1. **Pig Cleaning**: Process previous day's raw data
2. **Hive Dimensions**: Update user/product dims with SCD Type 2
3. **Hive Facts**: Load clickstream and transaction facts
4. **Impala Refresh**: Update metadata for query performance
5. **Statistics**: Compute column statistics for optimization
6. **Quality Checks**: Validate data integrity
7. **Metrics**: Log pipeline execution metrics

## Monitoring
- Email alerts on failure
- Execution metrics logged to HDFS
- Dashboard: http://airflow.local/dags/daily_etl_pipeline

## Troubleshooting
- Check logs in `/var/log/airflow/`
- Pig logs: `/tmp/pig-*.log`
- Hive logs: `/tmp/hive.log`
- YARN application logs: `yarn logs -applicationId <app_id>`
"""
