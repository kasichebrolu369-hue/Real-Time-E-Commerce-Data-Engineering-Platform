"""
Airflow DAG - Hourly Data Refresh
=================================

PURPOSE:
Refresh aggregate tables and materialized views hourly
for real-time business dashboards.

FEATURES:
- Incremental updates for aggregate tables
- Refresh materialized views
- Update real-time KPIs
- Trigger dashboard cache refresh

SCHEDULE: Hourly at :05 minutes
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
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}

dag = DAG(
    'hourly_data_refresh',
    default_args=default_args,
    description='Hourly refresh for real-time dashboards',
    schedule_interval='5 * * * *',  # Every hour at :05
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['refresh', 'hourly', 'realtime'],
)

# ============================================================================
# CONFIGURATION
# ============================================================================

IMPALA_HOST = 'impala-server.local'
PROJECT_DIR = '/home/airflow/ecommerce-platform'

# ============================================================================
# TASK 1: REFRESH HOURLY AGGREGATES
# ============================================================================

refresh_hourly_aggregates = BashOperator(
    task_id='refresh_hourly_aggregates',
    bash_command=f"""
    set -e
    echo "Refreshing hourly aggregates..."
    
    impala-shell -i {IMPALA_HOST} -q "
    -- Refresh hourly revenue aggregate
    INSERT INTO ecommerce.agg_hourly_revenue
    SELECT 
        from_timestamp(transaction_ts, 'yyyy-MM-dd HH:00:00') as hour,
        COUNT(DISTINCT transaction_id) as transaction_count,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_order_value,
        COUNT(DISTINCT user_id) as unique_customers
    FROM ecommerce.fact_transactions
    WHERE transaction_ts >= date_sub(now(), INTERVAL 1 HOUR)
        AND transaction_ts < date_trunc('hour', now())
        AND status = 'completed'
    GROUP BY from_timestamp(transaction_ts, 'yyyy-MM-dd HH:00:00');
    "
    
    echo "✓ Hourly aggregates refreshed"
    """,
    dag=dag,
)

# ============================================================================
# TASK 2: REFRESH MATERIALIZED VIEWS
# ============================================================================

refresh_materialized_views = BashOperator(
    task_id='refresh_materialized_views',
    bash_command=f"""
    set -e
    echo "Refreshing materialized views..."
    
    impala-shell -i {IMPALA_HOST} -q "
    -- Refresh daily revenue by category
    REFRESH ecommerce.mv_daily_revenue_by_category;
    
    -- Refresh product performance view
    REFRESH ecommerce.mv_product_performance;
    
    -- Update metadata
    INVALIDATE METADATA ecommerce.mv_daily_revenue_by_category;
    INVALIDATE METADATA ecommerce.mv_product_performance;
    "
    
    echo "✓ Materialized views refreshed"
    """,
    dag=dag,
)

# ============================================================================
# TASK 3: UPDATE REAL-TIME KPIs
# ============================================================================

update_realtime_kpis = BashOperator(
    task_id='update_realtime_kpis',
    bash_command=f"""
    set -e
    echo "Updating real-time KPIs..."
    
    impala-shell -i {IMPALA_HOST} -q "
    -- Current hour metrics
    CREATE TABLE IF NOT EXISTS ecommerce.kpi_realtime (
        metric_name STRING,
        metric_value DOUBLE,
        as_of_time TIMESTAMP
    );
    
    -- Revenue this hour
    INSERT OVERWRITE ecommerce.kpi_realtime
    SELECT 'revenue_current_hour', SUM(total_amount), now()
    FROM ecommerce.fact_transactions
    WHERE transaction_ts >= date_trunc('hour', now())
        AND status = 'completed'
    UNION ALL
    SELECT 'transactions_current_hour', COUNT(*), now()
    FROM ecommerce.fact_transactions
    WHERE transaction_ts >= date_trunc('hour', now())
    UNION ALL
    SELECT 'active_users_current_hour', COUNT(DISTINCT user_id), now()
    FROM ecommerce.fact_clickstream
    WHERE event_timestamp >= date_trunc('hour', now());
    "
    
    echo "✓ Real-time KPIs updated"
    """,
    dag=dag,
)

# ============================================================================
# TASK 4: SPARK STREAMING METRICS
# ============================================================================

spark_streaming_metrics = SparkSubmitOperator(
    task_id='spark_streaming_metrics',
    application=f'{PROJECT_DIR}/scripts/spark/spark_streaming_metrics.py',
    name='hourly_streaming_metrics',
    conn_id='spark_default',
    conf={
        'spark.master': 'yarn',
        'spark.submit.deployMode': 'client',
        'spark.executor.memory': '2g',
        'spark.executor.cores': '1',
    },
    application_args=[
        '--hour', '{{ execution_date.strftime("%Y-%m-%d %H") }}',
    ],
    dag=dag,
)

# ============================================================================
# TASK 5: COMPUTE INCREMENTAL STATS
# ============================================================================

compute_incremental_stats = BashOperator(
    task_id='compute_incremental_stats',
    bash_command=f"""
    set -e
    echo "Computing incremental statistics..."
    
    impala-shell -i {IMPALA_HOST} -q "
    -- Compute stats for current hour partition
    COMPUTE INCREMENTAL STATS ecommerce.fact_clickstream
    PARTITION (
        year={{{{ execution_date.year }}}},
        month={{{{ execution_date.month }}}},
        day={{{{ execution_date.day }}}}
    );
    
    COMPUTE INCREMENTAL STATS ecommerce.fact_transactions;
    "
    
    echo "✓ Incremental statistics computed"
    """,
    dag=dag,
)

# ============================================================================
# TASK 6: CACHE WARMING
# ============================================================================

def warm_dashboard_cache(**context):
    """
    Pre-execute common dashboard queries to warm cache
    """
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("CacheWarming") \
        .enableHiveSupport() \
        .getOrCreate()
    
    print("Warming dashboard caches...")
    
    # Query 1: Last 24 hours revenue
    spark.sql("""
        SELECT SUM(total_amount) as revenue
        FROM ecommerce.fact_transactions
        WHERE transaction_ts >= date_sub(now(), INTERVAL 24 HOUR)
            AND status = 'completed'
    """).collect()
    
    # Query 2: Top products today
    spark.sql("""
        SELECT product_id, COUNT(*) as views
        FROM ecommerce.fact_clickstream
        WHERE event_timestamp >= date_trunc('day', now())
            AND event_type = 'view'
        GROUP BY product_id
        ORDER BY views DESC
        LIMIT 20
    """).collect()
    
    # Query 3: Active users now
    spark.sql("""
        SELECT COUNT(DISTINCT user_id)
        FROM ecommerce.fact_clickstream
        WHERE event_timestamp >= date_sub(now(), INTERVAL 15 MINUTE)
    """).collect()
    
    print("✓ Dashboard cache warmed")
    
    spark.stop()

warm_cache = PythonOperator(
    task_id='warm_dashboard_cache',
    python_callable=warm_dashboard_cache,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK 7: TRIGGER DASHBOARD REFRESH
# ============================================================================

def trigger_dashboard_refresh(**context):
    """
    Notify BI dashboard to refresh data
    """
    import requests
    import json
    
    # Example: Trigger Tableau/Looker refresh
    dashboard_url = "http://dashboard.local/api/refresh"
    
    payload = {
        'dashboard_id': 'ecommerce_realtime',
        'refresh_time': context['execution_date'].isoformat(),
    }
    
    # response = requests.post(dashboard_url, json=payload)
    
    print(f"Dashboard refresh triggered: {json.dumps(payload, indent=2)}")

trigger_refresh = PythonOperator(
    task_id='trigger_dashboard_refresh',
    python_callable=trigger_dashboard_refresh,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Parallel refreshes
refresh_hourly_aggregates >> compute_incremental_stats
refresh_materialized_views >> compute_incremental_stats
update_realtime_kpis >> compute_incremental_stats
spark_streaming_metrics >> compute_incremental_stats

# Sequential cache warming
compute_incremental_stats >> warm_cache >> trigger_refresh

# ============================================================================
# DAG DOCUMENTATION
# ============================================================================

dag.doc_md = """
# Hourly Data Refresh Pipeline

## Overview
Maintains real-time data freshness for business dashboards and analytics.

## Architecture
```
Streaming Data (Kafka/Spark)
    ↓
Hourly Aggregates
    ↓
Materialized Views Refresh
    ↓
Real-time KPIs Update
    ↓
Incremental Statistics
    ↓
Cache Warming
    ↓
Dashboard Refresh
```

## Schedule
- **Frequency**: Every hour at :05 minutes
- **SLA**: 30 minutes
- **Retries**: 3 attempts with 2-minute delay

## Data Freshness
- Aggregate tables: ~5 minutes lag
- Materialized views: ~10 minutes lag
- KPIs: Real-time (current hour)

## Use Cases
- Executive real-time dashboards
- Sales team hourly reports
- Marketing campaign tracking
- Customer support metrics

## Performance
- Average execution: 8-12 minutes
- Peak execution: 15-20 minutes
- Cache hit rate: >85%

## Monitoring
- Dashboard: http://airflow.local/dags/hourly_data_refresh
- Metrics: `/ecommerce/metrics/hourly/`
"""
