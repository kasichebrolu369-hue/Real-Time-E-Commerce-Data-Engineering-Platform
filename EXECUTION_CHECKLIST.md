# ✅ Execution Checklist

## Complete Project Execution Guide

Follow this checklist to execute the entire project from start to finish.

---

## Phase 1: Environment Setup

### Prerequisites Installation
- [ ] Install Python 3.8+
- [ ] Install Apache Hadoop 3.x
- [ ] Install Apache Hive 3.x
- [ ] Install Apache Impala 4.x
- [ ] Install Apache Spark 3.5+
- [ ] Install Apache Kafka 3.x
- [ ] Install Apache Airflow 2.8+
- [ ] Install Java 8 or 11

### Python Dependencies
```bash
cd "Real-Time E-Commerce Data Engineering Platform"
pip install -r requirements.txt
```
- [ ] All Python packages installed successfully
- [ ] Test: `python -c "import pandas, faker, pyspark"`

---

## Phase 2: Data Generation (5 minutes)

### Generate All Datasets
```bash
cd data_generators
python run_all_generators.py
```

**Verify Output:**
- [ ] `data/raw/users/users.csv` created (100K records)
- [ ] `data/raw/products/products.csv` created (10K records)
- [ ] `data/raw/transactions/` contains Parquet files (500K records)
- [ ] `data/raw/clickstream/` contains JSON files (1M+ records)
- [ ] `data/samples/` contains sample files

**Check Data Quality:**
```bash
# Count lines
wc -l data/raw/users/users.csv
wc -l data/raw/products/products.csv

# View samples
head data/samples/*.csv
```
- [ ] Data files are non-empty
- [ ] Sample data looks realistic

---

## Phase 3: Hadoop HDFS Setup (3 minutes)

### Start HDFS
```bash
# Format namenode (first time only)
hdfs namenode -format

# Start HDFS services
start-dfs.sh

# Verify
jps | grep NameNode
jps | grep DataNode
```
- [ ] NameNode is running
- [ ] DataNode is running

### Create Directory Structure
```bash
hdfs dfs -mkdir -p /ecommerce/raw/clickstream
hdfs dfs -mkdir -p /ecommerce/raw/transactions
hdfs dfs -mkdir -p /ecommerce/raw/users
hdfs dfs -mkdir -p /ecommerce/raw/products
hdfs dfs -mkdir -p /ecommerce/clean
hdfs dfs -mkdir -p /ecommerce/warehouse
hdfs dfs -mkdir -p /ecommerce/analytics
hdfs dfs -mkdir -p /ecommerce/models
hdfs dfs -mkdir -p /ecommerce/checkpoints
```
- [ ] All directories created

### Upload Data to HDFS
```bash
hdfs dfs -put data/raw/users/* /ecommerce/raw/users/
hdfs dfs -put data/raw/products/* /ecommerce/raw/products/
hdfs dfs -put data/raw/transactions/* /ecommerce/raw/transactions/
hdfs dfs -put data/raw/clickstream/* /ecommerce/raw/clickstream/
```
- [ ] Users uploaded
- [ ] Products uploaded
- [ ] Transactions uploaded
- [ ] Clickstream uploaded

**Verify:**
```bash
hdfs dfs -ls -R /ecommerce/raw/
hdfs dfs -du -h /ecommerce/raw/
```
- [ ] All data visible in HDFS
- [ ] Total size is reasonable (2GB+)

---

## Phase 4: Pig ETL - Data Cleaning (5 minutes)

### Clean Clickstream Data
```bash
cd scripts/pig

pig -x mapreduce clickstream_clean.pig
```
- [ ] Job completes without errors
- [ ] Check logs for quality statistics

### Clean Transaction Data
```bash
pig -x mapreduce transactions_clean.pig
```
- [ ] Job completes without errors
- [ ] Check quality report

**Verify Output:**
```bash
hdfs dfs -ls /ecommerce/clean/clickstream/
hdfs dfs -ls /ecommerce/clean/transactions/
```
- [ ] Clean clickstream data exists
- [ ] Clean transaction data exists
- [ ] Parquet files created

---

## Phase 5: Hive Data Warehouse (10 minutes)

### Start Hive Services
```bash
# Initialize metastore (first time only)
schematool -dbType derby -initSchema

# Start HiveServer2
hive --service hiveserver2 &

# Wait for startup (30 seconds)
sleep 30
```
- [ ] HiveServer2 started
- [ ] Check: `jps | grep HiveServer2`

### Create Tables
```bash
cd scripts/hive

hive -f hive_ddl_create_tables.sql
```
- [ ] Database `ecommerce` created
- [ ] All tables created

**Verify:**
```bash
hive -e "SHOW TABLES IN ecommerce;"
```
Expected output:
- [ ] dim_users
- [ ] dim_products
- [ ] fact_clickstream
- [ ] fact_transactions
- [ ] agg_daily_sales
- [ ] agg_user_metrics
- [ ] mv_daily_revenue_by_category

### Load Dimensions (SCD Type 2)
```bash
hive -f hive_etl_load_dimensions.sql
```
- [ ] dim_users loaded
- [ ] dim_products loaded

**Verify:**
```bash
hive -e "SELECT COUNT(*) FROM ecommerce.dim_users;"
hive -e "SELECT COUNT(*) FROM ecommerce.dim_products;"
```
- [ ] User count matches input (~100K)
- [ ] Product count matches input (~10K)

### Load Fact Tables
```bash
hive -f hive_etl_load_facts.sql
```
- [ ] fact_clickstream loaded
- [ ] fact_transactions loaded

**Verify:**
```bash
hive -e "SELECT COUNT(*) FROM ecommerce.fact_clickstream;"
hive -e "SELECT COUNT(*) FROM ecommerce.fact_transactions;"
```
- [ ] Clickstream count matches input (~1M)
- [ ] Transaction count matches input (~500K)

---

## Phase 6: Impala Optimization (5 minutes)

### Start Impala Services
```bash
# Start Impala daemon
impalad &

# Start state store
statestored &

# Start catalog service
catalogd &

# Wait for startup
sleep 30
```
- [ ] Impala daemon running
- [ ] State store running
- [ ] Catalog service running

### Run Optimization
```bash
cd scripts/impala

impala-shell -f impala_optimization.sql
```
- [ ] Metadata refreshed
- [ ] Statistics computed

### Run BI Queries
```bash
impala-shell -f impala_bi_queries.sql
```
- [ ] Dashboard 1: Revenue query runs
- [ ] Dashboard 2: Product performance query runs
- [ ] Dashboard 3: Customer segmentation runs
- [ ] Dashboard 4: Conversion funnel runs
- [ ] Dashboard 5: Abandoned cart runs

---

## Phase 7: Spark ETL (10 minutes)

### Run Warehouse ETL
```bash
cd scripts/spark

spark-submit \
    --master local[*] \
    --executor-memory 4g \
    spark_etl_warehouse.py \
    --date 2025-11-30
```
- [ ] Job completes successfully
- [ ] Quality checks pass
- [ ] Statistics printed

**Verify Output:**
```bash
hdfs dfs -ls /ecommerce/warehouse/
```
- [ ] Warehouse tables created
- [ ] Data partitioned correctly

---

## Phase 8: Machine Learning (20 minutes)

### Train Recommendation Engine
```bash
cd scripts/spark

spark-submit \
    --master local[*] \
    --executor-memory 8g \
    spark_ml_recommendations.py \
    --train --evaluate --recommend
```
- [ ] Data prepared
- [ ] Model trained
- [ ] RMSE < 1.0
- [ ] MAE < 0.8
- [ ] Recommendations generated
- [ ] Model saved

**Verify:**
```bash
hdfs dfs -ls /ecommerce/models/recommendations/
hdfs dfs -ls /ecommerce/analytics/ml/recommendations/
```
- [ ] Model files exist
- [ ] Recommendation files exist

### Train Churn Prediction Model
```bash
spark-submit \
    --master local[*] \
    --executor-memory 8g \
    spark_ml_churn_prediction.py \
    --train --evaluate --predict
```
- [ ] Features engineered
- [ ] Model trained
- [ ] AUC-ROC > 0.75
- [ ] Accuracy > 0.80
- [ ] Predictions generated
- [ ] Model saved

**Verify:**
```bash
hdfs dfs -ls /ecommerce/models/churn/
hdfs dfs -ls /ecommerce/analytics/ml/churn_predictions/
```
- [ ] Model files exist
- [ ] Prediction files exist (partitioned by risk_level)

---

## Phase 9: Kafka Streaming (Optional, 15 minutes)

### Start Kafka Services
```bash
# Start Zookeeper
zookeeper-server-start.sh config/zookeeper.properties &

# Wait 10 seconds
sleep 10

# Start Kafka broker
kafka-server-start.sh config/server.properties &

# Wait 20 seconds
sleep 20
```
- [ ] Zookeeper running
- [ ] Kafka broker running

### Create Topic
```bash
kafka-topics.sh --create \
    --topic clickstream-events \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1
```
- [ ] Topic created

### Start Producer (Terminal 1)
```bash
cd scripts/kafka
python kafka_producer.py --rate 50
```
- [ ] Producer starts without errors
- [ ] Events being generated
- [ ] See output: "Produced 100 events..."

### Start Consumer (Terminal 2)
```bash
cd scripts/kafka
python kafka_consumer.py
```
- [ ] Consumer connects
- [ ] Events being consumed
- [ ] Files being written

### Start Spark Streaming (Terminal 3)
```bash
cd scripts/spark

spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    spark_streaming_kafka.py
```
- [ ] Streaming job starts
- [ ] Reading from Kafka
- [ ] Writing to HDFS
- [ ] Metrics displayed

**Verify:**
```bash
hdfs dfs -ls /ecommerce/raw/clickstream/streaming/
```
- [ ] Streaming data appears in HDFS

---

## Phase 10: Airflow Orchestration (10 minutes)

### Setup Airflow
```bash
cd airflow

# Option 1: Automated setup
chmod +x setup_airflow.sh
sudo ./setup_airflow.sh
```

OR

```bash
# Option 2: Manual setup
export AIRFLOW_HOME=~/airflow

airflow db init

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@company.com \
    --password admin123

# Copy DAGs
mkdir -p $AIRFLOW_HOME/dags
cp dags/*.py $AIRFLOW_HOME/dags/
```
- [ ] Database initialized
- [ ] Admin user created
- [ ] DAGs copied

### Start Airflow Services
```bash
# Terminal 1: Webserver
airflow webserver --port 8080 &

# Terminal 2: Scheduler
airflow scheduler &

# Wait 30 seconds for startup
sleep 30
```
- [ ] Webserver started
- [ ] Scheduler started

### Access Airflow UI
```
Open browser: http://localhost:8080
Username: admin
Password: admin123
```
- [ ] Login successful
- [ ] DAGs visible:
  - [ ] daily_etl_pipeline
  - [ ] hourly_data_refresh
  - [ ] weekly_ml_training
  - [ ] realtime_streaming_pipeline

### Test DAG Execution
```bash
# Trigger daily ETL
airflow dags trigger daily_etl_pipeline
```
- [ ] DAG triggered
- [ ] Check execution in UI
- [ ] All tasks green (success)

---

## Phase 11: Verification & Testing

### Data Verification
```bash
# Check Hive tables
hive -e "
SELECT 'fact_clickstream' as table_name, COUNT(*) as row_count FROM ecommerce.fact_clickstream
UNION ALL
SELECT 'fact_transactions', COUNT(*) FROM ecommerce.fact_transactions
UNION ALL
SELECT 'dim_users', COUNT(*) FROM ecommerce.dim_users
UNION ALL
SELECT 'dim_products', COUNT(*) FROM ecommerce.dim_products;
"
```
- [ ] Row counts are correct
- [ ] No NULL primary keys

### ML Model Verification
```bash
hdfs dfs -ls /ecommerce/models/
```
- [ ] Recommendation model exists
- [ ] Churn model exists

### Airflow Verification
```bash
airflow dags list
```
- [ ] All 4 DAGs listed
- [ ] No import errors

---

## Phase 12: Performance Testing

### Query Performance
```bash
cd scripts/impala

# Time a BI query
time impala-shell -q "
SELECT DATE_TRUNC('hour', transaction_ts) as hour, 
       SUM(total_amount) as revenue
FROM ecommerce.fact_transactions
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY 1
ORDER BY 1 DESC;
"
```
- [ ] Query completes < 5 seconds
- [ ] Results are accurate

### Spark Job Performance
```bash
# Monitor Spark UI
# Open: http://localhost:4040 while job is running
```
- [ ] Check execution plan
- [ ] Verify broadcast joins
- [ ] Check shuffle operations

---

## Final Checklist

### All Components Working
- [ ] Data generation complete
- [ ] HDFS operational with data
- [ ] Pig cleaning successful
- [ ] Hive warehouse populated
- [ ] Impala queries running
- [ ] Spark ETL working
- [ ] ML models trained and deployed
- [ ] Kafka streaming operational (if enabled)
- [ ] Airflow orchestration running

### Access Points
- [ ] Hadoop UI: http://localhost:9870
- [ ] Spark UI: http://localhost:4040
- [ ] YARN UI: http://localhost:8088
- [ ] Airflow UI: http://localhost:8080

### Documentation
- [ ] README.md reviewed
- [ ] PROJECT_SUMMARY.md reviewed
- [ ] QUICK_START.md reviewed
- [ ] All scripts have docstrings

---

## 🎉 Success Criteria

You have successfully completed the project when:

✅ All data generated (1.6M+ records)
✅ All processing pipelines execute successfully
✅ All ML models trained and evaluated
✅ All Airflow DAGs operational
✅ No critical errors in logs
✅ Data quality checks pass
✅ All services accessible

---

## 🐛 Troubleshooting

If any step fails, refer to:
- Component logs (check error messages)
- `QUICK_START.md` troubleshooting section
- `README.md` detailed instructions
- YARN logs: `yarn logs -applicationId <app_id>`
- Spark logs: Check UI at http://localhost:4040
- Airflow logs: Check UI task logs

---

## 📞 Next Steps After Completion

1. **Explore the data**: Run custom Impala queries
2. **Customize**: Modify generators, add features
3. **Experiment**: Try different ML algorithms
4. **Monitor**: Watch Airflow DAG executions
5. **Optimize**: Tune Spark and Hive configurations
6. **Extend**: Add more dashboards, models, pipelines

---

**Congratulations on completing the Real-Time E-Commerce Data Engineering Platform! 🚀**
