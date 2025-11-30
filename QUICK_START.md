# 🚀 Quick Start Guide

## Get Started in 10 Minutes!

This guide will get you up and running with the complete platform.

---

## Prerequisites

### Required Software
- Python 3.8+
- Apache Hadoop 3.x (HDFS)
- Apache Spark 3.5+
- Apache Hive 3.x
- Apache Impala 4.x
- Apache Kafka 3.x
- Apache Airflow 2.8+
- Java 8 or 11

### Check Installations
```bash
python --version          # Should be 3.8+
hadoop version           # Should be 3.x
spark-submit --version   # Should be 3.5+
hive --version          # Should be 3.x
```

---

## Step-by-Step Setup

### 1. Install Python Dependencies (2 minutes)

**Windows (Python 3.13 - Current Environment):**
```powershell
cd "Real-Time E-Commerce Data Engineering Platform"
.\ecom\Scripts\activate

# Install packages compatible with Python 3.13
pip install -r requirements-python313.txt

# Verify installation
python -c "import pandas, faker, pyspark; print('✓ All packages installed')"
```

**Linux/Mac (Python 3.8+):**
```bash
cd "Real-Time E-Commerce Data Engineering Platform"

# Install required packages
pip install -r requirements.txt

# Verify installation
python -c "import pandas, faker, pyspark; print('✓ All packages installed')"
```

**Note for Windows users**: If using Python 3.13, Airflow won't work. Create a separate Python 3.11 environment for Airflow (see Step 10).

---

### 2. Generate Synthetic Data (3 minutes)

```bash
cd data_generators

# Generate all datasets at once
python run_all_generators.py

# This creates:
# - 100K users
# - 10K products  
# - 500K transactions
# - 1M+ clickstream events
```

**Expected Output:**
```
✓ Users generated: data/raw/users/users.csv
✓ Products generated: data/raw/products/products.csv
✓ Transactions generated: data/raw/transactions/YYYY/MM/DD/*.parquet
✓ Clickstream generated: data/raw/clickstream/YYYY/MM/DD/*.json
```

---

### 3. Explore Your Generated Data (2 minutes)

**✅ Works on Windows without any additional setup!**

```powershell
# View user statistics
python -c "import pandas as pd; df = pd.read_csv('data/raw/users/users.csv'); print('Total Users:', len(df)); print('\nSegments:\n', df['customer_segment'].value_counts())"

# View product categories
python -c "import pandas as pd; df = pd.read_csv('data/raw/products/products.csv'); print('Total Products:', len(df)); print('\nTop Categories:\n', df['category'].value_counts().head())"

# Analyze transaction data
python -c "import pandas as pd; import glob; files = glob.glob('data/raw/transactions/**/*.csv', recursive=True); df = pd.read_csv(files[0]); print('Sample Transactions:\n', df[['transaction_id', 'user_id', 'total_amount', 'status']].head())"

# Check data directory structure
ls data\raw\ -Recurse | Select-Object -First 30
```

**Expected Output:**
```
✓ 100,000 users across 5 customer segments
✓ 10,000 products in 10 categories
✓ 500,000 transactions (date-partitioned)
✓ 1,000,000+ clickstream events (JSON format)
```

---

### 3b. Setup Hadoop HDFS (Optional - For Production Scale)

**⚠️ Windows Note:** Hadoop is complex to install on Windows. Skip this step and use Spark in local mode instead (see Step 4). You'll learn the same concepts!

<details>
<summary>Click to expand Hadoop setup instructions (Linux/Mac/Docker)</summary>

```bash
# Format namenode (first time only)
hdfs namenode -format

# Start HDFS
start-dfs.sh

# Create directory structure
hdfs dfs -mkdir -p /ecommerce/raw/clickstream
hdfs dfs -mkdir -p /ecommerce/raw/transactions
hdfs dfs -mkdir -p /ecommerce/raw/users
hdfs dfs -mkdir -p /ecommerce/raw/products
hdfs dfs -mkdir -p /ecommerce/clean
hdfs dfs -mkdir -p /ecommerce/warehouse

# Upload data
hdfs dfs -put data/raw/* /ecommerce/raw/

# Verify
hdfs dfs -ls -R /ecommerce/raw/
```

**Alternative:** Use Docker to run Hadoop:
```bash
docker run -it --name hadoop -p 9870:9870 sequenceiq/hadoop-docker:2.7.1 /etc/bootstrap.sh -bash
```
</details>

---

### 4. Run Spark ETL in Local Mode (3 minutes)

**✅ Works on Windows with just PySpark installed!**

Spark can read/write directly from your local filesystem - no Hadoop needed for learning!

```powershell
cd scripts\spark

# Run warehouse ETL on local data
spark-submit --master local[*] spark_etl_warehouse.py

# This will:
# ✓ Read CSV/Parquet files from data/raw/
# ✓ Clean and transform data
# ✓ Write output to data/warehouse/
# ✓ Show you Spark transformations in action
```

**Expected Output:**
```
---

### 5. Train Machine Learning Models (5 minutes)

**✅ Works on Windows - Most Exciting Part!**

Now that you have data, train ML models to predict customer behavior:

```powershell
cd scripts\spark

# Train recommendation engine (Collaborative Filtering)
spark-submit --master local[*] spark_ml_recommendations.py --train --evaluate

# Train churn prediction model (Random Forest)
spark-submit --master local[*] spark_ml_churn_prediction.py --train --evaluate --predict
```

**What This Does:**
- ✅ Loads your synthetic transaction data
- ✅ Trains ML models using Spark MLlib
- ✅ Evaluates model performance (accuracy, RMSE)
- ✅ Generates predictions
- ✅ Saves models to `models/` directory

**Expected Output:**
```
Recommendation Model:
✓ Training RMSE: 1.23
✓ Test RMSE: 1.45
✓ Model saved to: models/recommendation_model/

Churn Prediction:
✓ Accuracy: 87.3%
✓ F1 Score: 0.85
✓ Model saved to: models/churn_model/
✓ Predictions saved to: data/predictions/
```

---

### 5b. Create Hive Data Warehouse (Optional - Requires Hadoop)

**⚠️ Skip this on Windows** - Hive requires Hadoop. Use Spark SQL instead (similar syntax)!

<details>
<summary>Click to expand Hive instructions (Linux/Mac with Hadoop)</summary>

```bash
cd scripts/hive

# Start HiveServer2 (if not running)
hive --service hiveserver2 &

# Create tables
hive -f hive_ddl_create_tables.sql

# Load dimensions
hive -f hive_etl_load_dimensions.sql

# Load facts
hive -f hive_etl_load_facts.sql

# Verify
hive -e "SHOW TABLES IN ecommerce;"
```

**Expected Output:**
```
dim_users
dim_products
fact_clickstream
fact_transactions
agg_daily_sales
```
</details>

--- -x mapreduce transactions_clean.pig

# Check output
hdfs dfs -ls /ecommerce/clean/
```

**Expected Output:**
```
✓ Clickstream cleaned: /ecommerce/clean/clickstream/
✓ Transactions cleaned: /ecommerce/clean/transactions/
```
</details>

---

### 5. Create Hive Data Warehouse (1 minute)

```bash
cd scripts/hive

# Start HiveServer2 (if not running)
hive --service hiveserver2 &

# Create tables
hive -f hive_ddl_create_tables.sql

# Load dimensions
hive -f hive_etl_load_dimensions.sql

# Load facts
hive -f hive_etl_load_facts.sql

# Verify
hive -e "SHOW TABLES IN ecommerce;"
```

**Expected Output:**
```
dim_users
dim_products
fact_clickstream
fact_transactions
agg_daily_sales
```

---

### 6. Run Impala Queries (Optional)

```bash
cd scripts/impala

# Start Impala (if not running)
impalad &

# Refresh metadata
impala-shell -f impala_optimization.sql

# Run BI queries
impala-shell -f impala_bi_queries.sql
```

---

### 7. Run Spark ETL (2 minutes)

```bash
cd scripts/spark

# Run warehouse ETL
spark-submit \
    --master local[*] \
    spark_etl_warehouse.py \
    --date 2025-11-30

# Check output
hdfs dfs -ls /ecommerce/warehouse/
```

---

### 8. Train ML Models (Optional, 5 minutes)

```bash
# Train recommendation engine
spark-submit \
    --master local[*] \
    spark_ml_recommendations.py \
    --train --evaluate

# Train churn prediction
spark-submit \
    --master local[*] \
    spark_ml_churn_prediction.py \
    --train --evaluate --predict
```

---

### 9. Start Kafka Streaming (Optional)

```bash
cd scripts/kafka

# Terminal 1: Start producer
python kafka_producer.py

# Terminal 2: Start consumer
python kafka_consumer.py

# Terminal 3: Start Spark streaming
cd ../spark
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    spark_streaming_kafka.py
```

---

### 10. Setup Airflow (Optional, 5 minutes)

**Windows (Using Python 3.11):**
```powershell
# Create separate environment for Airflow (Python 3.13 not supported)
py -3.11 -m venv airflow_env
.\airflow_env\Scripts\activate

# Install Airflow
pip install apache-airflow==2.8.0
pip install apache-airflow-providers-apache-spark
pip install apache-airflow-providers-apache-kafka

# Initialize Airflow
$env:AIRFLOW_HOME="$PWD\airflow"
airflow db init

# Create admin user
airflow users create `
    --username admin `
    --firstname Admin `
    --lastname User `
    --role Admin `
    --email admin@company.com `
    --password admin123

# Copy DAGs
Copy-Item -Path "airflow\dags\*" -Destination "$env:AIRFLOW_HOME\dags\" -Recurse

# Start services (2 separate PowerShell windows)
# Window 1: airflow webserver --port 8080
# Window 2: airflow scheduler

# Access at: http://localhost:8080
# Username: admin, Password: admin123
```

**Linux/Mac:**
```bash
cd airflow

# Run setup script
chmod +x setup_airflow.sh
sudo ./setup_airflow.sh

# Or manual setup:
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
cp dags/* $AIRFLOW_HOME/dags/

# Start services
airflow webserver --port 8080 &
airflow scheduler &

# Access at: http://localhost:8080
# Username: admin, Password: admin123
```

---

## 🎯 Quick Commands Cheat Sheet

### Data Generation
```bash
# Generate all data
cd data_generators && python run_all_generators.py
```

### Pig ETL
```bash
# Clean clickstream
pig -x mapreduce scripts/pig/clickstream_clean.pig

# Clean transactions
pig -x mapreduce scripts/pig/transactions_clean.pig
```

### Hive
```bash
# Create tables
hive -f scripts/hive/hive_ddl_create_tables.sql

# Load data
hive -f scripts/hive/hive_etl_load_dimensions.sql
hive -f scripts/hive/hive_etl_load_facts.sql

# Query
hive -e "SELECT COUNT(*) FROM ecommerce.fact_transactions;"
```

### Spark
```bash
# ETL
spark-submit scripts/spark/spark_etl_warehouse.py

# ML
spark-submit scripts/spark/spark_ml_recommendations.py --train --evaluate
spark-submit scripts/spark/spark_ml_churn_prediction.py --train --predict
```

### Kafka
```bash
# Producer
python scripts/kafka/kafka_producer.py

# Consumer
python scripts/kafka/kafka_consumer.py

# Streaming
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    scripts/spark/spark_streaming_kafka.py
```

### Airflow
```bash
# Start services
airflow webserver --port 8080 &
airflow scheduler &

# List DAGs
airflow dags list

# Trigger DAG
airflow dags trigger daily_etl_pipeline

# UI: http://localhost:8080
```

---

## ✅ Verification Checklist

After setup, verify:

- [ ] HDFS is running (`hdfs dfs -ls /`)
- [ ] Data generated in `data/raw/`
- [ ] Data uploaded to HDFS (`hdfs dfs -ls /ecommerce/raw/`)
- [ ] Pig cleaning completed (`hdfs dfs -ls /ecommerce/clean/`)
- [ ] Hive tables created (`hive -e "SHOW TABLES;"`)
- [ ] Spark ETL completed (`hdfs dfs -ls /ecommerce/warehouse/`)
- [ ] Airflow UI accessible (http://localhost:8080)

---

## 🐛 Quick Troubleshooting

### Problem: Python packages not found
```bash
pip install -r requirements.txt --upgrade
```

### Problem: HDFS not accessible
```bash
# Check if running
jps | grep NameNode

# Restart HDFS
stop-dfs.sh
start-dfs.sh
```

### Problem: Hive tables not created
```bash
# Initialize metastore
schematool -dbType derby -initSchema

# Restart HiveServer2
pkill -f HiveServer2
hive --service hiveserver2 &
```

### Problem: Spark job fails
```bash
# Check YARN logs
yarn application -list
yarn logs -applicationId <app_id>

# Use local mode for testing
spark-submit --master local[*] script.py
```

### Problem: Airflow DAGs not visible
```bash
# Check DAGs folder
ls $AIRFLOW_HOME/dags/

# Refresh DAGs
airflow dags list-import-errors
```

---

## 📚 Next Steps

1. **Explore the Data**:
   ```bash
   # View sample data
   head data/samples/*.csv
   ```

2. **Run BI Queries**:
   ```bash
   impala-shell -f scripts/impala/impala_bi_queries.sql
   ```

3. **Train ML Models**:
   ```bash
   spark-submit scripts/spark/spark_ml_recommendations.py --train
   ```

4. **Setup Automation**:
   - Enable Airflow DAGs in UI
   - Set up email alerts
   - Monitor execution

5. **Customize**:
   - Modify data generation parameters
   - Add new features
   - Create custom queries

---

## 🎉 Success!

You now have a complete, production-ready data engineering platform running!

**What you've built:**
- ✅ 1M+ synthetic data records
- ✅ Multi-layer data pipeline (Raw → Clean → Warehouse)
- ✅ Batch processing (Pig, Hive, Spark)
- ✅ Real-time streaming (Kafka, Spark Streaming)
- ✅ Machine learning models (Recommendations, Churn)
- ✅ Workflow automation (Airflow)

**Access Points:**
- Airflow UI: http://localhost:8080
- Hadoop UI: http://localhost:9870
- Spark UI: http://localhost:4040
- YARN UI: http://localhost:8088

---

## 📞 Need Help?

Refer to:
- `README.md` - Complete documentation
- `PROJECT_SUMMARY.md` - Detailed project overview
- Component docstrings - Inline help
- `docs/` - Additional guides

**Happy Data Engineering! 🚀**
