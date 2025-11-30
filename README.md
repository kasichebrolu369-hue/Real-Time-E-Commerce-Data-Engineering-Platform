# Real-Time E-Commerce Data Engineering Platform

> **A Complete End-to-End Big Data Analytics Pipeline**
> 
> Pig + Hive + Impala + Kafka + Spark + Airflow with Synthetic Data

[![Data Engineering](https://img.shields.io/badge/Data-Engineering-blue)](https://github.com)
[![Big Data](https://img.shields.io/badge/Big-Data-green)](https://github.com)
[![Python](https://img.shields.io/badge/Python-3.8+-yellow)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange)](https://spark.apache.org/)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)
- [Project Structure](#project-structure)
- [Data Pipeline](#data-pipeline)
- [Getting Started](#getting-started)
- [Usage Guide](#usage-guide)
- [Components](#components)
- [Data Dictionary](#data-dictionary)
- [Performance Optimization](#performance-optimization)
- [Monitoring & Governance](#monitoring--governance)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## 🎯 Overview

This project demonstrates a **production-grade, enterprise-level e-commerce data engineering platform** that processes millions of events daily. It showcases the complete data lifecycle from ingestion to insights, utilizing industry-standard big data technologies.

### Key Features

✅ **Synthetic Data Generation** - Realistic 1M+ records across multiple datasets
✅ **Multi-Layer Architecture** - Raw → Clean → Warehouse → Analytics zones
✅ **Batch & Stream Processing** - Pig ETL + Spark + Kafka streaming
✅ **Data Warehousing** - Hive tables with partitioning and bucketing
✅ **Real-Time Analytics** - Impala queries for BI dashboards
✅ **Machine Learning** - Recommendation engine, churn prediction, product ranking
✅ **Workflow Orchestration** - Airflow DAGs for complete automation
✅ **Data Governance** - PII masking, audit logs, lineage tracking

### Use Cases

- **E-commerce Analytics** - Revenue tracking, product performance, customer behavior
- **Real-Time Monitoring** - Live dashboards for business metrics
- **Customer Intelligence** - Segmentation, RFM analysis, churn prediction
- **Product Optimization** - Recommendation systems, inventory management
- **Business Intelligence** - Conversion funnels, abandoned cart analysis

### What You'll Learn

This project is a **complete learning resource** for aspiring data engineers:

1. **Big Data Fundamentals** - HDFS, MapReduce, distributed computing
2. **ETL Pipelines** - Data cleaning, transformation, loading patterns
3. **Data Warehousing** - Star schema, SCD Type 2, partitioning strategies
4. **Stream Processing** - Kafka, Spark Structured Streaming, real-time analytics
5. **Machine Learning at Scale** - Spark MLlib, recommendation engines, churn prediction
6. **Workflow Orchestration** - Airflow DAGs, scheduling, monitoring
7. **Best Practices** - Code organization, documentation, testing, monitoring

---

## 🏗️ Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
├─────────────────────────────────────────────────────────────────────┤
│  Clickstream Events  │  Transactions  │  Users  │  Products         │
└────────────┬─────────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      INGESTION LAYER                                │
├─────────────────────────────────────────────────────────────────────┤
│  • Kafka Producers (Real-time streaming)                            │
│  • Batch File Uploads (CSV, JSON, Parquet)                          │
│  • Spark Structured Streaming                                       │
└────────────┬─────────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    RAW DATA ZONE (HDFS/S3)                          │
├─────────────────────────────────────────────────────────────────────┤
│  /ecommerce/raw/                                                    │
│    ├── clickstream/YYYY/MM/DD/                                      │
│    ├── transactions/YYYY/MM/DD/                                     │
│    ├── users/                                                       │
│    └── products/                                                    │
└────────────┬─────────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA CLEANING (PIG ETL)                          │
├─────────────────────────────────────────────────────────────────────┤
│  • Remove duplicates and invalid records                            │
│  • Normalize timestamps and formats                                 │
│  • Filter bot traffic                                               │
│  • Data validation and quality checks                               │
└────────────┬─────────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   CLEAN DATA ZONE (HDFS/S3)                         │
├─────────────────────────────────────────────────────────────────────┤
│  /ecommerce/clean/                                                  │
│    ├── clickstream/ (Parquet)                                       │
│    └── transactions/ (Parquet)                                      │
└────────────┬─────────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│              DATA WAREHOUSE (HIVE + SPARK ETL)                      │
├─────────────────────────────────────────────────────────────────────┤
│  Dimension Tables (SCD Type 2):                                     │
│    • dim_users                                                      │
│    • dim_products                                                   │
│                                                                     │
│  Fact Tables (Partitioned):                                         │
│    • fact_clickstream (by date)                                     │
│    • fact_transactions (by date)                                    │
│                                                                     │
│  Aggregate Tables:                                                  │
│    • agg_daily_sales                                                │
│    • agg_user_metrics                                               │
└────────────┬─────────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   ANALYTICS & ML LAYER                              │
├─────────────────────────────────────────────────────────────────────┤
│  Query Engine:                                                      │
│    • Impala (Real-time SQL queries)                                 │
│    • Hive (Batch queries)                                           │
│                                                                     │
│  Machine Learning (Spark MLlib):                                    │
│    • Recommendation Engine (ALS)                                    │
│    • Churn Prediction (Logistic Regression)                         │
│    • Product Ranking (ML Pipeline)                                  │
└────────────┬─────────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     PRESENTATION LAYER                              │
├─────────────────────────────────────────────────────────────────────┤
│  • BI Dashboards (Tableau, Power BI, Superset)                      │
│  • Real-time Metrics                                                │
│  • ML Model Predictions                                             │
│  • Operational Reports                                              │
└─────────────────────────────────────────────────────────────────────┘

       ┌──────────────────────────────────────────┐
       │   ORCHESTRATION (Apache Airflow)         │
       │  • Daily ETL Pipelines                   │
       │  • Hourly Data Refreshes                 │
       │  • Weekly ML Model Training              │
       │  • Data Quality Monitoring               │
       └──────────────────────────────────────────┘

       ┌──────────────────────────────────────────┐
       │   GOVERNANCE (Ranger + Atlas)            │
       │  • Access Control Policies               │
       │  • PII Data Masking                      │
       │  • Audit Logs                            │
       │  • Data Lineage Tracking                 │
       └──────────────────────────────────────────┘
```

### Data Flow

```
Raw Data → Pig Cleaning → Clean Zone → Spark ETL → Hive Warehouse → Impala Queries → Dashboards
                                            ↓
                                      Spark ML Models
                                            ↓
                                      Predictions/Recommendations
```

---

## 🛠️ Technologies Used

### Core Big Data Technologies

| Technology | Version | Purpose |
|------------|---------|---------|
| **Apache Hadoop** | 3.x | Distributed storage (HDFS) |
| **Apache Pig** | 0.17+ | ETL data cleaning |
| **Apache Hive** | 3.x | Data warehousing, SQL queries |
| **Apache Impala** | 4.x | Real-time SQL analytics |
| **Apache Spark** | 3.5+ | ETL, ML, batch processing |
| **Apache Kafka** | 3.x | Real-time event streaming |
| **Apache Airflow** | 2.8+ | Workflow orchestration |

### Machine Learning

| Component | Algorithm | Purpose |
|-----------|-----------|---------|
| **Recommendation Engine** | ALS (Alternating Least Squares) | Product recommendations |
| **Churn Prediction** | Logistic Regression | Customer retention |
| **Feature Engineering** | 18+ behavioral features | ML model inputs |

### Programming & Libraries

- **Python 3.8+** - Data generation, scripting
- **PySpark 3.5** - Distributed data processing
- **pandas** - Data manipulation
- **NumPy** - Numerical computing
- **Faker** - Synthetic data generation
- **scikit-learn** - ML preprocessing

### Data Formats

- **Parquet** - Columnar storage (clickstream, clean data)
- **ORC** - Optimized Row Columnar (warehouse tables)
- **JSON** - Raw event data
- **CSV** - Legacy data, exports

### Governance & Security

- **Apache Ranger** - Access control
- **Apache Atlas** - Metadata management, lineage
- **Hive Metastore** - Centralized metadata

---

## 📁 Project Structure

```
Real-Time E-Commerce Data Engineering Platform/
│
├── data/                           # Data storage directories
│   ├── raw/                       # Raw ingested data
│   │   ├── clickstream/           # JSON clickstream events
│   │   ├── transactions/          # CSV/Parquet transactions
│   │   ├── users/                 # User profile data
│   │   └── products/              # Product catalog
│   ├── clean/                     # Cleaned data (post-Pig)
│   │   ├── clickstream/
│   │   └── transactions/
│   ├── warehouse/                 # Hive warehouse tables
│   ├── analytics/                 # Analytics outputs
│   └── samples/                   # Sample datasets for testing
│
├── data_generators/               # Synthetic data generation
│   ├── generate_clickstream.py   # Clickstream event generator
│   ├── generate_transactions.py  # Transaction data generator
│   ├── generate_users.py         # User profile generator
│   ├── generate_products.py      # Product catalog generator
│   └── run_all_generators.py     # Master runner script
│
├── scripts/                       # Processing scripts
│   ├── pig/                      # Pig ETL scripts
│   │   ├── clickstream_clean.pig
│   │   └── transactions_clean.pig
│   │
│   ├── hive/                     # Hive DDL & ETL
│   │   ├── hive_ddl_create_tables.sql
│   │   ├── hive_etl_load_dimensions.sql
│   │   └── hive_etl_load_facts.sql
│   │
│   ├── impala/                   # Impala queries
│   │   ├── impala_optimization.sql
│   │   └── impala_bi_queries.sql
│   │
│   ├── spark/                    # Spark jobs
│   │   ├── spark_etl_warehouse.py
│   │   ├── spark_ml_recommendations.py
│   │   ├── spark_ml_churn_prediction.py
│   │   └── spark_streaming_kafka.py
│   │
│   └── kafka/                    # Kafka components
│       ├── kafka_producer.py
│       ├── kafka_consumer.py
│       └── kafka_config.properties
│
├── airflow/                       # Workflow orchestration
│   ├── dags/                     # DAG definitions
│   │   ├── dag_daily_etl.py          # Daily batch ETL pipeline
│   │   ├── dag_hourly_refresh.py     # Hourly aggregate refresh
│   │   ├── dag_weekly_ml_training.py # Weekly ML model training
│   │   └── dag_realtime_streaming.py # Streaming job management
│   ├── airflow.cfg               # Airflow configuration
│   ├── setup_airflow.sh          # Installation script
│   └── plugins/                  # Custom plugins
│
├── configs/                       # Configuration files
│   ├── hive-site.xml
│   ├── spark-defaults.conf
│   ├── impala-config.ini
│   └── kafka-config.properties
│
├── governance/                    # Security & governance
│   ├── ranger_policies.json
│   ├── atlas_lineage.json
│   ├── pii_masking_rules.sql
│   └── audit_strategy.md
│
├── docs/                          # Documentation
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── setup_guide.md
│   ├── operations_manual.md
│   └── troubleshooting.md
│
├── requirements.txt               # Python dependencies
├── .gitignore
└── README.md                      # This file
```

---

## 🚀 Getting Started

### Prerequisites

#### System Requirements

- **Operating System**: Linux (CentOS/Ubuntu) or macOS
- **RAM**: Minimum 16GB (32GB recommended)
- **Storage**: 100GB+ free space
- **Java**: JDK 8 or 11
- **Python**: 3.8 or higher

#### Software Installation

1. **Hadoop Ecosystem**
   ```bash
   # Install Hadoop
   wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
   tar -xzf hadoop-3.3.6.tar.gz
   
   # Set environment variables
   export HADOOP_HOME=/path/to/hadoop
   export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
   ```

2. **Apache Spark**
   ```bash
   # Install Spark
   wget https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
   tar -xzf spark-3.5.0-bin-hadoop3.tgz
   
   export SPARK_HOME=/path/to/spark
   export PATH=$PATH:$SPARK_HOME/bin
   ```

3. **Apache Hive**
   ```bash
   # Install Hive
   wget https://downloads.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
   tar -xzf apache-hive-3.1.3-bin.tar.gz
   
   export HIVE_HOME=/path/to/hive
   export PATH=$PATH:$HIVE_HOME/bin
   ```

4. **Apache Pig**
   ```bash
   # Install Pig
   wget https://downloads.apache.org/pig/pig-0.17.0/pig-0.17.0.tar.gz
   tar -xzf pig-0.17.0.tar.gz
   
   export PIG_HOME=/path/to/pig
   export PATH=$PATH:$PIG_HOME/bin
   ```

5. **Apache Kafka**
   ```bash
   # Install Kafka
   wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
   tar -xzf kafka_2.13-3.6.0.tgz
   
   # Start Zookeeper
   bin/zookeeper-server-start.sh config/zookeeper.properties
   
   # Start Kafka
   bin/kafka-server-start.sh config/server.properties
   ```

6. **Apache Airflow**
   ```bash
   # Install Airflow
   pip install apache-airflow==2.8.0
   
   # Initialize database
   airflow db init
   
   # Create admin user
   airflow users create \
       --username admin \
       --password admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com
   ```

### Project Setup

1. **Clone the Repository**
   ```bash
   git clone https://github.com/yourusername/ecommerce-data-engineering.git
   cd ecommerce-data-engineering
   ```

2. **Install Python Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure HDFS**
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
   ```

4. **Initialize Hive Metastore**
   ```bash
   # Initialize schema
   schematool -dbType derby -initSchema
   
   # Start HiveServer2
   hive --service hiveserver2
   ```

5. **Start Impala**
   ```bash
   # Start Impala daemon
   impalad
   
   # Start Impala state store
   statestored
   
   # Start Impala catalog
   catalogd
   ```

---

## 📊 Usage Guide

### Step 1: Generate Synthetic Data

Generate realistic synthetic datasets:

```bash
cd data_generators

# Generate all datasets at once
python run_all_generators.py

# Or generate individually
python generate_users.py          # 100K users
python generate_products.py       # 10K products
python generate_transactions.py   # 500K transactions
python generate_clickstream.py    # 1M+ events
```

**Output:**
- `data/raw/users/users.csv` - 100,000 user profiles
- `data/raw/products/products.csv` - 10,000 products
- `data/raw/transactions/YYYY/MM/DD/*.parquet` - 500,000 transactions
- `data/raw/clickstream/YYYY/MM/DD/*.json` - 1,000,000+ events
- `data/samples/*.csv` - Sample datasets for testing

### Step 2: Upload to HDFS

```bash
# Upload raw data to HDFS
hdfs dfs -put data/raw/users/* /ecommerce/raw/users/
hdfs dfs -put data/raw/products/* /ecommerce/raw/products/
hdfs dfs -put data/raw/transactions/* /ecommerce/raw/transactions/
hdfs dfs -put data/raw/clickstream/* /ecommerce/raw/clickstream/

# Verify upload
hdfs dfs -ls -R /ecommerce/raw/
```

### Step 3: Run Pig ETL (Data Cleaning)

Clean raw data using Pig:

```bash
cd scripts/pig

# Clean clickstream data
pig -param input_path=/ecommerce/raw/clickstream/2025/11/30 \
    -param output_path=/ecommerce/clean/clickstream/2025/11/30 \
    clickstream_clean.pig

# Clean transaction data
pig -param input_path=/ecommerce/raw/transactions/2025/11/30 \
    -param output_path=/ecommerce/clean/transactions/2025/11/30 \
    transactions_clean.pig
```

### Step 4: Create Hive Warehouse

```bash
cd scripts/hive

# Create database and tables
hive -f hive_ddl_create_tables.sql

# Load dimension tables
hive -f hive_etl_load_dimensions.sql

# Load fact tables
hive -f hive_etl_load_facts.sql

# Verify tables
hive -e "USE ecommerce_dwh; SHOW TABLES;"
hive -e "SELECT COUNT(*) FROM ecommerce_dwh.fact_transactions;"
```

### Step 5: Optimize with Impala

```bash
cd scripts/impala

# Refresh metadata and compute stats
impala-shell -i localhost -f impala_optimization.sql

# Run BI queries
impala-shell -i localhost -f impala_bi_queries.sql
```

### Step 6: Run Spark ETL

```bash
cd scripts/spark

# Run ETL pipeline
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 10 \
    --executor-memory 4G \
    --executor-cores 2 \
    spark_etl_warehouse.py \
    --date 2025-11-30
```

### Step 7: Start Kafka Streaming (Optional)

```bash
cd scripts/kafka

# Start Kafka producer (Terminal 1)
python kafka_producer.py

# Start Kafka consumer (Terminal 2)
python kafka_consumer.py

# Start Spark Structured Streaming (Terminal 3)
spark-submit spark_streaming_kafka.py
```

### Step 8: Train ML Models

```bash
cd scripts/spark

# Train recommendation engine
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 20 \
    --executor-memory 8G \
    spark_ml_recommendations.py \
    --train --evaluate --recommend

# Train churn prediction model
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 20 \
    --executor-memory 8G \
    spark_ml_churn_prediction.py \
    --train --evaluate --predict
```

**Output:**
- Recommendation model saved to `/ecommerce/models/recommendations/`
- Churn model saved to `/ecommerce/models/churn/`
- Predictions saved to `/ecommerce/analytics/ml/`

### Step 9: Setup Airflow Orchestration

```bash
cd airflow

# Run setup script
chmod +x setup_airflow.sh
sudo ./setup_airflow.sh

# Or manually:
export AIRFLOW_HOME=~/airflow

# Initialize database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@company.com \
    --password admin123

# Copy DAGs
cp dags/* $AIRFLOW_HOME/dags/

# Start Airflow webserver
airflow webserver --port 8080 &

# Start Airflow scheduler
airflow scheduler &

# Access UI at http://localhost:8080
# Username: admin, Password: admin123
```

**Available DAGs:**
- `daily_etl_pipeline` - Complete ETL (Pig → Hive → Impala) - Daily at 2 AM
- `hourly_data_refresh` - Aggregate tables refresh - Hourly at :05
- `weekly_ml_training` - ML model retraining - Sunday 3 AM
- `realtime_streaming_pipeline` - Streaming job monitoring - Every 5 minutes

---

## 📖 Components

### 1. Synthetic Data Generators

**Purpose**: Generate realistic, production-like datasets for testing and development.

**Features**:
- Realistic distributions and correlations
- 1M+ clickstream events
- 500K transactions
- 100K user profiles
- 10K products
- Configurable parameters

**Usage**:
```python
python data_generators/run_all_generators.py
```

### 2. Pig ETL Scripts

**Purpose**: Clean and normalize raw data.

**Operations**:
- Remove duplicates
- Filter bot traffic
- Normalize timestamps
- Validate data quality
- Remove malformed records

**Files**:
- `clickstream_clean.pig` - Clean clickstream events
- `transactions_clean.pig` - Clean transaction data

### 3. Hive Data Warehouse

**Purpose**: Store structured data for analytics.

**Schema**:
- **Dimension Tables**: `dim_users`, `dim_products` (SCD Type 2)
- **Fact Tables**: `fact_clickstream`, `fact_transactions` (Partitioned)
- **Aggregate Tables**: `agg_daily_sales`, `agg_user_metrics`

**Features**:
- Partitioning by date
- Bucketing for joins
- ORC/Parquet formats
- Materialized views

### 4. Impala Queries

**Purpose**: Real-time SQL analytics for BI dashboards.

**Dashboards**:
1. Real-Time Revenue Dashboard
2. Product Performance Leaderboard
3. Customer Segmentation
4. Conversion Funnel
5. Abandoned Cart Analytics

**Optimization**:
- Partition pruning
- Statistics computation
- Query hints (BROADCAST, SHUFFLE)

### 5. Spark Processing

**Purpose**: ETL, ML, and batch processing.

**Jobs**:
- **ETL**: `spark_etl_warehouse.py` - Load warehouse with quality checks
- **ML Recommendations**: `spark_ml_recommendations.py` - ALS collaborative filtering
- **ML Churn**: `spark_ml_churn_prediction.py` - Logistic regression churn model
- **Streaming**: `spark_streaming_kafka.py` - Real-time Kafka integration

**ML Models**:

#### Recommendation Engine (ALS)
- **Algorithm**: Alternating Least Squares
- **Input**: Transaction + Clickstream (implicit/explicit ratings)
- **Output**: Top 10 product recommendations per user
- **Metrics**: RMSE < 1.0, MAE < 0.8
- **Usage**: Product personalization, cross-selling
### 7. Airflow Orchestration

**Purpose**: Automate and schedule workflows.

**DAGs**:

#### `dag_daily_etl.py` - Daily Batch ETL
- **Schedule**: Daily at 2:00 AM
- **Tasks**: Pig cleaning → Hive dimension load → Fact load → Impala refresh → Quality checks
- **SLA**: 4 hours
- **Retries**: 2

#### `dag_hourly_refresh.py` - Hourly Data Refresh
- **Schedule**: Every hour at :05
- **Tasks**: Refresh aggregates → Materialized views → Real-time KPIs → Cache warming
- **SLA**: 30 minutes
- **Retries**: 3

#### `dag_weekly_ml_training.py` - ML Model Training
- **Schedule**: Sunday at 3:00 AM
- **Tasks**: Data prep → Train models (parallel) → Evaluate → Validate → Deploy → Report
- **Models**: Recommendation engine, Churn prediction
- **SLA**: 6 hours
- **Quality Gates**: RMSE < 1.0, AUC > 0.75

#### `dag_realtime_streaming.py` - Streaming Monitoring
- **Schedule**: Every 5 minutes
- **Tasks**: Kafka health → Producer status → Streaming job check → Lag monitoring → Auto-restart
- **SLA**: 10 minutes
- **Auto-recovery**: Yes

**Purpose**: Real-time event ingestion.

**Components**:
- Producer: Generate real-time events
- Consumer: Process event streams
- Spark Structured Streaming: ETL pipeline

### 7. Airflow Orchestration

**Purpose**: Automate and schedule workflows.

**DAGs**:
- `dag_daily_etl.py` - Daily batch processing
- `dag_hourly_refresh.py` - Hourly data refresh
- `dag_weekly_ml_training.py` - ML model training
- `dag_realtime_streaming.py` - Kafka streaming

---

## 📚 Data Dictionary

[Complete data dictionary available in `docs/data_dictionary.md`]

### Clickstream Events

| Field | Type | Description |
|-------|------|-------------|
| event_id | STRING | Unique event identifier (UUID) |
| user_id | STRING | User identifier |
| session_id | STRING | Session identifier (UUID) |
| event_time | TIMESTAMP | Event timestamp |
| page_url | STRING | URL of the page |
| product_id | STRING | Product identifier (if applicable) |
| event_type | STRING | Type: view, add_to_cart, purchase, exit |
| device_type | STRING | Device: mobile, desktop, tablet |
| ip_address | STRING | User IP address |
| traffic_source | STRING | Source: google, facebook, direct, etc. |

### Transactions

| Field | Type | Description |
|-------|------|-------------|
| transaction_id | STRING | Unique transaction ID |
| user_id | STRING | Customer user ID |
| product_id | STRING | Product purchased |
| total_amount | DECIMAL(12,2) | Total order amount |
| quantity | INT | Quantity purchased |
| payment_method | STRING | Payment type |
| status | STRING | Order status: completed, pending, cancelled |
| transaction_timestamp | TIMESTAMP | Order timestamp |

[See full data dictionary for all tables and fields]

---

## ⚡ Performance Optimization

### Hive Optimization

1. **Partitioning**
   ```sql
   -- Partition by date for time-series queries
   PARTITIONED BY (year INT, month INT, day INT)
   ```

2. **Bucketing**
   ```sql
   -- Bucket by user_id for join optimization
   CLUSTERED BY (user_id) INTO 32 BUCKETS
   ```

3. **File Formats**
   - Use **ORC** for transactional tables
   - Use **Parquet** for analytical tables
   - Enable compression (Snappy, Zlib)

### Impala Optimization

1. **Compute Statistics**
   ```sql
   COMPUTE STATS fact_transactions;
   COMPUTE INCREMENTAL STATS fact_clickstream;
   ```

2. **Query Hints**
   ```sql
   -- Broadcast join for small tables
   SELECT /*+ BROADCAST(dim_users) */ ...
   ```

3. **Partition Pruning**
   ```sql
   -- Always filter on partition columns
   WHERE year = 2025 AND month = 11 AND day = 30
   ```

### Spark Optimization

1. **Caching**
   ```python
   df.cache()  # Cache frequently accessed DataFrames
   ```

2. **Partitioning**
   ```python
   df.repartition(200, "user_id")  # Repartition for better parallelism
   ```

3. **Broadcast Joins**
   ```python
   df.join(broadcast(small_df), "key")
   ```

---

## 🔒 Monitoring & Governance

### Data Quality

- **Validation Rules**: Check for NULLs, duplicates, value ranges
- **Quality Metrics**: Completeness, accuracy, consistency
- **Automated Checks**: Built into Pig and Spark pipelines

### Security

1. **Access Control (Ranger)**
   - Role-based access control (RBAC)
   - Column-level security
   - Row-level filtering

2. **PII Masking**
   ```sql
   -- Mask email addresses
   SELECT CONCAT('***@', SUBSTR(email, INSTR(email, '@')+1)) AS masked_email
   ```

3. **Audit Logs**
   - All data access logged
   - Query tracking
   - User activity monitoring

### Lineage (Atlas)

- Track data flow from source to consumption
- Impact analysis for changes
- Metadata management

---

## 🐛 Troubleshooting

### Common Issues

1. **HDFS Out of Space**
   ```bash
   # Check space
   hdfs dfs -df -h
   
   # Clean old data
   hdfs dfs -rm -r /ecommerce/archive/*
   ```

2. **Hive Metastore Connection**
   ```bash
   # Restart metastore
   hive --service metastore &
   ```

3. **Impala Not Seeing New Data**
   ```sql
   -- Refresh metadata
   INVALIDATE METADATA;
   REFRESH fact_transactions;
   ```

4. **Spark Out of Memory**
   ```bash
   # Increase executor memory
   spark-submit --executor-memory 8G ...
   ```

5. **Airflow DAG Not Running**
   ```bash
   # Check scheduler
   airflow scheduler
   
   # Trigger manually
   airflow dags trigger dag_daily_etl
   ```

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/YourFeature`)
3. Commit changes (`git commit -m 'Add YourFeature'`)
4. Push to branch (`git push origin feature/YourFeature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 📞 Contact

**Project Maintainer**: Your Name
- Email: your.email@example.com
- LinkedIn: [Your LinkedIn](https://linkedin.com/in/yourprofile)
- GitHub: [Your GitHub](https://github.com/yourusername)

---

## 🎓 Learning Resources

### For Beginners

1. **Apache Hadoop**: [Official Documentation](https://hadoop.apache.org/docs/)
2. **Apache Spark**: [Spark Programming Guide](https://spark.apache.org/docs/latest/programming-guide.html)
3. **Apache Hive**: [Hive Tutorial](https://cwiki.apache.org/confluence/display/Hive/Tutorial)
4. **Apache Pig**: [Pig Latin Basics](https://pig.apache.org/docs/latest/basic.html)

### Advanced Topics

- Data Lake Architecture
- Lambda vs Kappa Architecture
- Real-Time Analytics
- ML Pipelines in Production

---

## 🎯 Next Steps

After completing this project, you'll have hands-on experience with:

✅ Big Data architectures and design patterns
✅ ETL pipeline development
✅ Data warehousing concepts
✅ Real-time streaming
✅ Machine learning in production
✅ Workflow orchestration
✅ Data governance and security

**Congratulations on building a production-grade data engineering platform!** 🎉

---

*Last Updated: November 30, 2025*
