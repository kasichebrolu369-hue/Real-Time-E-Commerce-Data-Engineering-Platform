# 🚀 Installation Guide for Windows

## Current Situation

You have Python 3.13 in your `ecom` environment, which is **perfect** for:
- ✅ Data generation
- ✅ Pig ETL scripts
- ✅ Hive queries
- ✅ Spark jobs
- ✅ Kafka streaming
- ✅ Machine Learning models

But **NOT** for:
- ❌ Apache Airflow (requires Python 3.8-3.12)

---

## Step 1: Install Dependencies for Current Environment (Python 3.13)

You've already done this! Now verify:

```powershell
# Activate your environment
cd "B:\Data Science Projects(Own)\Real-Time E-Commerce Data Engineering Platform"
.\ecom\Scripts\activate

# Verify installation
python -c "import pandas, faker, pyspark; print('✓ All packages working!')"
```

---

## Step 2: YOU CAN START WORKING NOW!

**You don't need Airflow to learn and run this project!** All the core components work perfectly:

### 🎯 What You Can Do Right Now:

#### A. Generate Synthetic Data (5 minutes)
```powershell
cd data_generators
python run_all_generators.py
```

#### B. Explore the Data
```powershell
# View sample users
python -c "import pandas as pd; print(pd.read_csv('data/raw/users/users.csv').head())"

# View sample products
python -c "import pandas as pd; print(pd.read_csv('data/raw/products/products.csv').head())"
```

#### C. Test Spark (if installed)
```powershell
cd scripts\spark
spark-submit --version
```

---

## Step 3: About Airflow (OPTIONAL)

Airflow is **only for workflow automation**. You can:

1. **Skip it for now** - Focus on learning the data pipeline
2. **Use Python scripts** - Run components manually (great for learning!)
3. **Install later** - Create separate Python 3.11 environment when needed

### If You Really Need Airflow:

You'll need to install Python 3.11 separately. Since conda isn't installed, use `venv`:

```powershell
# Download and install Python 3.11 from python.org
# Then create a separate environment:

cd "B:\Data Science Projects(Own)\Real-Time E-Commerce Data Engineering Platform"
python3.11 -m venv airflow_env
.\airflow_env\Scripts\activate

pip install apache-airflow==2.8.0
pip install apache-airflow-providers-apache-spark
```

---

## 🎓 Recommended Learning Path (No Airflow Needed!)

### Week 1: Data Generation & Exploration
```powershell
cd data_generators
python run_all_generators.py
```
**Learn**: Synthetic data patterns, pandas DataFrames

### Week 2: Data Processing with Python
```powershell
cd scripts\spark
python -c "from pyspark.sql import SparkSession; print('Spark ready!')"
```
**Learn**: PySpark basics, data transformations

### Week 3: Machine Learning
```powershell
# Generate data first, then:
cd scripts\spark
# spark-submit spark_ml_recommendations.py --train
```
**Learn**: Collaborative filtering, model training

### Week 4: Real-Time Streaming (Optional)
```powershell
# Requires Kafka installation
cd scripts\kafka
python kafka_producer.py
```

### Week 5: Add Airflow (Optional)
Only after mastering the above!

---

## ✅ Your Next Steps RIGHT NOW:

1. **Activate your environment**:
   ```powershell
   cd "B:\Data Science Projects(Own)\Real-Time E-Commerce Data Engineering Platform"
   .\ecom\Scripts\activate
   ```

2. **Generate data** (takes 2-3 minutes):
   ```powershell
   cd data_generators
   python run_all_generators.py
   ```

3. **Explore the generated data**:
   ```powershell
   python
   >>> import pandas as pd
   >>> users = pd.read_csv('data/raw/users/users.csv')
   >>> print(users.head())
   >>> print(f"Total users: {len(users)}")
   >>> exit()
   ```

4. **Check what was created**:
   ```powershell
   dir data\raw\users
   dir data\raw\products
   dir data\raw\transactions
   dir data\raw\clickstream
   ```

---

## 🎯 Focus Areas (No Big Data Infrastructure Needed)

You can learn EVERYTHING in this project using just Python:

### 1. Data Engineering Concepts ✅
- Data generation patterns
- ETL logic (even without Pig/Hive)
- Data quality validation
- Partitioning strategies

### 2. Machine Learning ✅
- Feature engineering
- Recommendation systems (ALS)
- Churn prediction
- Model evaluation

### 3. Data Processing ✅
- Pandas for data manipulation
- PySpark (local mode, no cluster needed)
- Data transformations
- Joins and aggregations

### 4. Code Organization ✅
- Project structure
- Configuration management
- Error handling
- Documentation

---

## 💡 Pro Tip: Learn by Doing

Instead of worrying about Airflow, **run the components manually** and understand what each does:

```powershell
# 1. Generate data
python data_generators\run_all_generators.py

# 2. "Manually run the daily ETL" (learn what Airflow would automate)
# - Clean data (Pig logic in Python)
# - Load to warehouse (Spark ETL)
# - Generate reports (BI queries)

# 3. Train ML models
# spark-submit scripts\spark\spark_ml_recommendations.py --train

# 4. THEN understand how Airflow automates this!
```

---

## 🚫 What You DON'T Need Right Now:

- ❌ Hadoop HDFS cluster
- ❌ Apache Hive installation
- ❌ Apache Impala
- ❌ Apache Kafka cluster
- ❌ Apache Airflow
- ❌ YARN cluster

All these are for **production deployment**. For learning, you can:
- Use local files instead of HDFS
- Use pandas instead of Hive
- Use Spark in local mode
- Run scripts manually instead of Airflow

---

## 📚 Updated Project Value

This project teaches you:

1. ✅ **Data Engineering Principles** (not just tools)
2. ✅ **Code Architecture** (how to structure projects)
3. ✅ **ML Pipelines** (feature engineering, training, evaluation)
4. ✅ **Best Practices** (error handling, logging, documentation)

The **concepts** matter more than the specific tools!

---

## 🎉 Ready to Start?

Run these commands NOW:

```powershell
# 1. Activate environment
.\ecom\Scripts\activate

# 2. Navigate to data generators
cd data_generators

# 3. Generate data
python run_all_generators.py

# 4. Celebrate! 🎉
```

You have a complete, working data engineering project ready to explore!

---

**Remember**: Airflow is just for automation. The real learning is in understanding the data pipeline, ML models, and best practices - all of which work perfectly in your current setup! 🚀
