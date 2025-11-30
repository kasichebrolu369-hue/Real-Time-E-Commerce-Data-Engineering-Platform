# 🐳 Docker Setup - Quick Start

## Prerequisites

1. **Install Docker Desktop for Windows**
   - Download from: https://www.docker.com/products/docker-desktop/
   - Requires Windows 10/11 Pro, Enterprise, or Education with WSL 2
   - Minimum 8GB RAM (16GB recommended)
   - 20GB free disk space

2. **Enable WSL 2** (if not already enabled)
   ```powershell
   wsl --install
   wsl --set-default-version 2
   ```

3. **Configure Docker Desktop**
   - Open Docker Desktop Settings
   - Resources → Memory: Allocate at least 8GB
   - Resources → CPU: Allocate at least 4 cores
   - Enable "Use Docker Compose V2"

## Quick Start (5 Minutes)

### Step 1: Start All Services
```powershell
# Navigate to project directory
cd "B:\Data Science Projects(Own)\Real-Time E-Commerce Data Engineering Platform"

# Run the startup script
.\docker-start.ps1
```

**What This Does:**
- ✅ Pulls all Docker images (~5GB, first time only)
- ✅ Starts 14 containers (Hadoop, Spark, Hive, Kafka, Airflow, Jupyter)
- ✅ Creates network and volumes
- ✅ Initializes databases
- ✅ Takes ~2-3 minutes

### Step 2: Verify Services Are Running
```powershell
docker-compose ps
```

You should see all services with status "Up".

### Step 3: Access Web Interfaces

Open your browser and visit:

| Service | URL | Purpose |
|---------|-----|---------|
| **Hadoop HDFS** | http://localhost:9870 | View distributed file system |
| **Spark Master** | http://localhost:8080 | Monitor Spark cluster |
| **Airflow** | http://localhost:8082 | Workflow orchestration (admin/admin123) |
| **Jupyter** | http://localhost:8888 | Interactive notebooks |

## Running the Complete Pipeline

### 1. Upload Your Generated Data to HDFS

```powershell
# Create HDFS directories
docker exec ecommerce-namenode hdfs dfs -mkdir -p /ecommerce/raw

# Upload data (this copies from your local data/raw folder)
docker exec ecommerce-namenode hdfs dfs -put /data/raw/users /ecommerce/raw/
docker exec ecommerce-namenode hdfs dfs -put /data/raw/products /ecommerce/raw/
docker exec ecommerce-namenode hdfs dfs -put /data/raw/transactions /ecommerce/raw/
docker exec ecommerce-namenode hdfs dfs -put /data/raw/clickstream /ecommerce/raw/

# Verify data is uploaded
docker exec ecommerce-namenode hdfs dfs -ls /ecommerce/raw/
```

### 2. Run Spark ETL Job

```powershell
# Run the Spark ETL pipeline
docker exec ecommerce-spark-master spark-submit \
    --master spark://spark-master:7077 \
    /scripts/spark/spark_etl_local.py
```

### 3. Train Machine Learning Models

```powershell
# Create ML models directory
New-Item -ItemType Directory -Path "models" -Force

# Train recommendation engine
docker exec ecommerce-spark-master python /scripts/spark/spark_ml_recommendations_local.py

# Train churn prediction model
docker exec ecommerce-spark-master python /scripts/spark/spark_ml_churn_local.py
```

### 4. Interactive Analysis with Jupyter

```powershell
# Get Jupyter token
docker logs ecommerce-jupyter 2>&1 | Select-String "token"

# Copy the token and open http://localhost:8888
# Create new notebook and start analyzing!
```

## Common Docker Commands

### Start/Stop Services
```powershell
# Start all services
docker-compose up -d

# Stop all services
docker-compose stop

# Stop and remove containers (keeps data)
docker-compose down

# Stop and remove everything including data (⚠️ WARNING)
docker-compose down -v
```

### View Logs
```powershell
# View all logs
docker-compose logs

# View logs for specific service (live)
docker-compose logs -f spark-master
docker-compose logs -f airflow-webserver

# View last 100 lines
docker-compose logs --tail=100 namenode
```

### Execute Commands Inside Containers
```powershell
# Open bash shell
docker exec -it ecommerce-spark-master bash

# Run HDFS command
docker exec ecommerce-namenode hdfs dfs -ls /

# Run Hive query
docker exec -it ecommerce-hiveserver2 hive -e "SHOW DATABASES;"
```

### Monitor Resources
```powershell
# Check container status
docker-compose ps

# Monitor resource usage (live)
docker stats

# Check disk usage
docker system df
```

## Troubleshooting

### Issue: Containers Won't Start
```powershell
# Check Docker is running
docker ps

# View container logs
docker-compose logs namenode

# Restart specific service
docker-compose restart namenode
```

### Issue: Port Already in Use
```powershell
# Find process using port
netstat -ano | findstr :8080

# Kill process (replace PID)
taskkill /PID <pid> /F

# Or change port in docker-compose.yml
```

### Issue: Out of Memory
```powershell
# Increase Docker memory in Docker Desktop settings
# Settings → Resources → Memory → Set to 12GB or more

# Restart Docker Desktop
```

### Issue: Slow Performance
```powershell
# Check resources
docker stats

# Reduce number of services
# Comment out services you don't need in docker-compose.yml

# Or run only essential services:
docker-compose up -d namenode datanode spark-master spark-worker
```

## What Each Service Does

| Service | Purpose | When to Use |
|---------|---------|-------------|
| **namenode** | HDFS master - stores metadata | Always |
| **datanode** | HDFS worker - stores actual data | Always |
| **spark-master** | Spark cluster manager | For Spark jobs |
| **spark-worker** | Spark executor | For Spark jobs |
| **hive-metastore** | Hive metadata database | For SQL queries |
| **hiveserver2** | Hive query server | For SQL queries |
| **zookeeper** | Kafka coordination | For streaming |
| **kafka** | Message broker | For real-time data |
| **airflow-webserver** | Workflow UI | For automation |
| **airflow-scheduler** | Workflow executor | For automation |
| **jupyter** | Interactive notebooks | For exploration |

## Resource Requirements

### Minimum (Core Services Only)
- **RAM**: 8GB
- **CPU**: 4 cores
- **Disk**: 20GB
- **Services**: namenode, datanode, spark-master, spark-worker

### Recommended (All Services)
- **RAM**: 16GB
- **CPU**: 8 cores
- **Disk**: 50GB
- **Services**: All 14 containers

### How to Run Minimal Setup
```powershell
# Edit docker-compose.yml and comment out services you don't need
# Or start only specific services:
docker-compose up -d namenode datanode spark-master spark-worker jupyter
```

## Next Steps

1. **✅ Data Generated** - You already have 1.6M+ records
2. **✅ Docker Running** - All services started
3. **⏭️ Upload to HDFS** - Run upload commands above
4. **⏭️ Run Spark ETL** - Process your data
5. **⏭️ Train ML Models** - Build recommendations & churn prediction
6. **⏭️ Create Dashboards** - Visualize insights in Jupyter

## Complete Workflow Example

```powershell
# 1. Start services
.\docker-start.ps1

# 2. Upload data
docker exec ecommerce-namenode hdfs dfs -mkdir -p /ecommerce/raw
docker exec ecommerce-namenode hdfs dfs -put /data/raw/users /ecommerce/raw/

# 3. Run Spark ETL
docker exec ecommerce-spark-master python /scripts/spark/spark_etl_local.py

# 4. Check results
docker exec ecommerce-namenode hdfs dfs -ls /ecommerce/warehouse/

# 5. Open Jupyter for analysis
# Get token: docker logs ecommerce-jupyter 2>&1 | Select-String "token"
# Visit: http://localhost:8888
```

Ready to start? Run `.\docker-start.ps1` now! 🚀
