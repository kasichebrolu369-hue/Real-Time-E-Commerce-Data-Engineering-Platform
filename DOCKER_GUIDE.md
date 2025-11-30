# ============================================================================
# Docker Commands Reference
# ============================================================================

## START ALL SERVICES
```powershell
# Start all containers in detached mode
docker-compose up -d

# Start and view logs
docker-compose up

# Start specific service
docker-compose up -d spark-master
```

## STOP SERVICES
```powershell
# Stop all containers
docker-compose stop

# Stop and remove containers
docker-compose down

# Stop and remove containers + volumes (WARNING: deletes all data!)
docker-compose down -v
```

## VIEW LOGS
```powershell
# View all logs
docker-compose logs

# Follow logs (live)
docker-compose logs -f

# View logs for specific service
docker-compose logs -f spark-master
docker-compose logs -f namenode
docker-compose logs -f airflow-webserver
```

## CHECK STATUS
```powershell
# List running containers
docker-compose ps

# List all containers (including stopped)
docker ps -a

# Check resource usage
docker stats
```

## EXECUTE COMMANDS IN CONTAINERS
```powershell
# Open bash shell in container
docker exec -it ecommerce-namenode bash
docker exec -it ecommerce-spark-master bash

# Run HDFS commands
docker exec ecommerce-namenode hdfs dfs -ls /
docker exec ecommerce-namenode hdfs dfs -mkdir -p /ecommerce/raw

# Run Spark job
docker exec ecommerce-spark-master spark-submit /scripts/spark/spark_etl_local.py

# Run Hive query
docker exec -it ecommerce-hiveserver2 beeline -u jdbc:hive2://localhost:10000
```

## DATA MANAGEMENT
```powershell
# Upload data to HDFS
docker exec ecommerce-namenode hdfs dfs -put /data/raw/users /ecommerce/raw/
docker exec ecommerce-namenode hdfs dfs -put /data/raw/products /ecommerce/raw/

# Download data from HDFS
docker exec ecommerce-namenode hdfs dfs -get /ecommerce/warehouse /data/

# Copy files to container
docker cp data/ ecommerce-namenode:/data/

# Copy files from container
docker cp ecommerce-namenode:/hadoop/logs ./logs/
```

## TROUBLESHOOTING
```powershell
# Restart specific service
docker-compose restart spark-master

# Rebuild container (after code changes)
docker-compose up -d --build spark-master

# View container details
docker inspect ecommerce-namenode

# Check network
docker network ls
docker network inspect ecommerce-data-engineering-platform_ecommerce-network
```

## CLEANUP
```powershell
# Remove stopped containers
docker container prune

# Remove unused images
docker image prune

# Remove unused volumes
docker volume prune

# Full cleanup (WARNING: removes everything!)
docker system prune -a --volumes
```

## USEFUL COMMANDS FOR THIS PROJECT

### 1. Upload Generated Data to HDFS
```powershell
# Create directories in HDFS
docker exec ecommerce-namenode hdfs dfs -mkdir -p /ecommerce/raw/users
docker exec ecommerce-namenode hdfs dfs -mkdir -p /ecommerce/raw/products
docker exec ecommerce-namenode hdfs dfs -mkdir -p /ecommerce/raw/transactions
docker exec ecommerce-namenode hdfs dfs -mkdir -p /ecommerce/raw/clickstream

# Upload data
docker exec ecommerce-namenode hdfs dfs -put /data/raw/users/users.csv /ecommerce/raw/users/
docker exec ecommerce-namenode hdfs dfs -put /data/raw/products/products.csv /ecommerce/raw/products/
docker exec ecommerce-namenode hdfs dfs -put /data/raw/transactions/ /ecommerce/raw/
docker exec ecommerce-namenode hdfs dfs -put /data/raw/clickstream/ /ecommerce/raw/

# Verify upload
docker exec ecommerce-namenode hdfs dfs -ls -R /ecommerce/raw/
```

### 2. Run Spark ETL Job
```powershell
# Submit Spark job to cluster
docker exec ecommerce-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /scripts/spark/spark_etl_local.py

# Run in local mode
docker exec ecommerce-spark-master python /scripts/spark/spark_etl_local.py
```

### 3. Run Machine Learning Models
```powershell
# Train recommendation model
docker exec ecommerce-spark-master spark-submit \
    --master spark://spark-master:7077 \
    /scripts/spark/spark_ml_recommendations.py

# Train churn prediction model
docker exec ecommerce-spark-master spark-submit \
    --master spark://spark-master:7077 \
    /scripts/spark/spark_ml_churn_prediction.py
```

### 4. Access Hive
```powershell
# Open Hive CLI
docker exec -it ecommerce-hiveserver2 hive

# Run Hive script
docker exec ecommerce-hiveserver2 hive -f /hive-scripts/hive_ddl_create_tables.sql

# Connect with Beeline
docker exec -it ecommerce-hiveserver2 beeline -u "jdbc:hive2://localhost:10000"
```

### 5. Kafka Operations
```powershell
# List Kafka topics
docker exec ecommerce-kafka kafka-topics --list --bootstrap-server localhost:9092

# Create topic
docker exec ecommerce-kafka kafka-topics --create \
    --topic ecommerce-events \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1

# Consume messages
docker exec ecommerce-kafka kafka-console-consumer \
    --topic ecommerce-events \
    --from-beginning \
    --bootstrap-server localhost:9092
```

### 6. Jupyter Notebook
```powershell
# Get Jupyter token
docker logs ecommerce-jupyter 2>&1 | Select-String "token"

# Access notebook at http://localhost:8888 with the token
```

### 7. Airflow Operations
```powershell
# Trigger DAG manually
docker exec ecommerce-airflow-webserver airflow dags trigger daily_etl_pipeline

# List all DAGs
docker exec ecommerce-airflow-webserver airflow dags list

# Test specific task
docker exec ecommerce-airflow-webserver airflow tasks test daily_etl_pipeline task_name 2025-11-30
```

## WEB INTERFACES

| Service | URL | Credentials |
|---------|-----|-------------|
| Hadoop NameNode | http://localhost:9870 | - |
| Spark Master | http://localhost:8080 | - |
| Spark Worker | http://localhost:8081 | - |
| Airflow | http://localhost:8082 | admin/admin123 |
| HiveServer2 | http://localhost:10002 | - |
| Jupyter Notebook | http://localhost:8888 | See logs for token |

## PERFORMANCE MONITORING
```powershell
# Monitor resource usage
docker stats

# Check Spark job progress
# Open http://localhost:8080 and click on running applications

# Check HDFS health
docker exec ecommerce-namenode hdfs dfsadmin -report

# Check Kafka consumer lag
docker exec ecommerce-kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --describe --group my-consumer-group
```
