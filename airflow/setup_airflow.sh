#!/bin/bash

# ============================================================================
# Airflow Setup Script
# ============================================================================

set -e

echo "=========================================="
echo "Airflow Installation and Setup"
echo "=========================================="

# Configuration
export AIRFLOW_HOME=/home/airflow
export AIRFLOW_VERSION=2.8.0
export PYTHON_VERSION=3.8
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# ============================================================================
# 1. Install Airflow
# ============================================================================

echo ""
echo "Step 1: Installing Apache Airflow ${AIRFLOW_VERSION}..."

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Install providers
pip install apache-airflow-providers-apache-spark
pip install apache-airflow-providers-apache-hive
pip install apache-airflow-providers-celery
pip install apache-airflow-providers-redis
pip install apache-airflow-providers-postgres

# Install additional dependencies
pip install kafka-python
pip install pyspark

echo "✓ Airflow installed"

# ============================================================================
# 2. Initialize Database
# ============================================================================

echo ""
echo "Step 2: Initializing Airflow database..."

# Create Airflow home
mkdir -p $AIRFLOW_HOME

# Initialize database
airflow db init

echo "✓ Database initialized"

# ============================================================================
# 3. Create Admin User
# ============================================================================

echo ""
echo "Step 3: Creating admin user..."

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@company.com \
    --password admin123

echo "✓ Admin user created"
echo "  Username: admin"
echo "  Password: admin123"

# ============================================================================
# 4. Configure DAGs Folder
# ============================================================================

echo ""
echo "Step 4: Configuring DAGs folder..."

# Create symlink to project DAGs
PROJECT_DIR="/home/airflow/ecommerce-platform"
ln -sf ${PROJECT_DIR}/airflow/dags ${AIRFLOW_HOME}/dags

echo "✓ DAGs folder configured"

# ============================================================================
# 5. Configure Connections
# ============================================================================

echo ""
echo "Step 5: Creating Airflow connections..."

# Spark connection
airflow connections add spark_default \
    --conn-type spark \
    --conn-host yarn \
    --conn-extra '{"queue": "default", "deploy-mode": "cluster"}'

# Hive connection
airflow connections add hive_default \
    --conn-type hive_cli \
    --conn-host hive-server.local \
    --conn-port 10000 \
    --conn-schema default

# Kafka connection (using HTTP for simplicity)
airflow connections add kafka_default \
    --conn-type http \
    --conn-host kafka-broker.local \
    --conn-port 9092

echo "✓ Connections created"

# ============================================================================
# 6. Create System Services
# ============================================================================

echo ""
echo "Step 6: Creating systemd services..."

# Airflow Webserver service
cat > /etc/systemd/system/airflow-webserver.service << 'EOF'
[Unit]
Description=Airflow webserver
After=network.target

[Service]
Type=simple
User=airflow
Group=airflow
Environment="AIRFLOW_HOME=/home/airflow"
ExecStart=/usr/local/bin/airflow webserver -p 8080
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
EOF

# Airflow Scheduler service
cat > /etc/systemd/system/airflow-scheduler.service << 'EOF'
[Unit]
Description=Airflow scheduler
After=network.target

[Service]
Type=simple
User=airflow
Group=airflow
Environment="AIRFLOW_HOME=/home/airflow"
ExecStart=/usr/local/bin/airflow scheduler
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd
systemctl daemon-reload

echo "✓ Systemd services created"

# ============================================================================
# 7. Start Services
# ============================================================================

echo ""
echo "Step 7: Starting Airflow services..."

# Enable and start services
systemctl enable airflow-webserver
systemctl enable airflow-scheduler
systemctl start airflow-webserver
systemctl start airflow-scheduler

echo "✓ Services started"

# ============================================================================
# 8. Verification
# ============================================================================

echo ""
echo "Step 8: Verifying installation..."

sleep 5

# Check service status
if systemctl is-active --quiet airflow-webserver; then
    echo "✓ Webserver is running"
else
    echo "✗ Webserver failed to start"
fi

if systemctl is-active --quiet airflow-scheduler; then
    echo "✓ Scheduler is running"
else
    echo "✗ Scheduler failed to start"
fi

# List DAGs
echo ""
echo "Available DAGs:"
airflow dags list

# ============================================================================
# COMPLETE
# ============================================================================

echo ""
echo "=========================================="
echo "✅ Airflow Setup Complete!"
echo "=========================================="
echo ""
echo "Access the web UI at: http://localhost:8080"
echo "Username: admin"
echo "Password: admin123"
echo ""
echo "Available DAGs:"
echo "  - daily_etl_pipeline"
echo "  - hourly_data_refresh"
echo "  - weekly_ml_training"
echo "  - realtime_streaming_pipeline"
echo ""
echo "To manage services:"
echo "  systemctl status airflow-webserver"
echo "  systemctl status airflow-scheduler"
echo "  systemctl restart airflow-webserver"
echo "  systemctl restart airflow-scheduler"
echo ""
