#!/bin/bash
# ============================================================================
# HDFS Initialization Script
# Creates directory structure and uploads data to HDFS
# ============================================================================

set -e

echo "============================================================"
echo "  HDFS Initialization Starting..."
echo "============================================================"

# Wait for HDFS to be ready
echo "[1/4] Waiting for HDFS NameNode..."
until hdfs dfs -ls / > /dev/null 2>&1; do
    echo "      Waiting for HDFS to be available..."
    sleep 5
done
echo "      ✓ HDFS is ready"

# Create directory structure
echo "[2/4] Creating HDFS directory structure..."
hdfs dfs -mkdir -p /ecommerce/raw/clickstream
hdfs dfs -mkdir -p /ecommerce/raw/transactions
hdfs dfs -mkdir -p /ecommerce/raw/users
hdfs dfs -mkdir -p /ecommerce/raw/products
hdfs dfs -mkdir -p /ecommerce/clean/clickstream
hdfs dfs -mkdir -p /ecommerce/clean/transactions
hdfs dfs -mkdir -p /ecommerce/warehouse
hdfs dfs -mkdir -p /ecommerce/analytics
hdfs dfs -mkdir -p /ecommerce/models
echo "      ✓ Directories created"

# Set permissions
echo "[3/4] Setting permissions..."
hdfs dfs -chmod -R 777 /ecommerce
echo "      ✓ Permissions set"

# Upload data if available
echo "[4/4] Checking for data to upload..."
if [ -d "/data/raw/users" ] && [ "$(ls -A /data/raw/users)" ]; then
    echo "      Uploading users data..."
    hdfs dfs -put /data/raw/users/* /ecommerce/raw/users/ || true
fi

if [ -d "/data/raw/products" ] && [ "$(ls -A /data/raw/products)" ]; then
    echo "      Uploading products data..."
    hdfs dfs -put /data/raw/products/* /ecommerce/raw/products/ || true
fi

if [ -d "/data/raw/transactions" ] && [ "$(ls -A /data/raw/transactions)" ]; then
    echo "      Uploading transactions data..."
    hdfs dfs -put -f /data/raw/transactions/* /ecommerce/raw/transactions/ || true
fi

if [ -d "/data/raw/clickstream" ] && [ "$(ls -A /data/raw/clickstream)" ]; then
    echo "      Uploading clickstream data..."
    hdfs dfs -put -f /data/raw/clickstream/* /ecommerce/raw/clickstream/ || true
fi

echo "      ✓ Data upload complete"

# Verify structure
echo ""
echo "============================================================"
echo "  HDFS Structure:"
echo "============================================================"
hdfs dfs -ls -R /ecommerce | head -20

echo ""
echo "============================================================"
echo "  ✓ HDFS Initialization Complete!"
echo "============================================================"
