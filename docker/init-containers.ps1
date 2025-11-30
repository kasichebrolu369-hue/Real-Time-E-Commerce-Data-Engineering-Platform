# ============================================================================
# Docker Containers Initialization Script
# Runs after containers are started to initialize services
# ============================================================================

param(
    [switch]$SkipData,
    [switch]$SkipHDFS
)

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Container Initialization Starting..." -ForegroundColor Yellow
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# Function to wait for container
function Wait-ForContainer {
    param($ContainerName, $MaxWait = 60)
    
    Write-Host "Waiting for $ContainerName..." -ForegroundColor Gray
    $waited = 0
    while ($waited -lt $MaxWait) {
        $status = docker ps --filter "name=$ContainerName" --filter "status=running" --format "{{.Names}}"
        if ($status) {
            Write-Host "  ✓ $ContainerName is running" -ForegroundColor Green
            return $true
        }
        Start-Sleep -Seconds 5
        $waited += 5
    }
    Write-Host "  ✗ $ContainerName failed to start" -ForegroundColor Red
    return $false
}

# Wait for critical containers
Write-Host "[1/5] Waiting for containers to start..." -ForegroundColor Green
Wait-ForContainer "ecommerce-namenode"
Wait-ForContainer "ecommerce-datanode"
Wait-ForContainer "ecommerce-spark-master"
Wait-ForContainer "ecommerce-spark-worker"

# Generate synthetic data
if (!$SkipData) {
    Write-Host ""
    Write-Host "[2/5] Generating synthetic data..." -ForegroundColor Green
    Write-Host "      This will take 2-5 minutes..." -ForegroundColor Yellow
    
    $dataGenRunning = docker ps --filter "name=ecommerce-data-generator" --format "{{.Names}}"
    if ($dataGenRunning) {
        Write-Host "      Data generator already running" -ForegroundColor Gray
        Write-Host "      Monitor: docker logs -f ecommerce-data-generator" -ForegroundColor Yellow
        
        # Wait for data generation to complete (check for completion message)
        $timeout = 300 # 5 minutes
        $elapsed = 0
        while ($elapsed -lt $timeout) {
            $logs = docker logs ecommerce-data-generator 2>&1 | Select-String "generation complete"
            if ($logs) {
                Write-Host "      ✓ Data generation complete" -ForegroundColor Green
                break
            }
            Start-Sleep -Seconds 10
            $elapsed += 10
        }
    } else {
        Write-Host "      Starting data generator..." -ForegroundColor Gray
        docker-compose run --rm data-generator
        Write-Host "      ✓ Data generated" -ForegroundColor Green
    }
} else {
    Write-Host "[2/5] Skipping data generation" -ForegroundColor Gray
}

# Initialize HDFS
if (!$SkipHDFS) {
    Write-Host ""
    Write-Host "[3/5] Initializing HDFS..." -ForegroundColor Green
    
    # Copy init script to namenode
    docker cp docker/init-hdfs.sh ecommerce-namenode:/tmp/init-hdfs.sh
    
    # Make it executable and run
    docker exec ecommerce-namenode bash -c "chmod +x /tmp/init-hdfs.sh && /tmp/init-hdfs.sh"
    
    Write-Host "      ✓ HDFS initialized" -ForegroundColor Green
} else {
    Write-Host "[3/5] Skipping HDFS initialization" -ForegroundColor Gray
}

# Initialize Hive tables
Write-Host ""
Write-Host "[4/5] Setting up Hive tables..." -ForegroundColor Green
Start-Sleep -Seconds 10  # Wait for Hive to be ready

$hiveRunning = docker ps --filter "name=ecommerce-hiveserver2" --format "{{.Names}}"
if ($hiveRunning) {
    Write-Host "      Creating database and tables..." -ForegroundColor Gray
    # Copy Hive scripts
    if (Test-Path "scripts\hive\hive_ddl_create_tables.sql") {
        docker cp scripts\hive\hive_ddl_create_tables.sql ecommerce-hiveserver2:/tmp/
        Write-Host "      ✓ Hive DDL scripts copied" -ForegroundColor Green
    }
} else {
    Write-Host "      ⚠ Hive not running (minimal mode)" -ForegroundColor Yellow
}

# Create Kafka topics
Write-Host ""
Write-Host "[5/5] Creating Kafka topics..." -ForegroundColor Green

$kafkaRunning = docker ps --filter "name=ecommerce-kafka" --format "{{.Names}}"
if ($kafkaRunning) {
    Start-Sleep -Seconds 10  # Wait for Kafka to be ready
    
    $topics = @("clickstream-events", "transactions", "user-activity")
    foreach ($topic in $topics) {
        Write-Host "      Creating topic: $topic" -ForegroundColor Gray
        docker exec ecommerce-kafka kafka-topics --create `
            --if-not-exists `
            --bootstrap-server localhost:9092 `
            --topic $topic `
            --partitions 3 `
            --replication-factor 1 2>$null
    }
    Write-Host "      ✓ Kafka topics created" -ForegroundColor Green
} else {
    Write-Host "      ⚠ Kafka not running (minimal mode)" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  ✓ Initialization Complete!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. View dashboard: http://localhost:8501" -ForegroundColor White
Write-Host "  2. Check Spark UI: http://localhost:8080" -ForegroundColor White
Write-Host "  3. Train ML models using Jupyter or Spark submit" -ForegroundColor White
Write-Host ""
