# ============================================================================
# E-Commerce Data Engineering Platform - Docker Complete Setup
# ============================================================================

param(
    [switch]$Minimal,
    [switch]$Clean,
    [switch]$Rebuild
)

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "  E-COMMERCE DATA ENGINEERING PLATFORM - Docker Complete Setup" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

# Check if Docker is running
Write-Host "[1/8] Checking Docker..." -ForegroundColor Green
docker ps *>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "      ERROR: Docker is not running!" -ForegroundColor Red
    Write-Host "      Please start Docker Desktop and try again." -ForegroundColor Yellow
    exit 1
}
Write-Host "      ✓ Docker is running" -ForegroundColor Gray

# Clean up existing containers if requested
if ($Clean) {
    Write-Host "[2/8] Cleaning up existing containers..." -ForegroundColor Green
    docker-compose down -v
    Write-Host "      ✓ Cleanup complete" -ForegroundColor Gray
} else {
    Write-Host "[2/8] Skipping cleanup (use -Clean to remove existing containers)" -ForegroundColor Gray
}

# Select compose file
$composeFile = if ($Minimal) { "docker-compose-minimal.yml" } else { "docker-compose.yml" }
Write-Host "[3/8] Using configuration: $composeFile" -ForegroundColor Green

if (!(Test-Path $composeFile)) {
    Write-Host "      ERROR: $composeFile not found!" -ForegroundColor Red
    exit 1
}
Write-Host "      ✓ Found $composeFile" -ForegroundColor Gray

# Create required directories
Write-Host "[4/8] Creating directories..." -ForegroundColor Green
$directories = @(
    "docker", "notebooks", "models", 
    "airflow", "airflow\dags", "airflow\logs", "airflow\plugins",
    "data\raw\users", "data\raw\products", "data\raw\transactions", "data\raw\clickstream",
    "data\clean", "data\warehouse", "data\analytics"
)
foreach ($dir in $directories) {
    if (!(Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Host "      Created: $dir" -ForegroundColor Gray
    }
}

# Build images if requested
if ($Rebuild) {
    Write-Host "[5/8] Building Docker images..." -ForegroundColor Green
    Write-Host "      This may take 10-15 minutes..." -ForegroundColor Yellow
    docker-compose -f $composeFile build --no-cache
    Write-Host "      ✓ Build complete" -ForegroundColor Gray
} else {
    Write-Host "[5/8] Using existing images (use -Rebuild to force rebuild)" -ForegroundColor Gray
}

# Start all containers
Write-Host "[6/8] Starting containers..." -ForegroundColor Green
Write-Host "      This may take 5-10 minutes on first run..." -ForegroundColor Yellow
docker-compose -f $composeFile up -d

# Wait for services to be ready
Write-Host "[7/8] Waiting for services to start..." -ForegroundColor Green
Start-Sleep -Seconds 30
Write-Host "      ✓ Services started" -ForegroundColor Gray

# Initialize data
Write-Host "[8/8] Generating synthetic data..." -ForegroundColor Green
Write-Host "      This may take 2-5 minutes..." -ForegroundColor Yellow

# Check if data generator is running
$dataGenStatus = docker ps --filter "name=ecommerce-data-generator" --format "{{.Status}}"
if ($dataGenStatus) {
    Write-Host "      ✓ Data generator is running" -ForegroundColor Gray
    Write-Host "      Monitor progress: docker logs -f ecommerce-data-generator" -ForegroundColor Yellow
} else {
    Write-Host "      ⚠ Data generator not started (minimal mode or error)" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "  🎉 PLATFORM READY!" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "📊 WEB INTERFACES:" -ForegroundColor Yellow
Write-Host "  ├─ Hadoop NameNode:      http://localhost:9870" -ForegroundColor Cyan
Write-Host "  ├─ Spark Master UI:      http://localhost:8080" -ForegroundColor Cyan
Write-Host "  ├─ Spark Worker UI:      http://localhost:8081" -ForegroundColor Cyan
Write-Host "  ├─ Airflow Dashboard:    http://localhost:8082 (admin/admin123)" -ForegroundColor Cyan
Write-Host "  ├─ HiveServer2 UI:       http://localhost:10002" -ForegroundColor Cyan
Write-Host "  ├─ Jupyter Lab:          http://localhost:8888" -ForegroundColor Cyan
Write-Host "  └─ Streamlit Dashboard:  http://localhost:8501" -ForegroundColor Cyan
Write-Host ""
Write-Host "🛠️  USEFUL COMMANDS:" -ForegroundColor Yellow
Write-Host "  Check status:         docker-compose -f $composeFile ps" -ForegroundColor Gray
Write-Host "  View logs:            docker-compose -f $composeFile logs -f [service]" -ForegroundColor Gray
Write-Host "  Generate data:        docker exec ecommerce-data-generator python run_all_generators.py" -ForegroundColor Gray
Write-Host "  Run Spark job:        docker exec ecommerce-spark-master spark-submit /scripts/spark/[script].py" -ForegroundColor Gray
Write-Host "  Access Spark shell:   docker exec -it ecommerce-spark-master bash" -ForegroundColor Gray
Write-Host "  Stop all:             docker-compose -f $composeFile down" -ForegroundColor Gray
Write-Host "  Clean everything:     docker-compose -f $composeFile down -v" -ForegroundColor Gray
Write-Host ""
Write-Host "📚 NEXT STEPS:" -ForegroundColor Yellow
Write-Host "  1. Wait for data generation to complete (check logs)" -ForegroundColor White
Write-Host "  2. Open Streamlit dashboard at http://localhost:8501" -ForegroundColor White
Write-Host "  3. Train ML models: docker exec ecommerce-spark-master spark-submit /scripts/ml/train_recommendations.py" -ForegroundColor White
Write-Host "  4. Explore Jupyter notebooks at http://localhost:8888" -ForegroundColor White
Write-Host ""
Write-Host "📖 Documentation: See DOCKER_GUIDE.md for detailed instructions" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Cyan

# Show running containers
Write-Host ""
Write-Host "📦 RUNNING CONTAINERS:" -ForegroundColor Yellow
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' --filter 'name=ecommerce'
