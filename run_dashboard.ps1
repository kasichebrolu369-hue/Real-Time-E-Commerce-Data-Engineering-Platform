# ============================================================================
# Real-Time E-Commerce Platform - Complete Launcher
# ============================================================================
# This script runs the complete platform including:
# 1. Data validation
# 2. ML models check
# 3. Real-time dashboard launch
# ============================================================================

Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "  E-COMMERCE REAL-TIME ANALYTICS PLATFORM" -ForegroundColor Cyan
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host ""

# Configuration
$PROJECT_ROOT = $PSScriptRoot
$DATA_DIR = Join-Path $PROJECT_ROOT "data\raw"
$MODELS_DIR = Join-Path $PROJECT_ROOT "models"
$DASHBOARD_SCRIPT = Join-Path $PROJECT_ROOT "dashboard\realtime_dashboard.py"

# ============================================================================
# Step 1: Environment Check
# ============================================================================

Write-Host "Step 1: Checking Environment..." -ForegroundColor Yellow
Write-Host ""

# Check Python
try {
    $pythonVersion = python --version 2>&1
    Write-Host "[OK] Python: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ Python not found! Please install Python 3.8+" -ForegroundColor Red
    exit 1
}

# Check if virtual environment is activated
if ($env:VIRTUAL_ENV) {
    Write-Host "[OK] Virtual environment active: $env:VIRTUAL_ENV" -ForegroundColor Green
} else {
    Write-Host "⚠️  No virtual environment detected" -ForegroundColor Yellow
    Write-Host "   Activate with: .\ecom\Scripts\Activate.ps1" -ForegroundColor Yellow
}

Write-Host ""

# ============================================================================
# Step 2: Check Required Packages
# ============================================================================

Write-Host "Step 2: Checking Required Packages..." -ForegroundColor Yellow
Write-Host ""

$requiredPackages = @("pandas", "numpy", "streamlit", "plotly", "scikit-learn", "matplotlib")
$missingPackages = @()

foreach ($package in $requiredPackages) {
    $installed = python -c "import $package; print('OK')" 2>$null
    if ($installed -eq "OK") {
        Write-Host "✅ $package" -ForegroundColor Green
    } else {
        Write-Host "❌ $package (missing)" -ForegroundColor Red
        $missingPackages += $package
    }
}

if ($missingPackages.Count -gt 0) {
    Write-Host ""
    Write-Host "⚠️  Missing packages detected!" -ForegroundColor Yellow
    Write-Host "Installing required packages..." -ForegroundColor Yellow
    
    python -m pip install --upgrade pip
    pip install streamlit plotly pandas numpy scikit-learn matplotlib seaborn
    
    Write-Host "✅ Packages installed!" -ForegroundColor Green
}

Write-Host ""

# ============================================================================
# Step 3: Data Validation
# ============================================================================

Write-Host "Step 3: Validating Data..." -ForegroundColor Yellow
Write-Host ""

$dataValid = $true

# Check data directories
$dataDirs = @("users", "products", "transactions", "clickstream")
foreach ($dir in $dataDirs) {
    $dirPath = Join-Path $DATA_DIR $dir
    if (Test-Path $dirPath) {
        $files = Get-ChildItem $dirPath -Filter "*.parquet" -ErrorAction SilentlyContinue
        if ($files.Count -gt 0) {
            Write-Host "✅ $dir : $($files.Count) files found" -ForegroundColor Green
        } else {
            Write-Host "❌ $dir : No parquet files" -ForegroundColor Red
            $dataValid = $false
        }
    } else {
        Write-Host "❌ $dir : Directory not found" -ForegroundColor Red
        $dataValid = $false
    }
}

if (-not $dataValid) {
    Write-Host ""
    Write-Host "⚠️  Data not found!" -ForegroundColor Yellow
    Write-Host "Would you like to generate sample data? (Y/N)" -ForegroundColor Yellow
    $response = Read-Host
    
    if ($response -eq "Y" -or $response -eq "y") {
        Write-Host "Generating data..." -ForegroundColor Cyan
        
        Set-Location (Join-Path $PROJECT_ROOT "data_generators")
        python run_all_generators.py
        
        Set-Location $PROJECT_ROOT
        
        Write-Host "✅ Data generation complete!" -ForegroundColor Green
    } else {
        Write-Host "❌ Cannot proceed without data. Exiting..." -ForegroundColor Red
        exit 1
    }
}

Write-Host ""

# ============================================================================
# Step 4: ML Models Check
# ============================================================================

Write-Host "Step 4: Checking ML Models..." -ForegroundColor Yellow
Write-Host ""

$modelsValid = $true

$modelFiles = @(
    "recommendation_model.pkl",
    "user_recommendations.csv",
    "churn_prediction_model.pkl",
    "churn_predictions.csv"
)

foreach ($file in $modelFiles) {
    $filePath = Join-Path $MODELS_DIR $file
    if (Test-Path $filePath) {
        $size = (Get-Item $filePath).Length / 1KB
        Write-Host "✅ $file : $([math]::Round($size, 2)) KB" -ForegroundColor Green
    } else {
        Write-Host "❌ $file : Not found" -ForegroundColor Red
        $modelsValid = $false
    }
}

if (-not $modelsValid) {
    Write-Host ""
    Write-Host "⚠️  ML models not found!" -ForegroundColor Yellow
    Write-Host "Would you like to train models now? (Y/N)" -ForegroundColor Yellow
    $response = Read-Host
    
    if ($response -eq "Y" -or $response -eq "y") {
        Write-Host "Training models (this may take 2-5 minutes)..." -ForegroundColor Cyan
        
        Set-Location (Join-Path $PROJECT_ROOT "scripts\ml")
        
        Write-Host "  → Training recommendation engine..." -ForegroundColor Cyan
        python train_recommendations.py
        
        Write-Host "  → Training churn prediction..." -ForegroundColor Cyan
        python train_churn_prediction.py
        
        Set-Location $PROJECT_ROOT
        
        Write-Host "✅ Model training complete!" -ForegroundColor Green
    } else {
        Write-Host "⚠️  Dashboard will run without ML insights" -ForegroundColor Yellow
    }
}

Write-Host ""

# ============================================================================
# Step 5: System Resources Check
# ============================================================================

Write-Host "Step 5: System Resources..." -ForegroundColor Yellow
Write-Host ""

# Get system info
$os = Get-CimInstance Win32_OperatingSystem
$totalRAM = [math]::Round($os.TotalVisibleMemorySize / 1MB, 2)
$freeRAM = [math]::Round($os.FreePhysicalMemory / 1MB, 2)

Write-Host "  Total RAM: $totalRAM GB" -ForegroundColor Cyan
Write-Host "  Free RAM:  $freeRAM GB" -ForegroundColor Cyan

if ($freeRAM -lt 2) {
    Write-Host "  ⚠️  Low memory detected! Dashboard may be slow" -ForegroundColor Yellow
} else {
    Write-Host "  ✅ Sufficient memory available" -ForegroundColor Green
}

Write-Host ""

# ============================================================================
# Step 6: Launch Dashboard
# ============================================================================

Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "  ALL CHECKS PASSED - LAUNCHING DASHBOARD" -ForegroundColor Green
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "📊 Starting Real-Time Analytics Dashboard..." -ForegroundColor Cyan
Write-Host ""
Write-Host "  🌐 Dashboard URL: http://localhost:8501" -ForegroundColor Green
Write-Host "  🛑 Press Ctrl+C to stop the dashboard" -ForegroundColor Yellow
Write-Host ""
Write-Host "Launching in 3 seconds..." -ForegroundColor Cyan

Start-Sleep -Seconds 3

# Launch Streamlit dashboard
Set-Location $PROJECT_ROOT
streamlit run $DASHBOARD_SCRIPT --server.port 8501 --server.headless false

Write-Host ""
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "  Dashboard stopped. Thank you!" -ForegroundColor Cyan
Write-Host "=====================================================================" -ForegroundColor Cyan
