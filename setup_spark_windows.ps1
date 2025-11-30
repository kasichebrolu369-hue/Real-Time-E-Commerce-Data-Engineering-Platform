# ============================================================================
# Spark Setup for Windows
# ============================================================================
# This script sets up PySpark to work on Windows without full Hadoop install
# ============================================================================

Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("="*79) -ForegroundColor Cyan
Write-Host "  SPARK SETUP FOR WINDOWS" -ForegroundColor Yellow
Write-Host ("="*80) -ForegroundColor Cyan
Write-Host ""

$projectRoot = $PSScriptRoot
$hadoopHome = Join-Path $projectRoot "hadoop_home"
$binDir = Join-Path $hadoopHome "bin"

# Create hadoop_home directory structure
Write-Host "[1/4] Creating Hadoop home directory..." -ForegroundColor Green
if (!(Test-Path $hadoopHome)) {
    New-Item -ItemType Directory -Path $hadoopHome -Force | Out-Null
    New-Item -ItemType Directory -Path $binDir -Force | Out-Null
    Write-Host "      Created: $hadoopHome" -ForegroundColor Gray
} else {
    Write-Host "      Already exists: $hadoopHome" -ForegroundColor Gray
}

# Download winutils.exe if not present
Write-Host "[2/4] Checking for winutils.exe..." -ForegroundColor Green
$winutilsPath = Join-Path $binDir "winutils.exe"

if (!(Test-Path $winutilsPath)) {
    Write-Host "      Downloading winutils.exe..." -ForegroundColor Yellow
    $winutilsUrl = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe"
    
    try {
        # Download winutils.exe
        Invoke-WebRequest -Uri $winutilsUrl -OutFile $winutilsPath -UseBasicParsing
        Write-Host "      Downloaded: $winutilsPath" -ForegroundColor Gray
    } catch {
        Write-Host "      Could not download winutils.exe automatically" -ForegroundColor Red
        Write-Host "      Please download manually from:" -ForegroundColor Yellow
        Write-Host "      $winutilsUrl" -ForegroundColor Cyan
        Write-Host "      And place it in: $binDir" -ForegroundColor Cyan
    }
} else {
    Write-Host "      Found: $winutilsPath" -ForegroundColor Gray
}

# Download hadoop.dll if not present
Write-Host "[3/4] Checking for hadoop.dll..." -ForegroundColor Green
$hadoopDllPath = Join-Path $binDir "hadoop.dll"

if (!(Test-Path $hadoopDllPath)) {
    Write-Host "      Downloading hadoop.dll..." -ForegroundColor Yellow
    $hadoopDllUrl = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll"
    
    try {
        Invoke-WebRequest -Uri $hadoopDllUrl -OutFile $hadoopDllPath -UseBasicParsing
        Write-Host "      Downloaded: $hadoopDllPath" -ForegroundColor Gray
    } catch {
        Write-Host "      Could not download hadoop.dll automatically" -ForegroundColor Red
        Write-Host "      Please download manually from:" -ForegroundColor Yellow
        Write-Host "      $hadoopDllUrl" -ForegroundColor Cyan
    }
} else {
    Write-Host "      Found: $hadoopDllPath" -ForegroundColor Gray
}

# Set environment variables
Write-Host "[4/4] Setting environment variables..." -ForegroundColor Green
$env:HADOOP_HOME = $hadoopHome
$env:SPARK_HOME = $hadoopHome  # Point to same location
$env:PYSPARK_PYTHON = "python"
$env:PYSPARK_DRIVER_PYTHON = "python"

Write-Host "      HADOOP_HOME = $hadoopHome" -ForegroundColor Gray
Write-Host "      SPARK_HOME = $hadoopHome" -ForegroundColor Gray

Write-Host ""
Write-Host ("="*80) -ForegroundColor Cyan
Write-Host "  SETUP COMPLETE!" -ForegroundColor Green
Write-Host ("="*80) -ForegroundColor Cyan
Write-Host ""
Write-Host "To use Spark, run this in your PowerShell session:" -ForegroundColor Yellow
Write-Host "  . .\setup_spark_windows.ps1" -ForegroundColor Cyan
Write-Host ""
Write-Host "Then test with:" -ForegroundColor Yellow
Write-Host "  python -c `"from pyspark.sql import SparkSession; spark = SparkSession.builder.master('local').getOrCreate(); print('Spark works!'); spark.stop()`"" -ForegroundColor Cyan
Write-Host ""
