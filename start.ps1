# External Data Manager - Start All Services
$root = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host ""
Write-Host "  Starting External Data Manager..." -ForegroundColor Cyan
Write-Host ""

# 0. Kill any existing processes
Write-Host "  [0/4] Stopping old processes..." -ForegroundColor Yellow
Get-Process python* -ErrorAction SilentlyContinue | Stop-Process -Force
Get-Process node*   -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 1
Write-Host "        OK" -ForegroundColor Green

# 1. Docker (PostgreSQL)
Write-Host "  [1/4] PostgreSQL..." -ForegroundColor Yellow
docker compose --project-directory $root up -d 2>&1 | Out-Null
Write-Host "        OK" -ForegroundColor Green

# 2. Pipeline
Write-Host "  [2/4] Pipeline (main.py)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$root'; python main.py" -WindowStyle Normal
Write-Host "        OK" -ForegroundColor Green

# 3. FastAPI
Write-Host "  [3/4] FastAPI (run_api.py)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$root'; python run_api.py" -WindowStyle Normal
Write-Host "        OK" -ForegroundColor Green

# 4. Vite
Write-Host "  [4/4] React frontend (npm run dev)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$root\frontend'; npm run dev" -WindowStyle Normal
Write-Host "        OK" -ForegroundColor Green

Write-Host ""
Write-Host "  All services started!" -ForegroundColor Green
Write-Host ""
Write-Host "  Frontend  ->  http://localhost:5173" -ForegroundColor Cyan
Write-Host "  API       ->  http://localhost:8000" -ForegroundColor Cyan
Write-Host "  Adminer   ->  http://localhost:8080" -ForegroundColor Cyan
Write-Host ""
