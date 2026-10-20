# External Data Manager - Stop All Services
$root = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host ""
Write-Host "  Stopping External Data Manager..." -ForegroundColor Cyan
Write-Host ""

Write-Host "  Stopping Python processes..." -ForegroundColor Yellow
Get-Process python* -ErrorAction SilentlyContinue | Stop-Process -Force
Write-Host "  Stopping Node processes..." -ForegroundColor Yellow
Get-Process node* -ErrorAction SilentlyContinue | Stop-Process -Force
Write-Host "  Stopping PostgreSQL (Docker)..." -ForegroundColor Yellow
docker compose --project-directory $root down 2>&1 | Out-Null

Write-Host ""
Write-Host "  All services stopped." -ForegroundColor Green
Write-Host ""
