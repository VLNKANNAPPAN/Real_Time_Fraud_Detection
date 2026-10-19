param(
    [switch]$SkipInfrastructure
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot
$Python = (Resolve-Path '.venv/Scripts/python.exe').Path
$TempDirectory = Join-Path $ProjectRoot '.tmp'
New-Item -ItemType Directory -Force -Path $TempDirectory | Out-Null

if (-not $SkipInfrastructure) {
    docker compose up -d --wait postgres redis kafka
    & $Python -m alembic upgrade head
    & (Resolve-Path '.venv/Scripts/fraud-init-topics.exe').Path
}

function Start-DemoProcess {
    param([string]$Name, [string[]]$Arguments)
    $PidFile = Join-Path $TempDirectory "$Name.pid"
    if (Test-Path $PidFile) {
        $ExistingId = [int](Get-Content $PidFile -Raw)
        $Existing = Get-Process -Id $ExistingId -ErrorAction SilentlyContinue
        if ($Existing -and $Existing.Path -eq $Python) {
            Write-Host "$Name already running as PID $ExistingId"
            return
        }
    }
    $Process = Start-Process -FilePath $Python -ArgumentList $Arguments `
        -WorkingDirectory $ProjectRoot -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $TempDirectory "$Name.out.log") `
        -RedirectStandardError (Join-Path $TempDirectory "$Name.err.log") -PassThru
    Set-Content -Path $PidFile -Value $Process.Id
    Write-Host "Started $Name as PID $($Process.Id)"
}

Start-DemoProcess 'processor' @('-m', 'fraud_engine.processor_cli')
Start-DemoProcess 'api' @('-m', 'uvicorn', 'fraud_engine.api:app', '--host', '127.0.0.1', '--port', '8000')
Start-DemoProcess 'dashboard' @(
    '-m', 'streamlit', 'run', 'src/fraud_engine/dashboard.py',
    '--server.address', '127.0.0.1', '--server.port', '8501', '--server.headless', 'true'
)

Write-Host 'API docs:  http://127.0.0.1:8000/docs'
Write-Host 'Dashboard: http://127.0.0.1:8501'
