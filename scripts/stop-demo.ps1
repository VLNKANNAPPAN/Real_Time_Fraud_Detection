param(
    [switch]$StopInfrastructure
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot
$Python = (Resolve-Path '.venv/Scripts/python.exe').Path

foreach ($Name in @('processor', 'api', 'dashboard')) {
    $PidFile = Join-Path $ProjectRoot ".tmp/$Name.pid"
    if (-not (Test-Path $PidFile)) { continue }
    $ProcessId = [int](Get-Content $PidFile -Raw)
    $Process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    if ($Process -and $Process.Path -eq $Python) {
        Stop-Process -Id $ProcessId
        Write-Host "Stopped $Name PID $ProcessId"
    }
    Remove-Item -LiteralPath $PidFile -Force
}

if ($StopInfrastructure) {
    docker compose stop
}
