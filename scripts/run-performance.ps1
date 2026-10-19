param([int]$Events = 50)

$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot
$Python = (Resolve-Path '.venv/Scripts/python.exe').Path

foreach ($Scenario in @('normal', 'burst', 'slow_model', 'slow_db', 'slow_redis')) {
    & $Python -m fraud_engine.load_test --scenario $Scenario --events $Events `
        --output "artifacts/performance/$Scenario.json"
}
