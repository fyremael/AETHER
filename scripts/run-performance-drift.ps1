param(
    [switch]$PauseOnExit
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$outputTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$reportDir = Join-Path $repoRoot "artifacts\performance"
$baselinePath = Join-Path $reportDir "baseline.json"
$reportPath = Join-Path $reportDir "performance-drift-$outputTimestamp.md"
$latestPath = Join-Path $reportDir "latest-drift.md"
$commandText = "cargo run -p aether_api --example performance_drift_report --release -- $baselinePath"

function Close-Runner([int]$ExitCode) {
    if ($PauseOnExit) {
        Write-Host ""
        Read-Host "Press Enter to close"
    }
    exit $ExitCode
}

Write-Host ""
Write-Host "AETHER Performance Drift Runner"
Write-Host "==============================="
Write-Host "Started: $timestamp"
Write-Host ""

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargo) {
    Write-Host "Rust is not installed or cargo is not on PATH." -ForegroundColor Red
    Write-Host "Ask the platform team to restore the AETHER Rust toolchain before running drift checks."
    Close-Runner 1
}

if (-not (Test-Path $baselinePath)) {
    Write-Host "No performance baseline exists yet." -ForegroundColor Yellow
    Write-Host "Run scripts/run-performance-baseline.cmd first."
    Close-Runner 1
}

New-Item -ItemType Directory -Force $reportDir | Out-Null

Write-Host "Running: $commandText"
Write-Host "Baseline: $baselinePath"
Write-Host "Report:   $reportPath"
Write-Host ""

$outputLines = & cmd.exe /d /c "$commandText 2>&1" | Tee-Object -Variable outputLines
$exitCode = $LASTEXITCODE

$report = @(
    "<!-- AETHER performance drift capture -->"
    ""
    "> Generated: $timestamp"
    "> Repository: $repoRoot"
    "> Baseline: $baselinePath"
    "> Command: $commandText"
    ""
) + ($outputLines | ForEach-Object { $_.ToString() })

Set-Content -Path $reportPath -Value $report
Set-Content -Path $latestPath -Value $report

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "Performance drift report completed successfully." -ForegroundColor Green
} else {
    Write-Host "Performance drift report failed or detected a fail-level regression." -ForegroundColor Red
}
Write-Host "Report: $reportPath"
Write-Host "Latest: $latestPath"

Close-Runner $exitCode
