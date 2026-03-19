param(
    [switch]$PauseOnExit
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$outputTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$reportDir = Join-Path $repoRoot "artifacts\performance"
$reportPath = Join-Path $reportDir "performance-report-$outputTimestamp.md"
$latestPath = Join-Path $reportDir "latest.md"
$commandText = "cargo run -p aether_api --example performance_report --release"

function Close-Runner([int]$ExitCode) {
    if ($PauseOnExit) {
        Write-Host ""
        Read-Host "Press Enter to close"
    }
    exit $ExitCode
}

Write-Host ""
Write-Host "AETHER Performance Runner"
Write-Host "========================"
Write-Host "Started: $timestamp"
Write-Host ""

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargo) {
    Write-Host "Rust is not installed or cargo is not on PATH." -ForegroundColor Red
    Write-Host "Ask the platform team to restore the AETHER Rust toolchain before running performance reports."
    Close-Runner 1
}

New-Item -ItemType Directory -Force $reportDir | Out-Null

Write-Host "This runner will:"
Write-Host "  - execute the release-mode performance report example"
Write-Host "  - save a timestamped markdown report"
Write-Host "  - refresh artifacts\performance\latest.md"
Write-Host ""
Write-Host "Running: $commandText"
Write-Host "Report:  $reportPath"
Write-Host ""

$outputLines = & cmd.exe /d /c "$commandText 2>&1" | Tee-Object -Variable outputLines
$exitCode = $LASTEXITCODE

$report = @(
    "<!-- AETHER performance capture -->"
    ""
    "> Generated: $timestamp"
    "> Repository: $repoRoot"
    "> Command: $commandText"
    ""
) + ($outputLines | ForEach-Object { $_.ToString() })

Set-Content -Path $reportPath -Value $report
Set-Content -Path $latestPath -Value $report

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "Performance report completed successfully." -ForegroundColor Green
} else {
    Write-Host "Performance report failed." -ForegroundColor Red
}
Write-Host "Report: $reportPath"
Write-Host "Latest: $latestPath"
Write-Host ""
Write-Host "For the full suite, also run:"
Write-Host "  cargo bench -p aether_api"
Write-Host "  cargo test -p aether_api --test performance_stress --release -- --ignored --nocapture"

Close-Runner $exitCode
