param(
    [ValidateSet("01")]
    [string]$Demo = "01",
    [switch]$PauseOnExit
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$demoMap = @{
    "01" = @{
        Title = "Temporal Dependency Horizon"
        Crate = "aether_runtime"
        Example = "demo_01_temporal_dependency_horizon"
        Narrative = Join-Path $repoRoot "examples\demo-01-temporal-dependency-horizon.md"
    }
}

$selectedDemo = $demoMap[$Demo]
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$outputTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$reportDir = Join-Path $repoRoot "artifacts\demos\demo-$Demo"
$reportPath = Join-Path $reportDir "demo-$Demo-$outputTimestamp.txt"
$latestPath = Join-Path $reportDir "latest.txt"

function Close-Runner([int]$ExitCode) {
    if ($PauseOnExit) {
        Write-Host ""
        Read-Host "Press Enter to close"
    }
    exit $ExitCode
}

Write-Host ""
Write-Host "AETHER Demonstration Runner"
Write-Host "=========================="
Write-Host "Demo ${Demo}: $($selectedDemo.Title)"
Write-Host "Started: $timestamp"
Write-Host ""

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargo) {
    Write-Host "Rust is not installed or cargo is not on PATH." -ForegroundColor Red
    Write-Host "Ask the platform team to install the AETHER Rust toolchain before running demonstrations."
    Close-Runner 1
}

New-Item -ItemType Directory -Force $reportDir | Out-Null

$commandText = "cargo run -p $($selectedDemo.Crate) --example $($selectedDemo.Example)"
Write-Host "What this demo shows:"
Write-Host "  - append-only journal replay"
Write-Host "  - recursive rule compilation"
Write-Host "  - different semantic results at different points in time"
Write-Host ""
Write-Host "Running: $commandText"
Write-Host "A report will be saved to:"
Write-Host "  $reportPath"
Write-Host ""

$outputLines = & cmd.exe /d /c "$commandText 2>&1" |
    Tee-Object -Variable outputLines
$exitCode = $LASTEXITCODE

$report = @(
    "AETHER Demonstration Runner"
    "Demo: $Demo - $($selectedDemo.Title)"
    "Started: $timestamp"
    "Repository: $repoRoot"
    "Command: $commandText"
    "Narrative: $($selectedDemo.Narrative)"
    ""
    "---- Captured Output ----"
) + ($outputLines | ForEach-Object { $_.ToString() })

Set-Content -Path $reportPath -Value $report
Set-Content -Path $latestPath -Value $report

Write-Host ""
foreach ($line in $outputLines) {
    Write-Host $line
}
Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "Demo completed successfully." -ForegroundColor Green
} else {
    Write-Host "Demo failed." -ForegroundColor Red
}
Write-Host "Report: $reportPath"
Write-Host "Latest: $latestPath"
Write-Host "Narrative: $($selectedDemo.Narrative)"

Close-Runner $exitCode
