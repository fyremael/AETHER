param(
    [switch]$PauseOnExit
)

$ErrorActionPreference = "Stop"

$commandText = "cargo run -p aether_api --example capture_performance_baseline --release"

function Close-Runner([int]$ExitCode) {
    if ($PauseOnExit) {
        Write-Host ""
        Read-Host "Press Enter to close"
    }
    exit $ExitCode
}

Write-Host ""
Write-Host "AETHER Performance Baseline Runner"
Write-Host "=================================="
Write-Host "Running: $commandText"
Write-Host ""

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargo) {
    Write-Host "Rust is not installed or cargo is not on PATH." -ForegroundColor Red
    Write-Host "Ask the platform team to restore the AETHER Rust toolchain before capturing a baseline."
    Close-Runner 1
}

& cmd.exe /d /c $commandText
$exitCode = $LASTEXITCODE

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "Performance baseline captured successfully." -ForegroundColor Green
    Write-Host "Baseline: artifacts\performance\baseline.json"
} else {
    Write-Host "Performance baseline capture failed." -ForegroundColor Red
}

Close-Runner $exitCode
