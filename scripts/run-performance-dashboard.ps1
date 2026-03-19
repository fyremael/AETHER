param(
    [switch]$PauseOnExit
)

$ErrorActionPreference = "Stop"

$commandText = "cargo run -p aether_api --example performance_dashboard --release"

function Close-Runner([int]$ExitCode) {
    if ($PauseOnExit) {
        Write-Host ""
        Read-Host "Press Enter to close"
    }
    exit $ExitCode
}

Write-Host ""
Write-Host "AETHER Performance Dashboard"
Write-Host "============================"
Write-Host "Running: $commandText"
Write-Host ""

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargo) {
    Write-Host "Rust is not installed or cargo is not on PATH." -ForegroundColor Red
    Write-Host "Ask the platform team to restore the AETHER Rust toolchain before running the dashboard."
    Close-Runner 1
}

& cmd.exe /d /c $commandText
$exitCode = $LASTEXITCODE

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "Dashboard run completed successfully." -ForegroundColor Green
    Write-Host "For a saved markdown snapshot, run scripts/run-performance-report.cmd"
} else {
    Write-Host "Dashboard run failed." -ForegroundColor Red
}

Close-Runner $exitCode
