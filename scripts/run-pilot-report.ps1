param(
    [switch]$PauseOnExit
)

$ErrorActionPreference = "Stop"

$commandText = "cargo run -p aether_api --example pilot_coordination_report --release"

function Close-Runner([int]$ExitCode) {
    if ($PauseOnExit) {
        Write-Host ""
        Read-Host "Press Enter to close"
    }
    exit $ExitCode
}

Write-Host ""
Write-Host "AETHER Pilot Report Runner"
Write-Host "=========================="
Write-Host "Running: $commandText"
Write-Host ""

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargo) {
    Write-Host "Rust is not installed or cargo is not on PATH." -ForegroundColor Red
    Write-Host "Ask the platform team to restore the AETHER Rust toolchain before running pilot reports."
    Close-Runner 1
}

& cmd.exe /d /c $commandText
$exitCode = $LASTEXITCODE

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "Pilot report completed successfully." -ForegroundColor Green
    Write-Host "Artifacts: artifacts\pilot\reports\latest.md and latest.json"
} else {
    Write-Host "Pilot report failed." -ForegroundColor Red
}

Close-Runner $exitCode
