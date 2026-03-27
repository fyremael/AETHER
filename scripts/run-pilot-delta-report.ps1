param(
    [string]$DatabasePath = "artifacts/pilot/coordination.sqlite"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
Push-Location $repoRoot
try {
    cargo run -p aether_api --example pilot_coordination_delta_report --release -- $DatabasePath
} finally {
    Pop-Location
}
