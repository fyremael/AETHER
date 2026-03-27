param(
    [string]$Suite = "core_kernel",
    [string]$HostManifestPath,
    [string]$OutputPath,
    [switch]$PauseOnExit
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
if (-not $HostManifestPath) {
    $HostManifestPath = Join-Path $repoRoot "fixtures\performance\hosts\dev-chad-windows-native.json"
}
$hostManifest = Get-Content -Path $HostManifestPath | ConvertFrom-Json
$hostId = $hostManifest.host_id
if (-not $OutputPath) {
    $OutputPath = Join-Path $repoRoot ("artifacts\performance\baselines\{0}\{1}.json" -f $Suite, $hostId)
}

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
Write-Host "Suite:  $Suite"
Write-Host "Host:   $hostId"
Write-Host "Output: $OutputPath"
Write-Host ""

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargo) {
    Write-Host "Rust is not installed or cargo is not on PATH." -ForegroundColor Red
    Close-Runner 1
}

$arguments = @(
    "run", "-p", "aether_api", "--example", "capture_performance_baseline", "--release", "--",
    "--suite", $Suite,
    "--host-manifest", (Resolve-Path $HostManifestPath).Path,
    "--output", $OutputPath
)

& $cargo.Source @arguments
$exitCode = $LASTEXITCODE

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "Performance baseline captured successfully." -ForegroundColor Green
} else {
    Write-Host "Performance baseline capture failed." -ForegroundColor Red
}

Close-Runner $exitCode
