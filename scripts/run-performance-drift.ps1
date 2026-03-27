param(
    [string]$Suite = "core_kernel",
    [string]$HostManifestPath,
    [string]$BaselinePath,
    [switch]$PauseOnExit
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
if (-not $HostManifestPath) {
    $HostManifestPath = Join-Path $repoRoot "fixtures\performance\hosts\dev-chad-windows-native.json"
}
$hostManifest = Get-Content -Path $HostManifestPath | ConvertFrom-Json
$hostId = $hostManifest.host_id
if (-not $BaselinePath) {
    $BaselinePath = Join-Path $repoRoot ("artifacts\performance\baselines\{0}\{1}.json" -f $Suite, $hostId)
}

$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$outputTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $repoRoot ("artifacts\performance\runs\{0}-{1}-{2}" -f $outputTimestamp, $Suite, $hostId)
$bundlePath = Join-Path $runDir "bundle.json"
$driftPath = Join-Path $runDir "drift.md"
$latestBundlePath = Join-Path $repoRoot "artifacts\performance\latest-drift-bundle.json"
$latestDriftPath = Join-Path $repoRoot "artifacts\performance\latest-drift.md"
$latestSuiteDriftPath = Join-Path $repoRoot ("artifacts\performance\latest-drift-{0}.md" -f $Suite)

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
Write-Host "Started:  $timestamp"
Write-Host "Suite:    $Suite"
Write-Host "Host:     $hostId"
Write-Host "Baseline: $BaselinePath"
Write-Host ""

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargo) {
    Write-Host "Rust is not installed or cargo is not on PATH." -ForegroundColor Red
    Close-Runner 1
}
if (-not (Test-Path $BaselinePath)) {
    Write-Host "No performance baseline exists for $Suite on $hostId." -ForegroundColor Yellow
    Close-Runner 1
}

New-Item -ItemType Directory -Force $runDir | Out-Null

$arguments = @(
    "run", "-p", "aether_api", "--example", "performance_drift_report", "--release", "--",
    "--suite", $Suite,
    "--host-manifest", (Resolve-Path $HostManifestPath).Path,
    "--baseline", (Resolve-Path $BaselinePath).Path,
    "--bundle-path", $bundlePath,
    "--report-path", $driftPath
)

& $cargo.Source @arguments
$exitCode = $LASTEXITCODE

if (Test-Path $bundlePath) {
    Copy-Item -Force $bundlePath $latestBundlePath
}
if (Test-Path $driftPath) {
    Copy-Item -Force $driftPath $latestDriftPath
    Copy-Item -Force $driftPath $latestSuiteDriftPath
}

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "Performance drift report completed successfully." -ForegroundColor Green
} else {
    Write-Host "Performance drift report failed or detected a fail-level regression." -ForegroundColor Red
}
Write-Host "Bundle: $bundlePath"
Write-Host "Drift:  $driftPath"

Close-Runner $exitCode
