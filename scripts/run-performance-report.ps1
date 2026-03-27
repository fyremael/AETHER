param(
    [string]$Suite = "full_stack",
    [string]$HostManifestPath,
    [switch]$PauseOnExit
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
if (-not $HostManifestPath) {
    $HostManifestPath = Join-Path $repoRoot "fixtures\performance\hosts\dev-chad-windows-native.json"
}
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$outputTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$hostManifest = Get-Content -Path $HostManifestPath | ConvertFrom-Json
$hostId = $hostManifest.host_id
$runDir = Join-Path $repoRoot ("artifacts\performance\runs\{0}-{1}-{2}" -f $outputTimestamp, $Suite, $hostId)
$bundlePath = Join-Path $runDir "bundle.json"
$reportPath = Join-Path $runDir "report.md"
$latestBundlePath = Join-Path $repoRoot "artifacts\performance\latest.json"
$latestReportPath = Join-Path $repoRoot "artifacts\performance\latest.md"

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
Write-Host "Suite:   $Suite"
Write-Host "Host:    $hostId"
Write-Host ""

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargo) {
    Write-Host "Rust is not installed or cargo is not on PATH." -ForegroundColor Red
    Close-Runner 1
}

New-Item -ItemType Directory -Force $runDir | Out-Null
New-Item -ItemType Directory -Force (Split-Path -Parent $latestBundlePath) | Out-Null

$arguments = @(
    "run", "-p", "aether_api", "--example", "performance_report", "--release", "--",
    "--suite", $Suite,
    "--host-manifest", (Resolve-Path $HostManifestPath).Path,
    "--bundle-path", $bundlePath,
    "--report-path", $reportPath
)

Write-Host "Running: cargo $($arguments -join ' ')"
Write-Host "Bundle:  $bundlePath"
Write-Host "Report:  $reportPath"
Write-Host ""

& $cargo.Source @arguments
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Copy-Item -Force $bundlePath $latestBundlePath
    Copy-Item -Force $reportPath $latestReportPath
}

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "Performance report completed successfully." -ForegroundColor Green
    Write-Host "Latest bundle: $latestBundlePath"
    Write-Host "Latest report: $latestReportPath"
} else {
    Write-Host "Performance report failed." -ForegroundColor Red
}

Close-Runner $exitCode
