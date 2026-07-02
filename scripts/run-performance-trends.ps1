param(
    [string]$RunsRoot,
    [string]$BaselineRoot,
    [switch]$PauseOnExit
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
if (-not $RunsRoot) {
    $RunsRoot = Join-Path $repoRoot "artifacts\performance\runs"
}
if (-not $BaselineRoot) {
    $BaselineRoot = Join-Path $repoRoot "fixtures\performance\baselines"
}

$trendDir = Join-Path $repoRoot "artifacts\performance\trends"
$trendJsonPath = Join-Path $trendDir "latest.json"
$trendReportPath = Join-Path $trendDir "latest.md"

function Close-Runner([int]$ExitCode) {
    if ($PauseOnExit) {
        Write-Host ""
        Read-Host "Press Enter to close"
    }
    exit $ExitCode
}

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargo) {
    Write-Host "Rust is not installed or cargo is not on PATH." -ForegroundColor Red
    Close-Runner 1
}

if (-not (Test-Path $RunsRoot)) {
    Write-Host "No performance run directory exists at $RunsRoot." -ForegroundColor Red
    Close-Runner 1
}

$bundlePaths = @(Get-ChildItem -Path $RunsRoot -Recurse -Filter bundle.json -File | Sort-Object FullName | Select-Object -ExpandProperty FullName)
if ($bundlePaths.Count -eq 0) {
    Write-Host "No performance bundle.json files were found under $RunsRoot." -ForegroundColor Red
    Close-Runner 1
}

$baselineArgs = @()
if (Test-Path $BaselineRoot) {
    $baselinePaths = @(Get-ChildItem -Path $BaselineRoot -Recurse -Filter *.json -File | Sort-Object FullName | Select-Object -ExpandProperty FullName)
    foreach ($baselinePath in $baselinePaths) {
        $baselineArgs += @("--baseline", $baselinePath)
    }
}

New-Item -ItemType Directory -Force -Path $trendDir | Out-Null

$arguments = @(
    "run", "-p", "aether_api", "--example", "performance_trend_report", "--release", "--",
    "--output-json", $trendJsonPath,
    "--output-report", $trendReportPath
) + $baselineArgs + $bundlePaths

Write-Host ""
Write-Host "AETHER Performance Trend Runner"
Write-Host "=============================="
Write-Host "Bundles:   $($bundlePaths.Count)"
Write-Host "Baselines: $($baselineArgs.Count / 2)"
Write-Host "Output:    $trendReportPath"
Write-Host ""

& $cargo.Source @arguments
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Write-Host ""
    Write-Host "Trend report: $trendReportPath" -ForegroundColor Green
    Write-Host "Trend json:   $trendJsonPath"
} else {
    Write-Host "Trend report failed." -ForegroundColor Red
}

Close-Runner $exitCode
