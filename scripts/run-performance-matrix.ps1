param(
    [string]$Suite = "full_stack",
    [string]$WindowsHostManifestPath,
    [string]$WslHostManifestPath,
    [switch]$PauseOnExit
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
if (-not $WindowsHostManifestPath) {
    $WindowsHostManifestPath = Join-Path $repoRoot "fixtures\performance\hosts\dev-chad-windows-native.json"
}
if (-not $WslHostManifestPath) {
    $WslHostManifestPath = Join-Path $repoRoot "fixtures\performance\hosts\dev-chad-wsl-ubuntu.json"
}
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$matrixDir = Join-Path $repoRoot "artifacts\performance\matrix"
$runsDir = Join-Path $repoRoot "artifacts\performance\runs"

function Close-Runner([int]$ExitCode) {
    if ($PauseOnExit) {
        Write-Host ""
        Read-Host "Press Enter to close"
    }
    exit $ExitCode
}

function Convert-ToWslPath([string]$WindowsPath) {
    $resolved = [System.IO.Path]::GetFullPath($WindowsPath)
    if ($resolved -match '^([A-Za-z]):\\(.*)$') {
        $drive = $matches[1].ToLower()
        $rest = $matches[2] -replace '\\', '/'
        return "/mnt/$drive/$rest"
    }
    throw "Cannot convert path to WSL form: $WindowsPath"
}

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargo) {
    Write-Host "Rust is not installed or cargo is not on PATH." -ForegroundColor Red
    Close-Runner 1
}

New-Item -ItemType Directory -Force $matrixDir | Out-Null
New-Item -ItemType Directory -Force $runsDir | Out-Null

$windowsHost = Get-Content -Path $WindowsHostManifestPath | ConvertFrom-Json
$windowsBundleDir = Join-Path $runsDir ("{0}-{1}-{2}" -f $timestamp, $Suite, $windowsHost.host_id)
$windowsBundlePath = Join-Path $windowsBundleDir "bundle.json"
$windowsReportPath = Join-Path $windowsBundleDir "report.md"
New-Item -ItemType Directory -Force $windowsBundleDir | Out-Null

Write-Host ""
Write-Host "AETHER Performance Matrix Runner"
Write-Host "==============================="
Write-Host "Suite: $Suite"
Write-Host ""

& $cargo.Source @(
    "run", "-p", "aether_api", "--example", "performance_report", "--release", "--",
    "--suite", $Suite,
    "--host-manifest", (Resolve-Path $WindowsHostManifestPath).Path,
    "--bundle-path", $windowsBundlePath,
    "--report-path", $windowsReportPath
)
if ($LASTEXITCODE -ne 0) {
    Close-Runner $LASTEXITCODE
}

$bundlePaths = [System.Collections.Generic.List[string]]::new()
$bundlePaths.Add($windowsBundlePath)
$unavailableNotes = [System.Collections.Generic.List[string]]::new()

$wsl = Get-Command wsl.exe -ErrorAction SilentlyContinue
if ($wsl) {
    $wslRepoRoot = Convert-ToWslPath $repoRoot
    $wslHostManifest = Get-Content -Path $WslHostManifestPath | ConvertFrom-Json
    $wslBundleDir = Join-Path $runsDir ("{0}-{1}-{2}" -f $timestamp, $Suite, $wslHostManifest.host_id)
    $wslBundlePath = Join-Path $wslBundleDir "bundle.json"
    $wslReportPath = Join-Path $wslBundleDir "report.md"
    New-Item -ItemType Directory -Force $wslBundleDir | Out-Null

    $wslCommand = @(
        "cd '$wslRepoRoot' &&",
        "cargo run -p aether_api --example performance_report --release --",
        "--suite '$Suite'",
        "--host-manifest '$(Convert-ToWslPath $WslHostManifestPath)'",
        "--bundle-path '$(Convert-ToWslPath $wslBundlePath)'",
        "--report-path '$(Convert-ToWslPath $wslReportPath)'"
    ) -join " "

    & $wsl.Source bash -lc $wslCommand
    if ($LASTEXITCODE -eq 0 -and (Test-Path $wslBundlePath)) {
        $bundlePaths.Add($wslBundlePath)
    } else {
        $unavailableNotes.Add("WSL Ubuntu run was unavailable or failed on this host.")
    }
} else {
    $unavailableNotes.Add("WSL is not available on this host.")
}

$matrixJsonPath = Join-Path $matrixDir "latest.json"
$matrixReportPath = Join-Path $matrixDir "latest.md"
$matrixArguments = @(
    "run", "-p", "aether_api", "--example", "performance_matrix_report", "--release", "--",
    "--output-json", $matrixJsonPath,
    "--output-report", $matrixReportPath
) + $bundlePaths
& $cargo.Source @matrixArguments
if ($LASTEXITCODE -ne 0) {
    Close-Runner $LASTEXITCODE
}

if ($unavailableNotes.Count -gt 0) {
    Add-Content -Path $matrixReportPath -Value ""
    Add-Content -Path $matrixReportPath -Value "## Unavailable hosts"
    Add-Content -Path $matrixReportPath -Value ""
    foreach ($note in $unavailableNotes) {
        Add-Content -Path $matrixReportPath -Value "- $note"
    }
}

Write-Host ""
Write-Host "Matrix report: $matrixReportPath" -ForegroundColor Green
Write-Host "Matrix json:   $matrixJsonPath"
Close-Runner 0
