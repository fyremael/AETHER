param(
    [string]$HostManifestPath,
    [string]$PerturbationJsonPath,
    [string]$MatrixJsonPath,
    [string]$CapacityInputsJsonPath,
    [switch]$SkipHardening,
    [switch]$PauseOnExit
)

$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
if (-not $HostManifestPath) {
    $HostManifestPath = Join-Path $repoRoot "fixtures\performance\hosts\dev-chad-windows-native.json"
}
if (-not $PerturbationJsonPath) {
    $PerturbationJsonPath = Join-Path $repoRoot "artifacts\performance\perturbation\latest.json"
}
if (-not $MatrixJsonPath) {
    $MatrixJsonPath = Join-Path $repoRoot "artifacts\performance\matrix\latest.json"
}

function Close-Runner([int]$ExitCode) {
    if ($PauseOnExit) {
        Write-Host ""
        Read-Host "Press Enter to close"
    }
    exit $ExitCode
}

function Get-CommandPath([string]$Name) {
    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if (-not $command) {
        throw "Required command not found on PATH: $Name"
    }
    $command.Source
}

$cargo = Get-CommandPath "cargo"
$shell = Get-Command pwsh -ErrorAction SilentlyContinue
if ($shell) {
    $powerShell = $shell.Source
} else {
    $powerShell = Get-CommandPath "powershell"
}

if (-not (Test-Path $PerturbationJsonPath)) {
    Write-Host "Latest perturbation artifact was missing; generating a fresh sweep..." -ForegroundColor Yellow
    $perturbationArgs = @(
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        (Join-Path $repoRoot "scripts\run-perturbation-sweep.ps1"),
        "-HostManifestPath",
        (Resolve-Path $HostManifestPath).Path
    )
    if ($SkipHardening) {
        $perturbationArgs += "-SkipHardening"
    }
    & $powerShell @perturbationArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to generate perturbation prerequisites."
    }
}

if (-not (Test-Path $MatrixJsonPath)) {
    Write-Host "Latest matrix artifact was missing; generating a fresh local matrix..." -ForegroundColor Yellow
    & $powerShell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $repoRoot "scripts\run-performance-matrix.ps1") -Suite full_stack
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to generate performance matrix prerequisites."
    }
}

$perturbation = Get-Content -Path $PerturbationJsonPath -Raw | ConvertFrom-Json -Depth 64
if (-not $CapacityInputsJsonPath -and (-not $perturbation.capacity_inputs -or -not $perturbation.capacity_inputs.json_path)) {
    Write-Host "Latest perturbation artifact predates capacity curves; generating a fresh sweep..." -ForegroundColor Yellow
    $perturbationArgs = @(
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        (Join-Path $repoRoot "scripts\run-perturbation-sweep.ps1"),
        "-HostManifestPath",
        (Resolve-Path $HostManifestPath).Path,
        "-SkipHardening"
    )
    & $powerShell @perturbationArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to refresh perturbation artifact with capacity curves."
    }
    $perturbation = Get-Content -Path $PerturbationJsonPath -Raw | ConvertFrom-Json -Depth 64
}
if (-not $CapacityInputsJsonPath) {
    if ($perturbation.capacity_inputs -and $perturbation.capacity_inputs.json_path) {
        $CapacityInputsJsonPath = $perturbation.capacity_inputs.json_path
    } else {
        throw "Capacity inputs were not provided and the perturbation artifact does not point to them."
    }
}

$reportRoot = Join-Path $repoRoot "artifacts\performance\capacity"
$runTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $reportRoot ("runs\" + $runTimestamp)
$runJsonPath = Join-Path $runDir "capacity-report.json"
$runMarkdownPath = Join-Path $runDir "capacity-report.md"
$timestampedJsonPath = Join-Path $reportRoot ("capacity-" + $runTimestamp + ".json")
$timestampedMarkdownPath = Join-Path $reportRoot ("capacity-" + $runTimestamp + ".md")
$latestJsonPath = Join-Path $reportRoot "latest.json"
$latestMarkdownPath = Join-Path $reportRoot "latest.md"

New-Item -ItemType Directory -Force -Path $runDir | Out-Null
New-Item -ItemType Directory -Force -Path $reportRoot | Out-Null

$arguments = @(
    "run", "-p", "aether_api", "--example", "performance_capacity_report", "--release", "--",
    "--perturbation-json", (Resolve-Path $PerturbationJsonPath).Path,
    "--matrix-json", (Resolve-Path $MatrixJsonPath).Path,
    "--capacity-inputs-json", (Resolve-Path $CapacityInputsJsonPath).Path,
    "--output-json", $runJsonPath,
    "--output-report", $runMarkdownPath
)

Write-Host ""
Write-Host "AETHER Capacity Planner"
Write-Host "======================="
Write-Host "Perturbation: $PerturbationJsonPath"
Write-Host "Matrix:       $MatrixJsonPath"
Write-Host "Inputs:       $CapacityInputsJsonPath"
Write-Host "Run dir:      $runDir"
Write-Host ""

& $cargo @arguments
if ($LASTEXITCODE -ne 0) {
    throw "Capacity planner example failed."
}

Copy-Item -Force $runJsonPath $timestampedJsonPath
Copy-Item -Force $runMarkdownPath $timestampedMarkdownPath
Copy-Item -Force $runJsonPath $latestJsonPath
Copy-Item -Force $runMarkdownPath $latestMarkdownPath

Write-Host ""
Write-Host "Capacity artifacts refreshed." -ForegroundColor Green
Write-Host "Latest JSON: $latestJsonPath"
Write-Host "Latest MD:   $latestMarkdownPath"
Write-Host ""

Close-Runner 0
