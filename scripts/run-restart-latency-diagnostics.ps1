param(
    [ValidateSet("core_kernel", "service_in_process", "full_stack")]
    [string]$Suite = "service_in_process",
    [ValidateRange(2, 100)]
    [int]$Runs = 10,
    [ValidateRange(1, 20)]
    [int]$Samples = 1,
    [string]$HostManifestPath,
    [string]$OutputDirectory
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
if (-not $HostManifestPath) {
    $HostManifestPath = Join-Path $repoRoot "fixtures\performance\hosts\dev-chad-windows-native.json"
}
if (-not (Test-Path -LiteralPath $HostManifestPath -PathType Leaf)) {
    throw "Host manifest does not exist: $HostManifestPath"
}

Push-Location $repoRoot
try {
    $commit = (& git rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to resolve the current commit."
    }
    $tree = (& git rev-parse "$commit`^{tree}").Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to resolve the current tree."
    }
    $ref = (& git symbolic-ref -q HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or -not $ref) {
        throw "Restart diagnostics require an attached branch ref."
    }
    $dirty = & git status --porcelain --untracked-files=no
    if ($LASTEXITCODE -ne 0 -or $dirty) {
        throw "Restart diagnostics require a clean tracked worktree. Commit the instrumentation first."
    }

    $cargo = Get-Command cargo -ErrorAction Stop
    $python = Get-Command python -ErrorAction Stop
    $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
    if (-not $OutputDirectory) {
        $OutputDirectory = Join-Path $repoRoot (
            "artifacts\performance\restart-diagnostics\{0}-{1}-{2}" -f
            $timestamp, $Suite, $commit.Substring(0, 12)
        )
    }
    New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null

    Write-Host "Building the release-mode performance executable..."
    & $cargo.Source build -p aether_api --example performance_report --release
    if ($LASTEXITCODE -ne 0) {
        throw "Release build failed with exit code $LASTEXITCODE."
    }

    $binaryName = if ($IsWindows -or $env:OS -eq "Windows_NT") {
        "performance_report.exe"
    } else {
        "performance_report"
    }
    $binary = Join-Path $repoRoot "target\release\examples\$binaryName"
    if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) {
        throw "Performance executable is missing: $binary"
    }

    $bundlePaths = [System.Collections.Generic.List[string]]::new()
    for ($index = 1; $index -le $Runs; $index++) {
        $runDirectory = Join-Path $OutputDirectory ("run-{0:D3}" -f $index)
        $bundlePath = Join-Path $runDirectory "bundle.json"
        $reportPath = Join-Path $runDirectory "report.md"
        $logPath = Join-Path $runDirectory "process.log"
        New-Item -ItemType Directory -Force -Path $runDirectory | Out-Null

        Write-Host ("Running fresh process {0}/{1}..." -f $index, $Runs)
        & $binary @(
            "--suite", $Suite,
            "--samples", $Samples,
            "--host-manifest", (Resolve-Path -LiteralPath $HostManifestPath).Path,
            "--bundle-path", $bundlePath,
            "--report-path", $reportPath
        ) *> $logPath
        if ($LASTEXITCODE -ne 0) {
            throw "Fresh process $index failed; see $logPath."
        }
        if (-not (Test-Path -LiteralPath $bundlePath -PathType Leaf)) {
            throw "Fresh process $index did not produce $bundlePath."
        }
        $bundlePaths.Add($bundlePath)
    }

    $outputJson = Join-Path $OutputDirectory "restart-latency-diagnostics.json"
    $outputReport = Join-Path $OutputDirectory "restart-latency-diagnostics.md"
    $aggregateArguments = @(
        (Join-Path $PSScriptRoot "restart_latency_diagnostics.py"),
        "--expected-commit", $commit,
        "--expected-tree", $tree,
        "--expected-ref", $ref,
        "--output-json", $outputJson,
        "--output-report", $outputReport
    )
    foreach ($bundlePath in $bundlePaths) {
        $aggregateArguments += @("--bundle", $bundlePath)
    }
    & $python.Source @aggregateArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Restart diagnostic aggregation failed with exit code $LASTEXITCODE."
    }

    Write-Host ""
    Write-Host "Restart diagnostics completed." -ForegroundColor Green
    Write-Host "Commit: $commit"
    Write-Host "Tree:   $tree"
    Write-Host "Ref:    $ref"
    Write-Host "JSON:   $outputJson"
    Write-Host "Report: $outputReport"
}
finally {
    Pop-Location
}
