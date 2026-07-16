param(
    [switch]$PauseOnExit,
    [string]$BaselinePath,
    [string]$HostManifestPath
)

$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
if (-not $HostManifestPath) {
    $HostManifestPath = Join-Path $repoRoot "fixtures\performance\hosts\dev-chad-windows-native.json"
}
$hostManifest = Get-Content -Path $HostManifestPath | ConvertFrom-Json
$hostId = $hostManifest.host_id
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$outputTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$reportDir = Join-Path $repoRoot "artifacts\pilot\launch"
$reportPath = Join-Path $reportDir "pilot-launch-validation-$outputTimestamp.txt"
$latestPath = Join-Path $reportDir "latest.txt"
$performanceSummaryPath = Join-Path $repoRoot "artifacts\performance\latest-drift.md"
$transcript = [System.Collections.Generic.List[string]]::new()

function Close-Runner([int]$ExitCode) {
    if ($PauseOnExit) {
        Write-Host ""
        Read-Host "Press Enter to close"
    }
    exit $ExitCode
}

function Add-TranscriptLine([string]$Line) {
    $script:transcript.Add($Line)
}

function Format-CommandText([string]$Command, [string[]]$Arguments) {
    $parts = [System.Collections.Generic.List[string]]::new()
    $parts.Add($Command)
    foreach ($argument in $Arguments) {
        if ($argument -match '[\s"]') {
            $escaped = $argument.Replace('"', '\"')
            $parts.Add("`"$escaped`"")
        } else {
            $parts.Add($argument)
        }
    }
    $parts -join " "
}

function Resolve-BaselineReference {
    param(
        [string]$Suite,
        [string]$ExplicitPath,
        [string]$RepoRoot,
        [string]$HostId
    )

    if ($ExplicitPath) {
        if (-not (Test-Path $ExplicitPath)) {
            throw "Explicit baseline path not found: $ExplicitPath"
        }
        return [pscustomobject]@{
            Path = (Resolve-Path $ExplicitPath).Path
            Source = "explicit override"
        }
    }

    $localPath = Join-Path $RepoRoot ("artifacts\performance\baselines\{0}\{1}.json" -f $Suite, $HostId)
    if (Test-Path $localPath) {
        return [pscustomobject]@{
            Path = (Resolve-Path $localPath).Path
            Source = "local artifact"
        }
    }

    $fixturePath = Join-Path $RepoRoot ("fixtures\performance\baselines\{0}\{1}.json" -f $Suite, $HostId)
    if (Test-Path $fixturePath) {
        return [pscustomobject]@{
            Path = (Resolve-Path $fixturePath).Path
            Source = "tracked fixture"
        }
    }

    throw "No baseline found for suite $Suite on host $HostId."
}

function Invoke-Step([string]$Label, [string]$Command, [string[]]$Arguments) {
    $commandText = Format-CommandText $Command $Arguments

    Write-Host ""
    Write-Host "[$Label]" -ForegroundColor Cyan
    Write-Host "Running: $commandText"

    Add-TranscriptLine("## $Label")
    Add-TranscriptLine("")
    Add-TranscriptLine("Command: $commandText")
    Add-TranscriptLine("")
    Add-TranscriptLine('```text')

    $stdoutPath = Join-Path ([System.IO.Path]::GetTempPath()) ("aether-launch-" + [guid]::NewGuid().ToString() + ".out")
    $stderrPath = Join-Path ([System.IO.Path]::GetTempPath()) ("aether-launch-" + [guid]::NewGuid().ToString() + ".err")
    try {
        $process = Start-Process `
            -FilePath $Command `
            -ArgumentList $Arguments `
            -NoNewWindow `
            -Wait `
            -PassThru `
            -RedirectStandardOutput $stdoutPath `
            -RedirectStandardError $stderrPath

        $outputLines = [System.Collections.Generic.List[string]]::new()
        if (Test-Path $stdoutPath) {
            foreach ($line in Get-Content -Path $stdoutPath) {
                $outputLines.Add($line)
            }
        }
        if (Test-Path $stderrPath) {
            foreach ($line in Get-Content -Path $stderrPath) {
                $outputLines.Add($line)
            }
        }
        $exitCode = $process.ExitCode
    } finally {
        Remove-Item -Force -ErrorAction SilentlyContinue $stdoutPath, $stderrPath
    }

    foreach ($line in $outputLines) {
        $text = $line.ToString()
        Write-Host $text
        Add-TranscriptLine($text)
    }

    Add-TranscriptLine('```')
    Add-TranscriptLine("")

    if ($exitCode -ne 0) {
        throw "Step failed: $Label (exit $exitCode)"
    }
}

Write-Host ""
Write-Host "AETHER Pilot Launch Validation"
Write-Host "=============================="
Write-Host "Started: $timestamp"
Write-Host "Host:    $hostId"
Write-Host ""

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
$pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
if (-not $cargo) {
    Write-Host "Rust is not installed or cargo is not on PATH." -ForegroundColor Red
    Close-Runner 1
}
if (-not $pwsh) {
    Write-Host "PowerShell 7 (pwsh) is not available on PATH." -ForegroundColor Red
    Close-Runner 1
}

try {
    $coreBaseline = Resolve-BaselineReference -Suite "core_kernel" -ExplicitPath $BaselinePath -RepoRoot $repoRoot -HostId $hostId
    $serviceBaseline = Resolve-BaselineReference -Suite "service_in_process" -ExplicitPath $null -RepoRoot $repoRoot -HostId $hostId
} catch {
    Write-Host $_.Exception.Message -ForegroundColor Red
    Close-Runner 1
}

New-Item -ItemType Directory -Force $reportDir | Out-Null

Add-TranscriptLine("AETHER Pilot Launch Validation")
Add-TranscriptLine("==============================")
Add-TranscriptLine("Generated: $timestamp")
Add-TranscriptLine("Repository: $repoRoot")
Add-TranscriptLine("Host manifest: $HostManifestPath")
Add-TranscriptLine("Core baseline: $($coreBaseline.Path)")
Add-TranscriptLine("Service baseline: $($serviceBaseline.Path)")
Add-TranscriptLine("")

Write-Host "Core baseline:    $($coreBaseline.Path) [$($coreBaseline.Source)]"
Write-Host "Service baseline: $($serviceBaseline.Path) [$($serviceBaseline.Source)]"
Write-Host ""

$failed = $false
$failureMessage = $null

try {
    Invoke-Step "Pilot report" $cargo.Source @("run", "-p", "aether_api", "--example", "pilot_coordination_report", "--release")
    Invoke-Step "Performance report" $pwsh.Source @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $repoRoot "scripts/run-performance-report.ps1"), "-Suite", "full_stack", "-HostManifestPath", (Resolve-Path $HostManifestPath).Path)
    Invoke-Step "Core drift" $pwsh.Source @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $repoRoot "scripts/run-performance-drift.ps1"), "-Suite", "core_kernel", "-HostManifestPath", (Resolve-Path $HostManifestPath).Path, "-BaselinePath", $coreBaseline.Path)
    Invoke-Step "Service drift" $pwsh.Source @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $repoRoot "scripts/run-performance-drift.ps1"), "-Suite", "service_in_process", "-HostManifestPath", (Resolve-Path $HostManifestPath).Path, "-BaselinePath", $serviceBaseline.Path)
    Invoke-Step "Release API tests" $cargo.Source @("test", "-p", "aether_api", "--release")
    Invoke-Step "Pilot soak suite" $cargo.Source @("test", "-p", "aether_api", "--test", "pilot_soak", "--release", "--", "--ignored", "--nocapture")
    Invoke-Step "Performance stress suite" $cargo.Source @("test", "-p", "aether_api", "--test", "performance_stress", "--release", "--", "--ignored", "--nocapture")
} catch {
    $failed = $true
    $failureMessage = $_.Exception.Message
    Write-Host ""
    Write-Host $failureMessage -ForegroundColor Red
    Add-TranscriptLine("Launch validation failed: $failureMessage")
}

$combinedDrift = [System.Collections.Generic.List[string]]::new()
$combinedDrift.Add("# AETHER Performance Drift Summary")
$combinedDrift.Add("")
$combinedDrift.Add("## Core Kernel")
$combinedDrift.Add("")
if (Test-Path (Join-Path $repoRoot "artifacts\performance\latest-drift-core_kernel.md")) {
    foreach ($line in Get-Content -Path (Join-Path $repoRoot "artifacts\performance\latest-drift-core_kernel.md")) {
        $combinedDrift.Add($line)
    }
} else {
    $combinedDrift.Add("No core-kernel drift report was generated.")
}
$combinedDrift.Add("")
$combinedDrift.Add("## Service In Process")
$combinedDrift.Add("")
if (Test-Path (Join-Path $repoRoot "artifacts\performance\latest-drift-service_in_process.md")) {
    foreach ($line in Get-Content -Path (Join-Path $repoRoot "artifacts\performance\latest-drift-service_in_process.md")) {
        $combinedDrift.Add($line)
    }
} else {
    $combinedDrift.Add("No service drift report was generated.")
}
Set-Content -Path $performanceSummaryPath -Value $combinedDrift

Set-Content -Path $reportPath -Value $transcript
Set-Content -Path $latestPath -Value $transcript

Write-Host ""
Write-Host "Validation transcript: $reportPath"
Write-Host "Latest transcript:     $latestPath"

if ($failed) {
    Close-Runner 1
}

Write-Host "Pilot launch validation completed successfully." -ForegroundColor Green
Close-Runner 0
