param(
    [switch]$PauseOnExit,
    [string]$BaselinePath
)

$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$outputTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$reportDir = Join-Path $repoRoot "artifacts\pilot\launch"
$reportPath = Join-Path $reportDir "pilot-launch-validation-$outputTimestamp.txt"
$latestPath = Join-Path $reportDir "latest.txt"
$localBaselinePath = Join-Path $repoRoot "artifacts\performance\baseline.json"
$fixtureBaselinePath = Join-Path $repoRoot "fixtures\performance\accepted-baseline.windows-x86_64.json"
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
        [string]$ExplicitPath,
        [string]$LocalPath,
        [string]$FixturePath
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

    if (Test-Path $LocalPath) {
        return [pscustomobject]@{
            Path = (Resolve-Path $LocalPath).Path
            Source = "local artifact"
        }
    }

    if (Test-Path $FixturePath) {
        return [pscustomobject]@{
            Path = (Resolve-Path $FixturePath).Path
            Source = "tracked fixture"
        }
    }

    throw "No performance baseline was found. Provide -BaselinePath, capture a local baseline in artifacts/performance/baseline.json, or restore fixtures/performance/accepted-baseline.windows-x86_64.json."
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
Write-Host ""

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargo) {
    Write-Host "Rust is not installed or cargo is not on PATH." -ForegroundColor Red
    Write-Host "Ask the platform team to restore the AETHER Rust toolchain before running launch validation."
    Close-Runner 1
}

$cargoPath = $cargo.Source

try {
    $baseline = Resolve-BaselineReference -ExplicitPath $BaselinePath -LocalPath $localBaselinePath -FixturePath $fixtureBaselinePath
} catch {
    Write-Host $_.Exception.Message -ForegroundColor Red
    Write-Host "Capture a local baseline with scripts/run-performance-baseline.cmd or pass -BaselinePath explicitly."
    Close-Runner 1
}

New-Item -ItemType Directory -Force $reportDir | Out-Null

Add-TranscriptLine("AETHER Pilot Launch Validation")
Add-TranscriptLine("==============================")
Add-TranscriptLine("Generated: $timestamp")
Add-TranscriptLine("Repository: $repoRoot")
Add-TranscriptLine("Baseline: $($baseline.Path)")
Add-TranscriptLine("Baseline source: $($baseline.Source)")
Add-TranscriptLine("")

Write-Host "Baseline: $($baseline.Path) [$($baseline.Source)]"
Write-Host ""

$failed = $false
$failureMessage = $null

try {
    Invoke-Step "Pilot report" $cargoPath @("run", "-p", "aether_api", "--example", "pilot_coordination_report", "--release")
    Invoke-Step "Performance report" $cargoPath @("run", "-p", "aether_api", "--example", "performance_report", "--release")
    Invoke-Step "Performance drift" $cargoPath @("run", "-p", "aether_api", "--example", "performance_drift_report", "--release", "--", $baseline.Path)
    Invoke-Step "Release API tests" $cargoPath @("test", "-p", "aether_api", "--release")
    Invoke-Step "Pilot soak suite" $cargoPath @("test", "-p", "aether_api", "--test", "pilot_soak", "--release", "--", "--ignored", "--nocapture")
    Invoke-Step "Performance stress suite" $cargoPath @("test", "-p", "aether_api", "--test", "performance_stress", "--release", "--", "--ignored", "--nocapture")
} catch {
    $failed = $true
    $failureMessage = $_.Exception.Message
    Write-Host ""
    Write-Host $failureMessage -ForegroundColor Red
    Add-TranscriptLine("Launch validation failed: $failureMessage")
}

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
