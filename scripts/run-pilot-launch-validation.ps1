param(
    [switch]$PauseOnExit
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$outputTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$reportDir = Join-Path $repoRoot "artifacts\pilot\launch"
$reportPath = Join-Path $reportDir "pilot-launch-validation-$outputTimestamp.txt"
$latestPath = Join-Path $reportDir "latest.txt"
$baselinePath = Join-Path $repoRoot "artifacts\performance\baseline.json"
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

function Invoke-Step([string]$Label, [string]$CommandText) {
    Write-Host ""
    Write-Host "[$Label]" -ForegroundColor Cyan
    Write-Host "Running: $CommandText"

    Add-TranscriptLine("## $Label")
    Add-TranscriptLine("")
    Add-TranscriptLine("Command: $CommandText")
    Add-TranscriptLine("")
    Add-TranscriptLine('```text')

    $outputLines = & cmd.exe /d /c "$CommandText 2>&1"
    $exitCode = $LASTEXITCODE

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

if (-not (Test-Path $baselinePath)) {
    Write-Host "No performance baseline exists yet." -ForegroundColor Red
    Write-Host "Run scripts/run-performance-baseline.cmd before launch validation."
    Close-Runner 1
}

New-Item -ItemType Directory -Force $reportDir | Out-Null

Add-TranscriptLine("AETHER Pilot Launch Validation")
Add-TranscriptLine("==============================")
Add-TranscriptLine("Generated: $timestamp")
Add-TranscriptLine("Repository: $repoRoot")
Add-TranscriptLine("Baseline: $baselinePath")
Add-TranscriptLine("")

$failed = $false
$failureMessage = $null

try {
    Invoke-Step "Pilot report" "cargo run -p aether_api --example pilot_coordination_report --release"
    Invoke-Step "Performance report" "cargo run -p aether_api --example performance_report --release"
    Invoke-Step "Performance drift" "cargo run -p aether_api --example performance_drift_report --release -- $baselinePath"
    Invoke-Step "Release API tests" "cargo test -p aether_api --release"
    Invoke-Step "Pilot soak suite" "cargo test -p aether_api --test pilot_soak --release -- --ignored --nocapture"
    Invoke-Step "Performance stress suite" "cargo test -p aether_api --test performance_stress --release -- --ignored --nocapture"
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
