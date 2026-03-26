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
$reportDir = Join-Path $repoRoot "artifacts\qa\release-readiness"
$transcriptPath = Join-Path $reportDir "release-readiness-$outputTimestamp.txt"
$latestTranscriptPath = Join-Path $reportDir "latest.txt"
$summaryPath = Join-Path $reportDir "release-readiness-$outputTimestamp.md"
$latestSummaryPath = Join-Path $reportDir "latest.md"
$pagesPreviewDir = Join-Path $repoRoot "artifacts\pages-preview-release"
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

function Format-CommandText([string]$Command, [string[]]$Arguments, [string]$WorkingDirectory) {
    $parts = [System.Collections.Generic.List[string]]::new()
    if ($WorkingDirectory -and $WorkingDirectory -ne $repoRoot) {
        $parts.Add("(cd `"$WorkingDirectory`" &&")
    }
    $parts.Add($Command)
    foreach ($argument in $Arguments) {
        if ($argument -match '[\s"]') {
            $escaped = $argument.Replace('"', '\"')
            $parts.Add("`"$escaped`"")
        } else {
            $parts.Add($argument)
        }
    }
    if ($WorkingDirectory -and $WorkingDirectory -ne $repoRoot) {
        $parts.Add(")")
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

function Invoke-Step(
    [string]$Label,
    [string]$Command,
    [string[]]$Arguments,
    [string]$WorkingDirectory = $repoRoot
) {
    $commandText = Format-CommandText $Command $Arguments $WorkingDirectory

    Write-Host ""
    Write-Host "[$Label]" -ForegroundColor Cyan
    Write-Host "Running: $commandText"

    Add-TranscriptLine("## $Label")
    Add-TranscriptLine("")
    Add-TranscriptLine("Command: $commandText")
    Add-TranscriptLine("")
    Add-TranscriptLine('```text')

    $stdoutPath = Join-Path ([System.IO.Path]::GetTempPath()) ("aether-release-" + [guid]::NewGuid().ToString() + ".out")
    $stderrPath = Join-Path ([System.IO.Path]::GetTempPath()) ("aether-release-" + [guid]::NewGuid().ToString() + ".err")
    try {
        $process = Start-Process `
            -FilePath $Command `
            -ArgumentList $Arguments `
            -WorkingDirectory $WorkingDirectory `
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
Write-Host "AETHER Release Readiness Suite"
Write-Host "=============================="
Write-Host "Started: $timestamp"
Write-Host ""

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
$python = Get-Command python -ErrorAction SilentlyContinue
$go = Get-Command go -ErrorAction SilentlyContinue
$pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
$git = Get-Command git -ErrorAction SilentlyContinue

if (-not $cargo) {
    Write-Host "Rust is not installed or cargo is not on PATH." -ForegroundColor Red
    Close-Runner 1
}
if (-not $python) {
    Write-Host "Python is not installed or python is not on PATH." -ForegroundColor Red
    Close-Runner 1
}
if (-not $go) {
    Write-Host "Go is not installed or go is not on PATH." -ForegroundColor Red
    Close-Runner 1
}
if (-not $pwsh) {
    Write-Host "PowerShell 7 (pwsh) is not available on PATH." -ForegroundColor Red
    Close-Runner 1
}

try {
    $baseline = Resolve-BaselineReference -ExplicitPath $BaselinePath -LocalPath $localBaselinePath -FixturePath $fixtureBaselinePath
} catch {
    Write-Host $_.Exception.Message -ForegroundColor Red
    Write-Host "Capture a local baseline with scripts/run-performance-baseline.cmd or pass -BaselinePath explicitly."
    Close-Runner 1
}

$commit = if ($git) {
    (& $git.Source -C $repoRoot rev-parse HEAD).Trim()
} else {
    "<git unavailable>"
}

New-Item -ItemType Directory -Force $reportDir | Out-Null

Add-TranscriptLine("AETHER Release Readiness Suite")
Add-TranscriptLine("==============================")
Add-TranscriptLine("Generated: $timestamp")
Add-TranscriptLine("Repository: $repoRoot")
Add-TranscriptLine("Commit: $commit")
Add-TranscriptLine("Baseline: $($baseline.Path)")
Add-TranscriptLine("Baseline source: $($baseline.Source)")
Add-TranscriptLine("")

Write-Host "Baseline: $($baseline.Path) [$($baseline.Source)]"
Write-Host "Commit:   $commit"
Write-Host ""

$failed = $false
$failureMessage = $null

try {
    Invoke-Step "Rust format check" $cargo.Source @("fmt", "--all", "--check")
    Invoke-Step "Rust clippy" $cargo.Source @("clippy", "--workspace", "--all-targets", "--", "-D", "warnings")
    Invoke-Step "Workspace tests" $cargo.Source @("test", "--workspace", "-j", "1", "--", "--test-threads=1")
    Invoke-Step "Python SDK tests" $python.Source @("-m", "unittest", "discover", "python/tests", "-v")
    Invoke-Step "Go boundary tests" $go.Source @("test", "./...") (Join-Path $repoRoot "go")
    Invoke-Step "Rust API docs" $cargo.Source @("doc", "--workspace", "--no-deps")
    Invoke-Step "Pages preview" $python.Source @("scripts/build_pages.py", "--out-dir", $pagesPreviewDir)
    Invoke-Step "Benchmark compile" $cargo.Source @("bench", "-p", "aether_api", "--no-run")
    Invoke-Step "Pilot launch validation" $pwsh.Source @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $repoRoot "scripts/run-pilot-launch-validation.ps1"), "-BaselinePath", $baseline.Path)
    Invoke-Step "Pilot package build" $pwsh.Source @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $repoRoot "scripts/build-pilot-package.ps1"))
} catch {
    $failed = $true
    $failureMessage = $_.Exception.Message
    Write-Host ""
    Write-Host $failureMessage -ForegroundColor Red
    Add-TranscriptLine("Release-readiness suite failed: $failureMessage")
}

$summaryLines = [System.Collections.Generic.List[string]]::new()
$summaryLines.Add("# AETHER Release Readiness")
$summaryLines.Add("")
$summaryLines.Add('- Generated: `' + $timestamp + '`')
$summaryLines.Add('- Commit: `' + $commit + '`')
$summaryLines.Add('- Baseline: `' + $baseline.Path + '`')
$summaryLines.Add('- Baseline source: `' + $baseline.Source + '`')
$summaryLines.Add("")
$summaryLines.Add("## Executed gates")
$summaryLines.Add("")
$summaryLines.Add("1. Rust format check")
$summaryLines.Add("2. Rust clippy")
$summaryLines.Add("3. Full Rust workspace tests")
$summaryLines.Add("4. Python SDK tests")
$summaryLines.Add("5. Go boundary tests")
$summaryLines.Add("6. Rust API docs build")
$summaryLines.Add("7. GitHub Pages preview bundle build")
$summaryLines.Add("8. Criterion benchmark compile")
$summaryLines.Add("9. Pilot launch validation pack")
$summaryLines.Add("10. Packaged pilot bundle build")
$summaryLines.Add("")
$summaryLines.Add("## Primary artifacts")
$summaryLines.Add("")
$summaryLines.Add('- `artifacts/qa/release-readiness/latest.txt`')
$summaryLines.Add('- `artifacts/pages-preview-release/`')
$summaryLines.Add('- `artifacts/pilot/reports/latest.md`')
$summaryLines.Add('- `artifacts/performance/latest.md`')
$summaryLines.Add('- `artifacts/performance/latest-drift.md`')
$summaryLines.Add('- `artifacts/pilot/launch/latest.txt`')
$summaryLines.Add('- `artifacts/pilot/packages/aether-pilot-service-windows-x86_64.zip`')
$summaryLines.Add("")
$summaryLines.Add("## Result")
$summaryLines.Add("")

if ($failed) {
    $summaryLines.Add("Release readiness failed: `"${failureMessage}`".")
} else {
    $summaryLines.Add("Release readiness completed successfully.")
}

$summary = $summaryLines -join "`r`n"

Set-Content -Path $transcriptPath -Value $transcript
Set-Content -Path $latestTranscriptPath -Value $transcript
Set-Content -Path $summaryPath -Value $summary
Set-Content -Path $latestSummaryPath -Value $summary

Write-Host ""
Write-Host "Release-readiness transcript: $transcriptPath"
Write-Host "Latest transcript:            $latestTranscriptPath"
Write-Host "Release-readiness summary:    $summaryPath"
Write-Host "Latest summary:               $latestSummaryPath"

if ($failed) {
    Close-Runner 1
}

Write-Host "Release-readiness suite completed successfully." -ForegroundColor Green
Close-Runner 0
