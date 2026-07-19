param(
    [switch]$PauseOnExit,
    [string]$BaselinePath,
    [string]$HostManifestPath,
    [string]$CandidatePackageZip,
    [string]$CandidateSha,
    [string]$CandidateRef = "refs/heads/main",
    [string]$EvidenceManifestPath,
    [switch]$CommercialBetaCandidate
)

$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$outputTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$suiteStartedAtUtc = [DateTime]::UtcNow
$reportDir = Join-Path $repoRoot "artifacts\qa\release-readiness"
$transcriptPath = Join-Path $reportDir "release-readiness-$outputTimestamp.txt"
$latestTranscriptPath = Join-Path $reportDir "latest.txt"
$summaryPath = Join-Path $reportDir "release-readiness-$outputTimestamp.md"
$latestSummaryPath = Join-Path $reportDir "latest.md"
$hardeningGateJsonPath = Join-Path $reportDir "hardening-gates-$outputTimestamp.json"
$hardeningGateSummaryPath = Join-Path $reportDir "hardening-gates-$outputTimestamp.md"
$latestHardeningGateJsonPath = Join-Path $reportDir "hardening-gates-latest.json"
$latestHardeningGateSummaryPath = Join-Path $reportDir "hardening-gates-latest.md"
$latestHardeningJsonPath = Join-Path $repoRoot "artifacts\qa\hardening\latest.json"
$performanceBetaJsonPath = Join-Path $reportDir "performance-beta-$outputTimestamp.json"
$performanceBetaSummaryPath = Join-Path $reportDir "performance-beta-$outputTimestamp.md"
$latestPerformanceBetaJsonPath = Join-Path $reportDir "performance-beta-latest.json"
$latestPerformanceBetaSummaryPath = Join-Path $reportDir "performance-beta-latest.md"
$performanceBetaThresholdsPath = Join-Path $repoRoot "fixtures\release\performance-beta-thresholds.json"
$latestPerformanceBundlePath = Join-Path $repoRoot "artifacts\performance\latest.json"
$serviceV2JsonPath = Join-Path $reportDir "service-v2-operability-$outputTimestamp.json"
$serviceV2SummaryPath = Join-Path $reportDir "service-v2-operability-$outputTimestamp.md"
$latestServiceV2JsonPath = Join-Path $reportDir "service-v2-operability-latest.json"
$latestServiceV2SummaryPath = Join-Path $reportDir "service-v2-operability-latest.md"
$rollbackJsonPath = Join-Path $reportDir "rollback-record-$outputTimestamp.json"
$rollbackSummaryPath = Join-Path $reportDir "rollback-record-$outputTimestamp.md"
$latestRollbackJsonPath = Join-Path $reportDir "rollback-record-latest.json"
$latestRollbackSummaryPath = Join-Path $reportDir "rollback-record-latest.md"
$customerWorkflowJsonPath = Join-Path $reportDir "customer-workflow-$outputTimestamp.json"
$customerWorkflowSummaryPath = Join-Path $reportDir "customer-workflow-$outputTimestamp.md"
$latestCustomerWorkflowJsonPath = Join-Path $reportDir "customer-workflow-latest.json"
$latestCustomerWorkflowSummaryPath = Join-Path $reportDir "customer-workflow-latest.md"
$securityKeyJsonPath = Join-Path $reportDir "security-key-lifecycle-$outputTimestamp.json"
$securityKeySummaryPath = Join-Path $reportDir "security-key-lifecycle-$outputTimestamp.md"
$latestSecurityKeyJsonPath = Join-Path $reportDir "security-key-lifecycle-latest.json"
$latestSecurityKeySummaryPath = Join-Path $reportDir "security-key-lifecycle-latest.md"
$commercialLedgerPath = Join-Path $repoRoot "fixtures\release\commercial-readiness-ledger.json"
$commercialReadinessJsonPath = Join-Path $reportDir "commercial-readiness-$outputTimestamp.json"
$commercialReadinessSummaryPath = Join-Path $reportDir "commercial-readiness-$outputTimestamp.md"
$latestCommercialReadinessJsonPath = Join-Path $reportDir "commercial-readiness-latest.json"
$latestCommercialReadinessSummaryPath = Join-Path $reportDir "commercial-readiness-latest.md"
$pagesPreviewDir = Join-Path $repoRoot "artifacts\pages-preview-release"
$pilotPackageRoot = Join-Path $repoRoot "artifacts\pilot\packages\aether-pilot-service-windows-x86_64"
$pilotPackageZip = "$pilotPackageRoot.zip"
$serviceV2PackageProofDir = Join-Path $reportDir ("service-v2-package-proof-" + $outputTimestamp)
$securityKeyProofDir = Join-Path $reportDir ("security-key-proof-" + $outputTimestamp)
$supplyChainProofDir = Join-Path $reportDir ("supply-chain-proof-" + $outputTimestamp)
$pilotLaunchArtifactPath = $null
$transcript = [System.Collections.Generic.List[string]]::new()
if (-not $HostManifestPath) {
    $HostManifestPath = Join-Path $repoRoot "fixtures\performance\hosts\dev-chad-windows-native.json"
}
$hostManifest = Get-Content -Path $HostManifestPath | ConvertFrom-Json
$hostId = $hostManifest.host_id
$commercialTargetStage = "unknown"
if (Test-Path $commercialLedgerPath) {
    $commercialLedger = Get-Content -Path $commercialLedgerPath -Raw | ConvertFrom-Json
    $commercialTargetStage = $commercialLedger.current_target_stage
}
$enforceCommercialBeta = $CommercialBetaCandidate -or ($commercialTargetStage -eq "commercial_beta")

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

    $localPath = Join-Path $RepoRoot ("artifacts\performance\baselines\core_kernel\{0}.json" -f $HostId)
    if (Test-Path $localPath) {
        return [pscustomobject]@{
            Path = (Resolve-Path $localPath).Path
            Source = "local artifact"
        }
    }

    $fixturePath = Join-Path $RepoRoot ("fixtures\performance\baselines\core_kernel\{0}.json" -f $HostId)
    if (Test-Path $fixturePath) {
        return [pscustomobject]@{
            Path = (Resolve-Path $fixturePath).Path
            Source = "tracked fixture"
        }
    }

    throw "No core-kernel performance baseline was found for host $HostId. Provide -BaselinePath, capture a local baseline in artifacts/performance/baselines/core_kernel/$HostId.json, or restore fixtures/performance/baselines/core_kernel/$HostId.json."
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
    $baseline = Resolve-BaselineReference -ExplicitPath $BaselinePath -RepoRoot $repoRoot -HostId $hostId
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

function Get-FileReceipt([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Required immutable readiness output is missing: $Path"
    }
    $resolved = (Resolve-Path -LiteralPath $Path).Path
    $relative = [System.IO.Path]::GetRelativePath($repoRoot, $resolved).Replace('\', '/')
    if ($relative.ToLowerInvariant().Contains('latest')) {
        throw "Mutable latest path cannot enter the readiness evidence manifest: $relative"
    }
    [ordered]@{
        path = $relative
        sha256 = (Get-FileHash -LiteralPath $resolved -Algorithm SHA256).Hash.ToLowerInvariant()
        byte_size = (Get-Item -LiteralPath $resolved).Length
    }
}
if ($CandidateSha -and $commit -ne $CandidateSha) {
    Write-Host "Checked-out commit $commit does not match candidate $CandidateSha." -ForegroundColor Red
    Close-Runner 1
}
if ($CandidatePackageZip) {
    $CandidatePackageZip = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($CandidatePackageZip)
    if (-not (Test-Path -LiteralPath $CandidatePackageZip -PathType Leaf)) {
        Write-Host "Canonical candidate package does not exist: $CandidatePackageZip" -ForegroundColor Red
        Close-Runner 1
    }
}

New-Item -ItemType Directory -Force $reportDir | Out-Null

Add-TranscriptLine("AETHER Release Readiness Suite")
Add-TranscriptLine("==============================")
Add-TranscriptLine("Generated: $timestamp")
Add-TranscriptLine("Repository: $repoRoot")
Add-TranscriptLine("Commit: $commit")
Add-TranscriptLine("Host manifest: $HostManifestPath")
Add-TranscriptLine("Baseline: $($baseline.Path)")
Add-TranscriptLine("Baseline source: $($baseline.Source)")
Add-TranscriptLine("Commercial target stage: $commercialTargetStage")
Add-TranscriptLine("")

Write-Host "Baseline: $($baseline.Path) [$($baseline.Source)]"
Write-Host "Host:     $hostId"
Write-Host "Commit:   $commit"
Write-Host "Target:   $commercialTargetStage"
Write-Host ""

$failed = $false
$failureMessage = $null

try {
    $hardeningGateArgs = @(
        (Join-Path $repoRoot "scripts\hardening_promotion.py"),
        "gate-summary",
        "--config", (Join-Path $repoRoot ".github\hardening-promotion-state.json"),
        "--out-json", $hardeningGateJsonPath,
        "--out-md", $hardeningGateSummaryPath
    )
    if (Test-Path $latestHardeningJsonPath) {
        $hardeningGateArgs += @("--hardening-json", $latestHardeningJsonPath)
    }
    Invoke-Step "Hardening gate summary" $python.Source $hardeningGateArgs
    if (Test-Path $hardeningGateJsonPath) {
        Copy-Item -Force $hardeningGateJsonPath $latestHardeningGateJsonPath
    }
    if (Test-Path $hardeningGateSummaryPath) {
        Copy-Item -Force $hardeningGateSummaryPath $latestHardeningGateSummaryPath
    }
    Invoke-Step "Rust format check" $cargo.Source @("fmt", "--all", "--check")
    Invoke-Step "Rust clippy" $cargo.Source @("clippy", "--workspace", "--all-targets", "--", "-D", "warnings")
    Invoke-Step "Workspace tests" $cargo.Source @("test", "--workspace", "-j", "1", "--", "--test-threads=1")
    Invoke-Step "Python SDK tests" $python.Source @("-m", "unittest", "discover", "python/tests", "-v")
    Invoke-Step "Go boundary tests" $go.Source @("test", "./...") (Join-Path $repoRoot "go")
    Invoke-Step "Rust API docs" $cargo.Source @("doc", "--workspace", "--no-deps")
    Invoke-Step "Pages preview" $python.Source @("scripts/build_pages.py", "--out-dir", $pagesPreviewDir)
    Invoke-Step "Benchmark compile" $cargo.Source @("bench", "-p", "aether_api", "--no-run")
    Invoke-Step "Pilot launch validation" $pwsh.Source @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $repoRoot "scripts/run-pilot-launch-validation.ps1"), "-BaselinePath", $baseline.Path, "-HostManifestPath", (Resolve-Path $HostManifestPath).Path)
    $pilotLaunchArtifactPath = Get-ChildItem -Path (Join-Path $repoRoot "artifacts\pilot\launch") -Filter "pilot-launch-validation-*.txt" |
        Where-Object { $_.LastWriteTimeUtc -ge $suiteStartedAtUtc.AddSeconds(-1) } |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1 -ExpandProperty FullName
    if (-not $pilotLaunchArtifactPath) {
        throw "Pilot launch validation did not produce a current-run immutable transcript."
    }
    $performanceBetaArgs = @(
        (Join-Path $repoRoot "scripts\performance_beta_gate.py"),
        "run",
        "--thresholds", $performanceBetaThresholdsPath,
        "--bundle", $latestPerformanceBundlePath,
        "--out-json", $performanceBetaJsonPath,
        "--out-md", $performanceBetaSummaryPath,
        "--enforce"
    )
    Invoke-Step "Performance beta gate" $python.Source $performanceBetaArgs
    if (Test-Path $performanceBetaJsonPath) {
        Copy-Item -Force $performanceBetaJsonPath $latestPerformanceBetaJsonPath
    }
    if (Test-Path $performanceBetaSummaryPath) {
        Copy-Item -Force $performanceBetaSummaryPath $latestPerformanceBetaSummaryPath
    }
    if (-not $CandidatePackageZip) {
        throw "Release Readiness requires -CandidatePackageZip; rebuilding candidate bytes is forbidden."
    }
    if (Test-Path -LiteralPath $pilotPackageRoot) {
        Remove-Item -LiteralPath $pilotPackageRoot -Recurse -Force
    }
    if (Test-Path -LiteralPath $pilotPackageZip) {
        Remove-Item -LiteralPath $pilotPackageZip -Force
    }
    $pilotPackageParent = Split-Path -Parent $pilotPackageZip
    New-Item -ItemType Directory -Force $pilotPackageParent | Out-Null
    Copy-Item -LiteralPath $CandidatePackageZip -Destination $pilotPackageZip
    Expand-Archive -LiteralPath $pilotPackageZip -DestinationPath $pilotPackageRoot -Force
    $sourcePackageSha = (Get-FileHash -LiteralPath $CandidatePackageZip -Algorithm SHA256).Hash.ToLowerInvariant()
    $testedPackageSha = (Get-FileHash -LiteralPath $pilotPackageZip -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($sourcePackageSha -ne $testedPackageSha) {
        throw "Canonical package digest changed while staging Release Readiness."
    }
    Add-TranscriptLine("Canonical package SHA-256: $testedPackageSha")
    Add-TranscriptLine("Canonical package input: $CandidatePackageZip")
    Write-Host "Testing canonical Supply Chain package $testedPackageSha" -ForegroundColor Cyan
    Invoke-Step "CycloneDX SBOM, license, and delivery-input gate" $python.Source @(
        (Join-Path $repoRoot "scripts\supply_chain.py"),
        "generate",
        "--candidate-sha", $commit,
        "--package-zip", $pilotPackageZip,
        "--out-dir", $supplyChainProofDir
    )
    $securityKeyArgs = @(
        (Join-Path $repoRoot "scripts\security_key_lifecycle.py"),
        "run",
        "--package-root", $pilotPackageRoot,
        "--artifact-dir", $securityKeyProofDir,
        "--out-json", $securityKeyJsonPath,
        "--out-md", $securityKeySummaryPath,
        "--enforce"
    )
    Invoke-Step "Security and key lifecycle gate" $python.Source $securityKeyArgs
    if (Test-Path $securityKeyJsonPath) {
        Copy-Item -Force $securityKeyJsonPath $latestSecurityKeyJsonPath
    }
    if (Test-Path $securityKeySummaryPath) {
        Copy-Item -Force $securityKeySummaryPath $latestSecurityKeySummaryPath
    }
    $serviceV2Args = @(
        (Join-Path $repoRoot "scripts\service_v2_operability.py"),
        "run",
        "--out-json", $serviceV2JsonPath,
        "--out-md", $serviceV2SummaryPath,
        "--package-root", $pilotPackageRoot,
        "--artifact-dir", $serviceV2PackageProofDir,
        "--accept-ci-postgres"
    )
    if ($enforceCommercialBeta) {
        $serviceV2Args += @("--enforce-beta")
    }
    if (Test-Path $latestHardeningJsonPath) {
        $serviceV2Args += @("--hardening-json", $latestHardeningJsonPath)
    }
    Invoke-Step "Service v2 operability proof" $python.Source $serviceV2Args
    if (Test-Path $serviceV2JsonPath) {
        Copy-Item -Force $serviceV2JsonPath $latestServiceV2JsonPath
    }
    if (Test-Path $serviceV2SummaryPath) {
        Copy-Item -Force $serviceV2SummaryPath $latestServiceV2SummaryPath
    }
    $rollbackArgs = @(
        (Join-Path $repoRoot "scripts\release_rollback_record.py"),
        "render",
        "--service-v2-json", $serviceV2JsonPath,
        "--package-root", $pilotPackageRoot,
        "--package-zip", $pilotPackageZip,
        "--out-json", $rollbackJsonPath,
        "--out-md", $rollbackSummaryPath
    )
    if ($enforceCommercialBeta) {
        $rollbackArgs += @("--enforce")
    }
    Invoke-Step "Release rollback record" $python.Source $rollbackArgs
    if (Test-Path $rollbackJsonPath) {
        Copy-Item -Force $rollbackJsonPath $latestRollbackJsonPath
    }
    if (Test-Path $rollbackSummaryPath) {
        Copy-Item -Force $rollbackSummaryPath $latestRollbackSummaryPath
    }
    $customerWorkflowArgs = @(
        (Join-Path $repoRoot "scripts\customer_workflow_acceptance.py"),
        "run",
        "--out-json", $customerWorkflowJsonPath,
        "--out-md", $customerWorkflowSummaryPath,
        "--enforce"
    )
    Invoke-Step "Customer workflow acceptance" $python.Source $customerWorkflowArgs
    if (Test-Path $customerWorkflowJsonPath) {
        Copy-Item -Force $customerWorkflowJsonPath $latestCustomerWorkflowJsonPath
    }
    if (Test-Path $customerWorkflowSummaryPath) {
        Copy-Item -Force $customerWorkflowSummaryPath $latestCustomerWorkflowSummaryPath
    }
    $commercialReadinessArgs = @(
        (Join-Path $repoRoot "scripts\commercial_readiness.py"),
        "render",
        "--ledger", $commercialLedgerPath,
        "--out-json", $commercialReadinessJsonPath,
        "--out-md", $commercialReadinessSummaryPath
    )
    Invoke-Step "Commercial claim policy (diagnostic renderer)" $python.Source $commercialReadinessArgs
    if (Test-Path $commercialReadinessJsonPath) {
        Copy-Item -Force $commercialReadinessJsonPath $latestCommercialReadinessJsonPath
    }
    if (Test-Path $commercialReadinessSummaryPath) {
        Copy-Item -Force $commercialReadinessSummaryPath $latestCommercialReadinessSummaryPath
    }
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
$summaryLines.Add("1. Hardening gate-state summary")
$summaryLines.Add("2. Rust format check")
$summaryLines.Add("3. Rust clippy")
$summaryLines.Add("4. Full Rust workspace tests")
$summaryLines.Add("5. Python SDK tests")
$summaryLines.Add("6. Go boundary tests")
$summaryLines.Add("7. Rust API docs build")
$summaryLines.Add("8. GitHub Pages preview bundle build")
$summaryLines.Add("9. Criterion benchmark compile")
$summaryLines.Add("10. Pilot launch validation pack")
$summaryLines.Add("11. Performance beta gate")
$summaryLines.Add("12. Packaged pilot bundle build")
$summaryLines.Add("13. CycloneDX SBOM, license, and delivery-input gate")
$summaryLines.Add("14. Security and key lifecycle gate")
$summaryLines.Add("15. Service v2 operability proof")
$summaryLines.Add("16. Release rollback record")
$summaryLines.Add("17. Customer workflow acceptance")
$summaryLines.Add("18. Commercial release readiness ledger")
$summaryLines.Add("")
$summaryLines.Add("## Primary artifacts")
$summaryLines.Add("")
$summaryLines.Add('- `artifacts/qa/release-readiness/latest.txt`')
$summaryLines.Add('- `artifacts/qa/release-readiness/hardening-gates-latest.md`')
$summaryLines.Add('- `artifacts/qa/release-readiness/hardening-gates-latest.json`')
$summaryLines.Add('- `artifacts/qa/release-readiness/performance-beta-latest.md`')
$summaryLines.Add('- `artifacts/qa/release-readiness/performance-beta-latest.json`')
$summaryLines.Add('- `artifacts/qa/release-readiness/security-key-lifecycle-latest.md`')
$summaryLines.Add('- `artifacts/qa/release-readiness/security-key-lifecycle-latest.json`')
$summaryLines.Add('- `artifacts/qa/release-readiness/supply-chain-proof-*/supply-chain-summary.json`')
$summaryLines.Add('- `artifacts/qa/release-readiness/supply-chain-proof-*/*.cdx.json`')
$summaryLines.Add('- `artifacts/qa/release-readiness/service-v2-operability-latest.md`')
$summaryLines.Add('- `artifacts/qa/release-readiness/service-v2-operability-latest.json`')
$summaryLines.Add('- `artifacts/qa/release-readiness/rollback-record-latest.md`')
$summaryLines.Add('- `artifacts/qa/release-readiness/rollback-record-latest.json`')
$summaryLines.Add('- `artifacts/qa/release-readiness/customer-workflow-latest.md`')
$summaryLines.Add('- `artifacts/qa/release-readiness/customer-workflow-latest.json`')
$summaryLines.Add('- `artifacts/qa/release-readiness/commercial-readiness-latest.md`')
$summaryLines.Add('- `artifacts/qa/release-readiness/commercial-readiness-latest.json`')
$summaryLines.Add('- `artifacts/pages-preview-release/`')
$summaryLines.Add('- `artifacts/pilot/reports/latest.md`')
$summaryLines.Add('- `artifacts/performance/latest.md`')
$summaryLines.Add('- `artifacts/performance/latest-drift.md`')
$summaryLines.Add('- `artifacts/pilot/launch/latest.txt`')
$summaryLines.Add('- `artifacts/pilot/packages/aether-pilot-service-windows-x86_64.zip`')
$summaryLines.Add("")
$summaryLines.Add("## Hardening Gate State")
$summaryLines.Add("")
if (Test-Path $hardeningGateSummaryPath) {
    $gateLines = Get-Content -Path $hardeningGateSummaryPath
    foreach ($line in $gateLines) {
        if ($line -eq "# AETHER Hardening Gate State") {
            continue
        }
        $summaryLines.Add($line)
    }
} else {
    $summaryLines.Add("Hardening gate-state summary was not generated.")
}
$summaryLines.Add("")
$summaryLines.Add("## Performance Beta Gate")
$summaryLines.Add("")
if (Test-Path $performanceBetaSummaryPath) {
    $performanceBetaLines = Get-Content -Path $performanceBetaSummaryPath
    foreach ($line in $performanceBetaLines) {
        if ($line -eq "# AETHER Performance Beta Gate") {
            continue
        }
        $summaryLines.Add($line)
    }
} else {
    $summaryLines.Add("Performance beta summary was not generated.")
}
$summaryLines.Add("")
$summaryLines.Add("## Security And Key Lifecycle Gate")
$summaryLines.Add("")
if (Test-Path $securityKeySummaryPath) {
    $securityKeyLines = Get-Content -Path $securityKeySummaryPath
    foreach ($line in $securityKeyLines) {
        if ($line -eq "# AETHER Security And Key Lifecycle Gate") {
            continue
        }
        $summaryLines.Add($line)
    }
} else {
    $summaryLines.Add("Security and key lifecycle summary was not generated.")
}
$summaryLines.Add("")
$summaryLines.Add("## Service V2 Operability Proof")
$summaryLines.Add("")
if (Test-Path $serviceV2SummaryPath) {
    $serviceLines = Get-Content -Path $serviceV2SummaryPath
    foreach ($line in $serviceLines) {
        if ($line -eq "# AETHER Service v2 Operability Proof") {
            continue
        }
        $summaryLines.Add($line)
    }
} else {
    $summaryLines.Add("Service v2 operability summary was not generated.")
}
$summaryLines.Add("")
$summaryLines.Add("## Release Rollback Record")
$summaryLines.Add("")
if (Test-Path $rollbackSummaryPath) {
    $rollbackLines = Get-Content -Path $rollbackSummaryPath
    foreach ($line in $rollbackLines) {
        if ($line -eq "# AETHER Release Rollback Record") {
            continue
        }
        $summaryLines.Add($line)
    }
} else {
    $summaryLines.Add("Release rollback record was not generated.")
}
$summaryLines.Add("")
$summaryLines.Add("## Customer Workflow Acceptance")
$summaryLines.Add("")
if (Test-Path $customerWorkflowSummaryPath) {
    $customerWorkflowLines = Get-Content -Path $customerWorkflowSummaryPath
    foreach ($line in $customerWorkflowLines) {
        if ($line -eq "# AETHER Customer Workflow Acceptance") {
            continue
        }
        $summaryLines.Add($line)
    }
} else {
    $summaryLines.Add("Customer workflow acceptance summary was not generated.")
}
$summaryLines.Add("")
$summaryLines.Add("## Commercial Release Readiness")
$summaryLines.Add("")
if (Test-Path $commercialReadinessSummaryPath) {
    $commercialLines = Get-Content -Path $commercialReadinessSummaryPath
    foreach ($line in $commercialLines) {
        if ($line -eq "# AETHER Commercial Release Readiness") {
            continue
        }
        $summaryLines.Add($line)
    }
} else {
    $summaryLines.Add("Commercial release readiness summary was not generated.")
}
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

if (-not $EvidenceManifestPath) {
    $EvidenceManifestPath = Join-Path $reportDir "release-readiness-evidence-$outputTimestamp.json"
} else {
    $EvidenceManifestPath = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($EvidenceManifestPath)
}
$manifestParent = Split-Path -Parent $EvidenceManifestPath
if ($manifestParent) {
    New-Item -ItemType Directory -Force -Path $manifestParent | Out-Null
}
$treeSha = if ($git) { (& $git.Source -C $repoRoot rev-parse "HEAD^{tree}").Trim() } else { "" }
$manifestStatus = if ($failed) { "failed" } else { "passed" }
$readinessOutputPaths = [ordered]@{
    performance_beta = $performanceBetaJsonPath
    service_operability = $serviceV2JsonPath
    rollback = $rollbackJsonPath
    customer_workflow = $customerWorkflowJsonPath
    security_lifecycle = $securityKeyJsonPath
    commercial_policy = $commercialReadinessJsonPath
    package_file_manifest = Join-Path $securityKeyProofDir "pilot-package-file-manifest.json"
    readiness_transcript = $transcriptPath
    pilot_launch_transcript = $pilotLaunchArtifactPath
}
$readinessOutputs = [ordered]@{}
foreach ($output in $readinessOutputPaths.GetEnumerator()) {
    if ($output.Value -and (Test-Path -LiteralPath $output.Value -PathType Leaf)) {
        $readinessOutputs[$output.Key] = Get-FileReceipt $output.Value
    } elseif (-not $failed) {
        throw "Required immutable readiness output is missing: $($output.Value)"
    }
}
$readinessEvidence = [ordered]@{
    schema_version = "aether.release-readiness-evidence.v1"
    status = $manifestStatus
    failure = if ($failed) { $failureMessage } else { $null }
    candidate = [ordered]@{
        commit_sha = $commit
        tree_sha = $treeSha
        ref = $CandidateRef
    }
    workflow = [ordered]@{
        run_id = [string]$env:GITHUB_RUN_ID
        attempt = if ($env:GITHUB_RUN_ATTEMPT) { [int]$env:GITHUB_RUN_ATTEMPT } else { 1 }
    }
    package = [ordered]@{
        path = [System.IO.Path]::GetRelativePath($repoRoot, $pilotPackageZip).Replace('\', '/')
        sha256 = if (Test-Path -LiteralPath $pilotPackageZip) { (Get-FileHash -LiteralPath $pilotPackageZip -Algorithm SHA256).Hash.ToLowerInvariant() } else { "" }
    }
    outputs = $readinessOutputs
}
$readinessEvidence | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $EvidenceManifestPath

Write-Host ""
Write-Host "Release-readiness transcript: $transcriptPath"
Write-Host "Latest transcript:            $latestTranscriptPath"
Write-Host "Release-readiness summary:    $summaryPath"
Write-Host "Latest summary:               $latestSummaryPath"
Write-Host "Immutable evidence manifest:  $EvidenceManifestPath"

if ($failed) {
    Close-Runner 1
}

Write-Host "Release-readiness suite completed successfully." -ForegroundColor Green
Close-Runner 0
