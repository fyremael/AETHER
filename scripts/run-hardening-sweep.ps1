param(
    [string[]]$Packs = @("admin", "operator", "user", "exec"),
    [switch]$PauseOnExit
)

$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$outputTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$reportDir = Join-Path $repoRoot "artifacts\qa\hardening"
$runDir = Join-Path $reportDir ("runs\" + $outputTimestamp)
$summaryPath = Join-Path $reportDir ("hardening-" + $outputTimestamp + ".md")
$jsonPath = Join-Path $reportDir ("hardening-" + $outputTimestamp + ".json")
$latestSummaryPath = Join-Path $reportDir "latest.md"
$latestJsonPath = Join-Path $reportDir "latest.json"
$pagesPreviewDir = Join-Path $repoRoot "artifacts\pages-preview-hardening"
$results = [System.Collections.Generic.List[object]]::new()
$transcript = [System.Collections.Generic.List[string]]::new()
$normalizedPacks = $Packs | ForEach-Object { $_.Trim().ToLowerInvariant() } | Where-Object { $_ }
$script:hasFailures = $false

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

function New-ArtifactDir([string]$PackName) {
    $path = Join-Path $runDir $PackName
    New-Item -ItemType Directory -Force -Path $path | Out-Null
    $path
}

function New-ArtifactPath([string]$PackName, [string]$Name, [string]$Extension = "txt") {
    $dir = New-ArtifactDir $PackName
    Join-Path $dir ("{0}.{1}" -f $Name, $Extension)
}

function Get-CommandPath([string]$Name, [switch]$Required) {
    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if (-not $command) {
        if ($Required) {
            throw "Required command not found on PATH: $Name"
        }
        return $null
    }
    $command.Source
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

function Invoke-CapturedCommand {
    param(
        [string]$PackName,
        [string]$Label,
        [string]$Command,
        [string[]]$Arguments,
        [string]$WorkingDirectory = $repoRoot,
        [switch]$AllowFailure
    )

    $outputPath = New-ArtifactPath $PackName ($Label -replace '[^A-Za-z0-9\-_]+', '-')
    $commandText = Format-CommandText $Command $Arguments $WorkingDirectory
    Add-TranscriptLine("## $Label")
    Add-TranscriptLine("")
    Add-TranscriptLine("Command: $commandText")
    Add-TranscriptLine("")
    Add-TranscriptLine('```text')

    Write-Host ""
    Write-Host "[$PackName] $Label" -ForegroundColor Cyan
    Write-Host "Running: $commandText"

    Push-Location $WorkingDirectory
    try {
        & $Command @Arguments *>&1 | Tee-Object -FilePath $outputPath | Out-Host
        $exitCode = if ($null -ne $LASTEXITCODE) { $LASTEXITCODE } else { 0 }
    } finally {
        Pop-Location
    }

    $outputText = if (Test-Path $outputPath) {
        Get-Content -Path $outputPath -Raw
    } else {
        ""
    }
    foreach ($line in ($outputText -split "`r?`n")) {
        if ($line -ne "") {
            Add-TranscriptLine($line)
        }
    }
    Add-TranscriptLine('```')
    Add-TranscriptLine("")

    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "Command failed ($exitCode): $commandText"
    }

    [pscustomobject]@{
        ExitCode = $exitCode
        OutputPath = $outputPath
        OutputText = $outputText
        CommandText = $commandText
    }
}

function Invoke-JsonRequest {
    param(
        [string]$Method,
        [string]$Url,
        [string]$Token,
        $Body
    )

    $headers = @{ Accept = "application/json" }
    if ($Token) {
        $headers["Authorization"] = "Bearer $Token"
    }

    $params = @{
        Uri = $Url
        Method = $Method
        Headers = $headers
        SkipHttpErrorCheck = $true
    }

    if ($null -ne $Body) {
        $params["Body"] = ($Body | ConvertTo-Json -Depth 16)
        $params["ContentType"] = "application/json"
    }

    $response = Invoke-WebRequest @params
    $parsed = $null
    if ($response.Content) {
        try {
            $parsed = $response.Content | ConvertFrom-Json -Depth 32
        } catch {
            $parsed = $response.Content
        }
    }

    [pscustomobject]@{
        StatusCode = [int]$response.StatusCode
        RawBody = [string]$response.Content
        Body = $parsed
    }
}

function Wait-ForHealth {
    param(
        [string]$BaseUrl,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-JsonRequest -Method "GET" -Url "$BaseUrl/health" -Token "" -Body $null
            if ($response.StatusCode -eq 200) {
                return
            }
        } catch {
        }
        Start-Sleep -Seconds 1
    }

    throw "Service at $BaseUrl did not become healthy within $TimeoutSeconds seconds"
}

function Get-FreeTcpPort {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try {
        $listener.LocalEndpoint.Port
    } finally {
        $listener.Stop()
    }
}

function Start-ServiceProcess {
    param(
        [string]$PackName,
        [string]$WorkingDirectory,
        [string]$CommandPath,
        [string[]]$Arguments = @()
    )

    $stdoutPath = New-ArtifactPath $PackName "service-stdout"
    $stderrPath = New-ArtifactPath $PackName "service-stderr"
    $process = Start-Process `
        -FilePath $CommandPath `
        -ArgumentList $Arguments `
        -WorkingDirectory $WorkingDirectory `
        -PassThru `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath

    [pscustomobject]@{
        Process = $process
        StdoutPath = $stdoutPath
        StderrPath = $stderrPath
    }
}

function Stop-ServiceProcess($ServiceHandle) {
    if ($null -eq $ServiceHandle -or $null -eq $ServiceHandle.Process) {
        return
    }

    if (-not $ServiceHandle.Process.HasExited) {
        Stop-Process -Id $ServiceHandle.Process.Id -Force
        $ServiceHandle.Process.WaitForExit()
    }
}

function Assert-Contains([string]$Text, [string]$Expected, [string]$Context) {
    if ($Text -notmatch [regex]::Escape($Expected)) {
        throw "$Context did not contain expected text: $Expected"
    }
}

function Assert-PathExists([string]$Path, [string]$Context) {
    if (-not (Test-Path $Path)) {
        throw "$Context expected path to exist: $Path"
    }
}

function Assert-StatusCode($Response, [int[]]$ExpectedStatusCodes, [string]$Context) {
    if ($ExpectedStatusCodes -notcontains [int]$Response.StatusCode) {
        throw "$Context returned status $($Response.StatusCode); expected $($ExpectedStatusCodes -join ', ')"
    }
}

function Add-Result {
    param(
        [string]$Persona,
        [string]$Surface,
        [string]$Severity,
        [string]$Title,
        [string]$Status,
        [string]$ReproCommand,
        [string]$ArtifactPath,
        [string]$Notes
    )

    $results.Add([pscustomobject]@{
            persona = $Persona
            surface = $Surface
            severity = $Severity
            title = $Title
            status = $Status
            repro_command = $ReproCommand
            artifact_path = $ArtifactPath
            notes = $Notes
        })
}

function Add-SkippedResult {
    param(
        [string]$Persona,
        [string]$Surface,
        [string]$Title,
        [string]$ReproCommand,
        [string]$Notes
    )

    Add-Result `
        -Persona $Persona `
        -Surface $Surface `
        -Severity "observational" `
        -Title $Title `
        -Status "skipped" `
        -ReproCommand $ReproCommand `
        -ArtifactPath "" `
        -Notes $Notes
}

function Invoke-HardeningCheck {
    param(
        [string]$Persona,
        [string]$Surface,
        [string]$Title,
        [string]$ReproCommand,
        [string]$FailureSeverity,
        [scriptblock]$Action
    )

    try {
        $result = & $Action
        $artifactPath = ""
        $notes = "completed successfully"
        if ($null -ne $result) {
            if ($result.PSObject.Properties["ArtifactPath"]) {
                $artifactPath = [string]$result.ArtifactPath
            }
            if ($result.PSObject.Properties["Notes"]) {
                $notes = [string]$result.Notes
            }
        }
        Add-Result `
            -Persona $Persona `
            -Surface $Surface `
            -Severity "observational" `
            -Title $Title `
            -Status "passed" `
            -ReproCommand $ReproCommand `
            -ArtifactPath $artifactPath `
            -Notes $notes
    } catch {
        $script:hasFailures = $true
        Write-Host "Check failed: $Title" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
        Add-Result `
            -Persona $Persona `
            -Surface $Surface `
            -Severity $FailureSeverity `
            -Title $Title `
            -Status "failed" `
            -ReproCommand $ReproCommand `
            -ArtifactPath "" `
            -Notes $_.Exception.Message
    }
}

function New-TestDatom([uint64]$Element, [string]$Value) {
    @{
        entity = 1
        attribute = 1
        value = @{ String = $Value }
        op = "Assert"
        element = $Element
        replica = 1
        causal_context = @{ frontier = @() }
        provenance = @{
            author_principal = ""
            agent_id = ""
            tool_id = ""
            session_id = ""
            source_ref = @{
                uri = ""
                digest = $null
            }
            parent_datom_ids = @()
            confidence = 0.0
            trust_domain = ""
            schema_version = ""
        }
        policy = $null
    }
}

function Invoke-AdminPack {
    if (-not $IsWindows) {
        Add-SkippedResult `
            -Persona "admin" `
            -Surface "admin" `
            -Title "Admin package and recovery drills" `
            -ReproCommand "pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/run-hardening-sweep.ps1 -Packs admin" `
            -Notes "admin pack is Windows-only in phase one"
        return
    }

    $pwshPath = Get-CommandPath -Name "pwsh" -Required
    $port = Get-FreeTcpPort
    $packageRoot = Join-Path $runDir "admin\package\aether-pilot-service-windows-x86_64"
    $zipPath = "$packageRoot.zip"
    $unpackedRoot = Join-Path $runDir "admin\unpacked"
    $baseUrl = "http://127.0.0.1:$port"
    $packageTokenPath = Join-Path $packageRoot "config\pilot-operator.token"
    $packageConfigPath = Join-Path $packageRoot "config\pilot-service.json"
    $packageBinaryPath = Join-Path $packageRoot "bin\aether_pilot_service.exe"
    $runScriptPath = Join-Path $packageRoot "run-pilot-service.ps1"
    $backupScriptPath = Join-Path $packageRoot "backup-pilot-state.ps1"
    $restoreScriptPath = Join-Path $packageRoot "restore-pilot-state.ps1"
    $rotateScriptPath = Join-Path $packageRoot "rotate-pilot-token.ps1"

    Invoke-HardeningCheck `
        -Persona "admin" `
        -Surface "admin" `
        -Title "Packaged pilot bundle builds, boots, rotates, and restores" `
        -ReproCommand "pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/build-pilot-package.ps1 -OutputDir $packageRoot -BindAddr 127.0.0.1:$port" `
        -FailureSeverity "critical" `
        -Action {
            $serviceHandle = $null
            New-Item -ItemType Directory -Force -Path (Split-Path -Parent $packageRoot) | Out-Null
            try {
                $build = Invoke-CapturedCommand `
                    -PackName "admin" `
                    -Label "build-package" `
                    -Command $pwshPath `
                    -Arguments @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $repoRoot "scripts/build-pilot-package.ps1"), "-OutputDir", $packageRoot, "-BindAddr", "127.0.0.1:$port")

                Assert-PathExists $zipPath "pilot package zip"
                if (Test-Path $unpackedRoot) {
                    Remove-Item -Recurse -Force $unpackedRoot
                }
                Expand-Archive -Path $zipPath -DestinationPath $unpackedRoot -Force
                Assert-PathExists (Join-Path $unpackedRoot "bin\aether_pilot_service.exe") "expanded pilot bundle"
                Assert-PathExists $runScriptPath "packaged launch script"

                $serviceHandle = Start-ServiceProcess `
                    -PackName "admin" `
                    -WorkingDirectory $packageRoot `
                    -CommandPath $packageBinaryPath `
                    -Arguments @("--config", $packageConfigPath)
                Wait-ForHealth -BaseUrl $baseUrl

                $token = (Get-Content -Path $packageTokenPath -Raw).Trim()
                $health = Invoke-JsonRequest -Method "GET" -Url "$baseUrl/health" -Token "" -Body $null
                Assert-StatusCode $health @(200) "/health"

                $status = Invoke-JsonRequest -Method "GET" -Url "$baseUrl/v1/status" -Token $token -Body $null
                Assert-StatusCode $status @(200) "/v1/status"
                if ($status.Body.config_version -ne "pilot-v1") {
                    throw "unexpected config version in packaged status response: $($status.Body.config_version)"
                }

                $appendOne = Invoke-JsonRequest -Method "POST" -Url "$baseUrl/v1/append" -Token $token -Body @{
                    datoms = @(
                        (New-TestDatom -Element 1 -Value "hardening-alpha")
                    )
                }
                Assert-StatusCode $appendOne @(200) "first packaged append"

                $historyAfterFirstAppend = Invoke-JsonRequest -Method "GET" -Url "$baseUrl/v1/history" -Token $token -Body $null
                Assert-StatusCode $historyAfterFirstAppend @(200) "/v1/history after first append"
                if ($historyAfterFirstAppend.Body.datoms.Count -ne 1) {
                    throw "expected one datom after first append, found $($historyAfterFirstAppend.Body.datoms.Count)"
                }

                Stop-ServiceProcess $serviceHandle
                $serviceHandle = $null

                $snapshotDir = Join-Path $runDir "admin\snapshot"
                $backup = Invoke-CapturedCommand `
                    -PackName "admin" `
                    -Label "backup-pilot-state" `
                    -Command $pwshPath `
                    -Arguments @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $backupScriptPath, "-SnapshotDir", $snapshotDir)
                Assert-PathExists (Join-Path $snapshotDir "manifest.json") "pilot snapshot manifest"

                $serviceHandle = Start-ServiceProcess `
                    -PackName "admin" `
                    -WorkingDirectory $packageRoot `
                    -CommandPath $packageBinaryPath `
                    -Arguments @("--config", $packageConfigPath)
                Wait-ForHealth -BaseUrl $baseUrl

                $appendTwo = Invoke-JsonRequest -Method "POST" -Url "$baseUrl/v1/append" -Token $token -Body @{
                    datoms = @(
                        (New-TestDatom -Element 2 -Value "hardening-beta")
                    )
                }
                Assert-StatusCode $appendTwo @(200) "second packaged append"

                $historyBeforeRestore = Invoke-JsonRequest -Method "GET" -Url "$baseUrl/v1/history" -Token $token -Body $null
                Assert-StatusCode $historyBeforeRestore @(200) "/v1/history before restore"
                if ($historyBeforeRestore.Body.datoms.Count -ne 2) {
                    throw "expected two datoms before restore, found $($historyBeforeRestore.Body.datoms.Count)"
                }

                $oldToken = $token
                $rotate = Invoke-CapturedCommand `
                    -PackName "admin" `
                    -Label "rotate-token" `
                    -Command $pwshPath `
                    -Arguments @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $rotateScriptPath, "-TokenPath", $packageTokenPath)
                $newToken = (Get-Content -Path $packageTokenPath -Raw).Trim()
                if ($newToken -eq $oldToken) {
                    throw "token rotation did not change the packaged token value"
                }

                $reload = Invoke-JsonRequest -Method "POST" -Url "$baseUrl/v1/admin/auth/reload" -Token $oldToken -Body @{}
                Assert-StatusCode $reload @(200) "/v1/admin/auth/reload"

                $rotatedStatus = Invoke-JsonRequest -Method "GET" -Url "$baseUrl/v1/status" -Token $newToken -Body $null
                Assert-StatusCode $rotatedStatus @(200) "/v1/status with rotated token"
                $oldTokenHistory = Invoke-JsonRequest -Method "GET" -Url "$baseUrl/v1/history" -Token $oldToken -Body $null
                Assert-StatusCode $oldTokenHistory @(401, 403) "/v1/history with old token"

                Stop-ServiceProcess $serviceHandle
                $serviceHandle = $null
                Start-Sleep -Seconds 1

                $restore = Invoke-CapturedCommand `
                    -PackName "admin" `
                    -Label "restore-pilot-state" `
                    -Command $pwshPath `
                    -Arguments @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $restoreScriptPath, "-SnapshotDir", $snapshotDir)

                $serviceHandle = Start-ServiceProcess `
                    -PackName "admin" `
                    -WorkingDirectory $packageRoot `
                    -CommandPath $packageBinaryPath `
                    -Arguments @("--config", $packageConfigPath)
                Wait-ForHealth -BaseUrl $baseUrl

                $restoredToken = (Get-Content -Path $packageTokenPath -Raw).Trim()
                $candidateTokens = @($restoredToken, $newToken, $oldToken) | Where-Object { $_ } | Select-Object -Unique
                $restoredHistory = $null
                foreach ($candidate in $candidateTokens) {
                    $attempt = Invoke-JsonRequest -Method "GET" -Url "$baseUrl/v1/history" -Token $candidate -Body $null
                    if ($attempt.StatusCode -eq 200) {
                        $restoredHistory = $attempt
                        break
                    }
                }
                if ($null -eq $restoredHistory) {
                    throw "no packaged token could read history after restore/restart"
                }
                if ($restoredHistory.Body.datoms.Count -ne 1) {
                    throw "expected one datom after restore/restart, found $($restoredHistory.Body.datoms.Count)"
                }

                [pscustomobject]@{
                    ArtifactPath = $build.OutputPath
                    Notes = "built package, expanded zip, rotated auth, and verified backup/restore via packaged restart"
                }
            } finally {
                Stop-ServiceProcess $serviceHandle
            }
        }

    Invoke-HardeningCheck `
        -Persona "admin" `
        -Surface "admin" `
        -Title "Config failures are explicit for missing token sources, stale paths, and bad commands" `
        -ReproCommand "`"$packageBinaryPath`" --config <bad-config>" `
        -FailureSeverity "high" `
        -Action {
            Assert-PathExists $packageConfigPath "packaged config"
            $badConfigDir = Join-Path $runDir "admin\bad-configs"
            New-Item -ItemType Directory -Force -Path $badConfigDir | Out-Null

            $missingSource = Get-Content -Path $packageConfigPath -Raw | ConvertFrom-Json -AsHashtable -Depth 16
            $missingSource.auth.tokens[0].token = $null
            $missingSource.auth.tokens[0].token_env = $null
            $missingSource.auth.tokens[0].token_file = $null
            $missingSource.auth.tokens[0].token_command = $null
            $missingSourcePath = Join-Path $badConfigDir "missing-token-source.json"
            $missingSource | ConvertTo-Json -Depth 16 | Set-Content -Path $missingSourcePath

            $stalePath = Get-Content -Path $packageConfigPath -Raw | ConvertFrom-Json -AsHashtable -Depth 16
            $stalePath.auth.tokens[0].token = $null
            $stalePath.auth.tokens[0].token_env = $null
            $stalePath.auth.tokens[0].token_file = "missing.token"
            $stalePath.auth.tokens[0].token_command = $null
            $stalePathConfig = Join-Path $badConfigDir "stale-token-path.json"
            $stalePath | ConvertTo-Json -Depth 16 | Set-Content -Path $stalePathConfig

            $badCommand = Get-Content -Path $packageConfigPath -Raw | ConvertFrom-Json -AsHashtable -Depth 16
            $badCommand.auth.tokens[0].token = $null
            $badCommand.auth.tokens[0].token_env = $null
            $badCommand.auth.tokens[0].token_file = $null
            $badCommand.auth.tokens[0].token_command = @($pwshPath, "-NoProfile", "-Command", "Write-Error 'hardening token failure'; exit 9")
            $badCommandPath = Join-Path $badConfigDir "bad-token-command.json"
            $badCommand | ConvertTo-Json -Depth 16 | Set-Content -Path $badCommandPath

            $missingRun = Invoke-CapturedCommand `
                -PackName "admin" `
                -Label "missing-token-source-config" `
                -Command $packageBinaryPath `
                -Arguments @("--config", $missingSourcePath) `
                -AllowFailure
            if ($missingRun.ExitCode -eq 0) {
                throw "missing token-source config unexpectedly succeeded"
            }
            Assert-Contains $missingRun.OutputText "exactly one token source" "missing token-source config output"

            $staleRun = Invoke-CapturedCommand `
                -PackName "admin" `
                -Label "stale-token-path-config" `
                -Command $packageBinaryPath `
                -Arguments @("--config", $stalePathConfig) `
                -AllowFailure
            if ($staleRun.ExitCode -eq 0) {
                throw "stale token path config unexpectedly succeeded"
            }
            Assert-Contains $staleRun.OutputText "ReadTokenFile" "stale token path output"

            $badCommandRun = Invoke-CapturedCommand `
                -PackName "admin" `
                -Label "bad-token-command-config" `
                -Command $packageBinaryPath `
                -Arguments @("--config", $badCommandPath) `
                -AllowFailure
            if ($badCommandRun.ExitCode -eq 0) {
                throw "bad token command config unexpectedly succeeded"
            }
            if (
                ($badCommandRun.OutputText -notmatch "TokenCommandFailed") -and
                ($badCommandRun.OutputText -notmatch "RunTokenCommand")
            ) {
                throw "bad token command output did not report a command-backed token failure"
            }

            [pscustomobject]@{
                ArtifactPath = $badConfigDir
                Notes = "captured explicit failures for missing token source, stale token path, and non-zero token command"
            }
        }
}

function Invoke-OperatorPack {
    if (-not $IsWindows) {
        Add-SkippedResult `
            -Persona "operator" `
            -Surface "operator" `
            -Title "Operator report and TUI drills" `
            -ReproCommand "pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/run-hardening-sweep.ps1 -Packs operator" `
            -Notes "operator pack is Windows-first in phase one"
        return
    }

    $cargoPath = Get-CommandPath -Name "cargo" -Required
    $goPath = Get-CommandPath -Name "go" -Required

    Invoke-HardeningCheck `
        -Persona "operator" `
        -Surface "operator" `
        -Title "Operator report and delta artifacts stay coherent" `
        -ReproCommand "cargo run -p aether_api --example pilot_coordination_report --release && cargo run -p aether_api --example pilot_coordination_delta_report --release -- artifacts/pilot/coordination.sqlite" `
        -FailureSeverity "high" `
        -Action {
            $report = Invoke-CapturedCommand `
                -PackName "operator" `
                -Label "pilot-coordination-report" `
                -Command $cargoPath `
                -Arguments @("run", "-p", "aether_api", "--example", "pilot_coordination_report", "--release")
            $delta = Invoke-CapturedCommand `
                -PackName "operator" `
                -Label "pilot-coordination-delta" `
                -Command $cargoPath `
                -Arguments @("run", "-p", "aether_api", "--example", "pilot_coordination_delta_report", "--release", "--", "artifacts/pilot/coordination.sqlite")

            $latestReport = Join-Path $repoRoot "artifacts\pilot\reports\latest.md"
            $latestDelta = Join-Path $repoRoot "artifacts\pilot\reports\latest-delta.md"
            $latestReportJson = Join-Path $repoRoot "artifacts\pilot\reports\latest.json"
            $latestDeltaJson = Join-Path $repoRoot "artifacts\pilot\reports\latest-delta.json"

            Assert-PathExists $latestReport "latest coordination report"
            Assert-PathExists $latestDelta "latest coordination delta report"
            Assert-PathExists $latestReportJson "latest coordination report json"
            Assert-PathExists $latestDeltaJson "latest coordination delta json"

            $reportText = Get-Content -Path $latestReport -Raw
            $deltaText = Get-Content -Path $latestDelta -Raw
            Assert-Contains $reportText "# AETHER Coordination Pilot Report" "coordination report markdown"
            Assert-Contains $reportText "## Proof Trace" "coordination report markdown"
            Assert-Contains $reportText "Authorization At Current" "coordination report markdown"
            Assert-Contains $deltaText "# AETHER Coordination Delta Report" "coordination delta markdown"
            Assert-Contains $deltaText "Claimable Work Delta" "coordination delta markdown"
            Assert-Contains $deltaText "Rejected Outcomes Delta" "coordination delta markdown"

            [pscustomobject]@{
                ArtifactPath = $latestReport
                Notes = "regenerated report and delta artifacts with the expected operator-facing sections"
            }
        }

    Invoke-HardeningCheck `
        -Persona "operator" `
        -Surface "operator" `
        -Title "Policy-aware reports, redaction, replay, and explain remain covered" `
        -ReproCommand "cargo test -p aether_api --test http_service authenticated_http_service_exposes_policy_aware_coordination_reports coordination_delta_report_endpoint_is_policy_aware -- --exact --nocapture" `
        -FailureSeverity "high" `
        -Action {
            $policyReports = Invoke-CapturedCommand `
                -PackName "operator" `
                -Label "policy-aware-coordination-report-test" `
                -Command $cargoPath `
                -Arguments @("test", "-p", "aether_api", "--test", "http_service", "authenticated_http_service_exposes_policy_aware_coordination_reports", "--", "--exact", "--nocapture")
            $deltaPolicy = Invoke-CapturedCommand `
                -PackName "operator" `
                -Label "policy-aware-delta-report-test" `
                -Command $cargoPath `
                -Arguments @("test", "-p", "aether_api", "--test", "http_service", "coordination_delta_report_endpoint_is_policy_aware", "--", "--exact", "--nocapture")

            [pscustomobject]@{
                ArtifactPath = $deltaPolicy.OutputPath
                Notes = "ran the policy-aware report and delta HTTP tests that cover redaction, replay, and explain visibility"
            }
        }

    Invoke-HardeningCheck `
        -Persona "operator" `
        -Surface "operator" `
        -Title "Operator TUI startup smoke stays green" `
        -ReproCommand "(cd go && go test ./internal/tui -run TestStartupLoadsOverviewData -count=1)" `
        -FailureSeverity "medium" `
        -Action {
            $tui = Invoke-CapturedCommand `
                -PackName "operator" `
                -Label "go-tui-smoke" `
                -Command $goPath `
                -Arguments @("test", "./internal/tui", "-run", "TestStartupLoadsOverviewData", "-count=1") `
                -WorkingDirectory (Join-Path $repoRoot "go")

            [pscustomobject]@{
                ArtifactPath = $tui.OutputPath
                Notes = "verified the read-only operator cockpit still performs its startup overview load"
            }
        }
}

function Invoke-UserPack {
    $pythonPath = Get-CommandPath -Name "python" -Required
    $goPath = Get-CommandPath -Name "go" -Required

    Invoke-HardeningCheck `
        -Persona "user" `
        -Surface "sdk/http" `
        -Title "Go boundary client contract checks stay green" `
        -ReproCommand "(cd go && go test ./...)" `
        -FailureSeverity "high" `
        -Action {
            $goTests = Invoke-CapturedCommand `
                -PackName "user" `
                -Label "go-boundary-tests" `
                -Command $goPath `
                -Arguments @("test", "./...") `
                -WorkingDirectory (Join-Path $repoRoot "go")
            [pscustomobject]@{
                ArtifactPath = $goTests.OutputPath
                Notes = "ran the full Go shell and typed client suite"
            }
        }

    Invoke-HardeningCheck `
        -Persona "user" `
        -Surface "sdk/http" `
        -Title "Python boundary client and notebook smoke checks stay green" `
        -ReproCommand "python -m unittest discover python/tests -v" `
        -FailureSeverity "high" `
        -Action {
            $pythonTests = Invoke-CapturedCommand `
                -PackName "user" `
                -Label "python-sdk-tests" `
                -Command $pythonPath `
                -Arguments @("-m", "unittest", "discover", "python/tests", "-v")
            [pscustomobject]@{
                ArtifactPath = $pythonTests.OutputPath
                Notes = "ran the full Python SDK suite, including notebook and malformed-input smoke coverage"
            }
        }

    Invoke-HardeningCheck `
        -Persona "user" `
        -Surface "demo/docs" `
        -Title "Onboarding docs and example paths still resolve" `
        -ReproCommand "Test-Path README.md docs/README.md python/notebooks/README.md examples/demo-04-governed-incident-blackboard.md" `
        -FailureSeverity "medium" `
        -Action {
            $requiredPaths = @(
                (Join-Path $repoRoot "README.md"),
                (Join-Path $repoRoot "docs\README.md"),
                (Join-Path $repoRoot "python\README.md"),
                (Join-Path $repoRoot "python\notebooks\README.md"),
                (Join-Path $repoRoot "examples\demo-04-governed-incident-blackboard.md")
            )
            foreach ($path in $requiredPaths) {
                Assert-PathExists $path "user onboarding path"
            }

            $readme = Get-Content -Path (Join-Path $repoRoot "python\notebooks\README.md") -Raw
            Assert-Contains $readme "04_governed_incident_blackboard.ipynb" "notebook README"
            Assert-Contains $readme "Open" "notebook README"

            [pscustomobject]@{
                ArtifactPath = (Join-Path $repoRoot "python\notebooks\README.md")
                Notes = "validated the main boundary-client and notebook onboarding entry points"
            }
        }
}

function Invoke-ExecPack {
    $cargoPath = Get-CommandPath -Name "cargo" -Required
    $pythonPath = Get-CommandPath -Name "python" -Required

    Invoke-HardeningCheck `
        -Persona "exec" `
        -Surface "demo/docs" `
        -Title "Demo 03 still tells the coordination proof story" `
        -ReproCommand "cargo run -p aether_api --example demo_03_coordination_situation_room" `
        -FailureSeverity "medium" `
        -Action {
            $demo = Invoke-CapturedCommand `
                -PackName "exec" `
                -Label "demo-03-coordination-situation-room" `
                -Command $cargoPath `
                -Arguments @("run", "-p", "aether_api", "--example", "demo_03_coordination_situation_room")
            Assert-Contains $demo.OutputText "Act I: Before completion closes the chain (AsOf e2)" "Demo 03 output"
            Assert-Contains $demo.OutputText "Act IV: Worker B takes over (Current)" "Demo 03 output"
            Assert-Contains $demo.OutputText "Fenced stale work at Current" "Demo 03 output"
            Assert-Contains $demo.OutputText "Bottom line:" "Demo 03 output"

            [pscustomobject]@{
                ArtifactPath = $demo.OutputPath
                Notes = "validated readiness, handoff, stale fencing, and proof in the flagship coordination showcase"
            }
        }

    Invoke-HardeningCheck `
        -Persona "exec" `
        -Surface "demo/docs" `
        -Title "Demo 04 still tells the governed incident blackboard story" `
        -ReproCommand "cargo run -p aether_api --example demo_04_governed_incident_blackboard" `
        -FailureSeverity "medium" `
        -Action {
            $demo = Invoke-CapturedCommand `
                -PackName "exec" `
                -Label "demo-04-governed-incident-blackboard" `
                -Command $cargoPath `
                -Arguments @("run", "-p", "aether_api", "--example", "demo_04_governed_incident_blackboard")
            Assert-Contains $demo.OutputText "Act I: Active observations on the board (Current)" "Demo 04 output"
            Assert-Contains $demo.OutputText "Act II: Which action is actually ready? (AsOf e15)" "Demo 04 output"
            Assert-Contains $demo.OutputText "Act IV: The same board before the handoff (AsOf e18)" "Demo 04 output"
            Assert-Contains $demo.OutputText "Fenced stale attempts at Current" "Demo 04 output"
            Assert-Contains $demo.OutputText "Act V: Why the current authorization is true" "Demo 04 output"

            [pscustomobject]@{
                ArtifactPath = $demo.OutputPath
                Notes = "validated observations, ready action, handoff, stale fencing, and proof in the product exemplar"
            }
        }

    Invoke-HardeningCheck `
        -Persona "exec" `
        -Surface "demo/docs" `
        -Title "Pages preview and front-door narrative links stay intact" `
        -ReproCommand "python scripts/build_pages.py --out-dir artifacts/pages-preview-hardening" `
        -FailureSeverity "medium" `
        -Action {
            $docs = Invoke-CapturedCommand `
                -PackName "exec" `
                -Label "cargo-docs-for-pages-preview" `
                -Command $cargoPath `
                -Arguments @("doc", "--workspace", "--no-deps")
            $pages = Invoke-CapturedCommand `
                -PackName "exec" `
                -Label "pages-preview-hardening" `
                -Command $pythonPath `
                -Arguments @("scripts/build_pages.py", "--out-dir", $pagesPreviewDir)

            $indexPath = Join-Path $pagesPreviewDir "index.html"
            $incidentPath = Join-Path $pagesPreviewDir "incident-blackboard.html"
            Assert-PathExists $indexPath "Pages preview index"
            Assert-PathExists $incidentPath "Pages preview incident blackboard page"

            $indexText = Get-Content -Path $indexPath -Raw
            $incidentText = Get-Content -Path $incidentPath -Raw
            $readmeText = ((Get-Content -Path (Join-Path $repoRoot "README.md") -Raw) -replace "\s+", " ").Trim()
            Assert-Contains $indexText "Open Incident Blackboard" "Pages index"
            Assert-Contains $indexText "education.html" "Pages index"
            Assert-Contains $incidentText "governed incident blackboard" "incident blackboard page"
            Assert-Contains $readmeText "governed shared workspace for agents and operators" "repository front door"

            [pscustomobject]@{
                ArtifactPath = $indexPath
                Notes = "rebuilt Rust docs plus Pages and verified the front door still leads with the flagship use-case story"
            }
        }
}

Write-Host ""
Write-Host "AETHER QA Hardening Sweep"
Write-Host "========================="
Write-Host "Generated: $timestamp"
Write-Host "Repository: $repoRoot"
Write-Host "Packs: $($normalizedPacks -join ', ')"
Write-Host ""

New-Item -ItemType Directory -Force -Path $reportDir, $runDir | Out-Null

$gitPath = Get-CommandPath -Name "git"
$commit = if ($gitPath) {
    (& $gitPath -C $repoRoot rev-parse HEAD).Trim()
} else {
    "<git unavailable>"
}

Add-TranscriptLine("AETHER QA Hardening Sweep")
Add-TranscriptLine("=========================")
Add-TranscriptLine("Generated: $timestamp")
Add-TranscriptLine("Repository: $repoRoot")
Add-TranscriptLine("Commit: $commit")
Add-TranscriptLine("Packs: $($normalizedPacks -join ', ')")
Add-TranscriptLine("")

foreach ($pack in $normalizedPacks) {
    switch ($pack) {
        "admin" { Invoke-AdminPack }
        "operator" { Invoke-OperatorPack }
        "user" { Invoke-UserPack }
        "exec" { Invoke-ExecPack }
        default {
            $script:hasFailures = $true
            Add-Result `
                -Persona $pack `
                -Surface "hardening" `
                -Severity "medium" `
                -Title "Unknown hardening pack requested" `
                -Status "failed" `
                -ReproCommand "pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/run-hardening-sweep.ps1 -Packs $pack" `
                -ArtifactPath "" `
                -Notes "unknown pack name: $pack"
        }
    }
}

$passed = @($results | Where-Object { $_.status -eq "passed" }).Count
$failed = @($results | Where-Object { $_.status -eq "failed" }).Count
$skipped = @($results | Where-Object { $_.status -eq "skipped" }).Count

$summaryLines = [System.Collections.Generic.List[string]]::new()
$summaryLines.Add("# AETHER QA Hardening Sweep")
$summaryLines.Add("")
$summaryLines.Add('- Generated: `' + $timestamp + '`')
$summaryLines.Add('- Commit: `' + $commit + '`')
$summaryLines.Add('- Packs: `' + ($normalizedPacks -join ', ') + '`')
$summaryLines.Add('- Run artifacts: `' + $runDir + '`')
$summaryLines.Add("")
$summaryLines.Add("## Totals")
$summaryLines.Add("")
$summaryLines.Add('- Passed: `' + $passed + '`')
$summaryLines.Add('- Failed: `' + $failed + '`')
$summaryLines.Add('- Skipped: `' + $skipped + '`')
$summaryLines.Add("")
$summaryLines.Add("## Results")
$summaryLines.Add("")
$summaryLines.Add("| Persona | Surface | Status | Severity | Title | Artifact |")
$summaryLines.Add("| --- | --- | --- | --- | --- | --- |")
foreach ($result in $results) {
    $artifact = if ($result.artifact_path) { $result.artifact_path } else { "-" }
    $summaryLines.Add("| $($result.persona) | $($result.surface) | $($result.status) | $($result.severity) | $($result.title) | $artifact |")
}
$summaryLines.Add("")
$summaryLines.Add("## Notes")
$summaryLines.Add("")
foreach ($result in $results) {
    $summaryLines.Add("- **$($result.title)**: $($result.notes)")
}

$summary = $summaryLines -join "`r`n"
$jsonBody = [pscustomobject]@{
    generated_at = $timestamp
    commit = $commit
    packs = $normalizedPacks
    results = $results
}

Set-Content -Path (Join-Path $runDir "transcript.txt") -Value $transcript
Set-Content -Path $summaryPath -Value $summary
Set-Content -Path $latestSummaryPath -Value $summary
$jsonBody | ConvertTo-Json -Depth 12 | Set-Content -Path $jsonPath
$jsonBody | ConvertTo-Json -Depth 12 | Set-Content -Path $latestJsonPath

Write-Host ""
Write-Host "Hardening summary: $summaryPath"
Write-Host "Latest summary:    $latestSummaryPath"
Write-Host "Hardening json:    $jsonPath"
Write-Host "Latest json:       $latestJsonPath"

if ($script:hasFailures) {
    Write-Host "QA hardening sweep completed with failures." -ForegroundColor Yellow
    Close-Runner 1
}

Write-Host "QA hardening sweep completed successfully." -ForegroundColor Green
Close-Runner 0
