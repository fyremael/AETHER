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

function Test-WindowsHost {
    if (Get-Variable -Name IsWindows -ErrorAction SilentlyContinue) {
        return [bool]$IsWindows
    }
    return [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [System.Runtime.InteropServices.OSPlatform]::Windows
    )
}

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

    $safeLabel = $Label -replace '[^A-Za-z0-9\-_]+', '-'
    $outputPath = New-ArtifactPath $PackName $safeLabel
    $stdoutPath = New-ArtifactPath $PackName "$safeLabel.stdout"
    $stderrPath = New-ArtifactPath $PackName "$safeLabel.stderr"
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
        $process = Start-Process `
            -FilePath $Command `
            -ArgumentList $Arguments `
            -WorkingDirectory $WorkingDirectory `
            -NoNewWindow `
            -Wait `
            -PassThru `
            -RedirectStandardOutput $stdoutPath `
            -RedirectStandardError $stderrPath
        $exitCode = $process.ExitCode
    } finally {
        Pop-Location
    }

    $outputLines = [System.Collections.Generic.List[string]]::new()
    foreach ($path in @($stdoutPath, $stderrPath)) {
        if (Test-Path $path) {
            foreach ($line in Get-Content -Path $path) {
                $outputLines.Add($line)
            }
        }
    }
    Set-Content -Path $outputPath -Value $outputLines

    $outputText = if (Test-Path $outputPath) {
        Get-Content -Path $outputPath -Raw
    } else {
        ""
    }
    foreach ($line in ($outputText -split "`r?`n")) {
        if ($line -ne "") {
            Write-Host $line
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
    }
    if ((Get-Command Invoke-WebRequest).Parameters.ContainsKey("SkipHttpErrorCheck")) {
        $params["SkipHttpErrorCheck"] = $true
    }
    if ((Get-Command Invoke-WebRequest).Parameters.ContainsKey("UseBasicParsing")) {
        $params["UseBasicParsing"] = $true
    }

    if ($null -ne $Body) {
        $params["Body"] = ($Body | ConvertTo-Json -Depth 16)
        $params["ContentType"] = "application/json"
    }

    $statusCode = $null
    $content = ""
    try {
        $response = Invoke-WebRequest @params
        $statusCode = [int]$response.StatusCode
        $content = [string]$response.Content
    } catch {
        $webResponse = $_.Exception.Response
        if ($null -eq $webResponse) {
            throw
        }
        $statusCode = [int]$webResponse.StatusCode
        $stream = $webResponse.GetResponseStream()
        if ($null -ne $stream) {
            $reader = [System.IO.StreamReader]::new($stream)
            try {
                $content = $reader.ReadToEnd()
            } finally {
                $reader.Dispose()
            }
        }
    }

    $parsed = $null
    if ($content) {
        try {
            if ((Get-Command ConvertFrom-Json).Parameters.ContainsKey("Depth")) {
                $parsed = $content | ConvertFrom-Json -Depth 32
            } else {
                $parsed = $content | ConvertFrom-Json
            }
        } catch {
            $parsed = $content
        }
    }

    [pscustomobject]@{
        StatusCode = $statusCode
        RawBody = $content
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

function Set-JsonProperty {
    param(
        [Parameter(Mandatory = $true)]$Object,
        [Parameter(Mandatory = $true)][string]$Name,
        $Value
    )

    if ($Object.PSObject.Properties[$Name]) {
        $Object.$Name = $Value
    } else {
        Add-Member -InputObject $Object -MemberType NoteProperty -Name $Name -Value $Value -Force
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
    if (-not (Test-WindowsHost)) {
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
    $operatorBinaryPath = Join-Path $packageRoot "bin\aetherctl.exe"
    $runScriptPath = Join-Path $packageRoot "run-pilot-service.ps1"
    $runCmdPath = Join-Path $packageRoot "run-pilot-service.cmd"
    $opsScriptPath = Join-Path $packageRoot "run-aether-ops.ps1"
    $opsCmdPath = Join-Path $packageRoot "run-aether-ops.cmd"
    $backupScriptPath = Join-Path $packageRoot "backup-pilot-state.ps1"
    $backupCmdPath = Join-Path $packageRoot "backup-pilot-state.cmd"
    $restoreScriptPath = Join-Path $packageRoot "restore-pilot-state.ps1"
    $restoreCmdPath = Join-Path $packageRoot "restore-pilot-state.cmd"
    $rotateScriptPath = Join-Path $packageRoot "rotate-pilot-token.ps1"
    $rotateCmdPath = Join-Path $packageRoot "rotate-pilot-token.cmd"

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
                Assert-PathExists (Join-Path $unpackedRoot "bin\aetherctl.exe") "expanded operator TUI binary"
                Assert-PathExists $runScriptPath "packaged launch script"
                Assert-PathExists $runCmdPath "packaged launch cmd"
                Assert-PathExists $operatorBinaryPath "packaged operator TUI binary"
                Assert-PathExists $opsScriptPath "packaged operator launch script"
                Assert-PathExists $opsCmdPath "packaged operator launch cmd"
                Assert-PathExists $backupScriptPath "packaged backup script"
                Assert-PathExists $backupCmdPath "packaged backup cmd"
                Assert-PathExists $restoreScriptPath "packaged restore script"
                Assert-PathExists $restoreCmdPath "packaged restore cmd"
                Assert-PathExists $rotateScriptPath "packaged token rotation script"
                Assert-PathExists $rotateCmdPath "packaged token rotation cmd"

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

                $schemaBeforeBackup = Invoke-JsonRequest -Method "GET" -Url "$baseUrl/v1/schema" -Token $token -Body $null
                Assert-StatusCode $schemaBeforeBackup @(200) "/v1/schema before backup"
                if (-not $schemaBeforeBackup.Body.active.schema_ref.digest) {
                    throw "active schema digest was missing before backup"
                }
                $receiptsBeforeBackup = Invoke-JsonRequest -Method "GET" -Url "$baseUrl/v1/append/receipts" -Token $token -Body $null
                Assert-StatusCode $receiptsBeforeBackup @(200) "/v1/append/receipts before backup"
                if ($receiptsBeforeBackup.Body.Count -ne 1) {
                    throw "expected one append receipt before backup, found $($receiptsBeforeBackup.Body.Count)"
                }

                $historyAfterFirstAppend = Invoke-JsonRequest -Method "GET" -Url "$baseUrl/v1/history" -Token $token -Body $null
                Assert-StatusCode $historyAfterFirstAppend @(200) "/v1/history after first append"
                if ($historyAfterFirstAppend.Body.datoms.Count -ne 1) {
                    throw "expected one datom after first append, found $($historyAfterFirstAppend.Body.datoms.Count)"
                }

                $proofRun = Invoke-JsonRequest -Method "POST" -Url "$baseUrl/v1/documents/run" -Token $token -Body @{
                    dsl = @'
schema proof_backup_v1 {
  attr attribute_1: ScalarLWW<String>
}

predicates {
  source(Entity)
  derived(Entity)
}

facts {
  source(entity(1))
}

rules {
  derived(x) <- source(x)
}

materialize {
  derived
}

query {
  current
  goal derived(x)
  keep x
}
'@
                }
                Assert-StatusCode $proofRun @(200) "proof execution before backup"
                $proofHandle = $proofRun.Body.execution.trace_handles[0].handle
                if (-not $proofHandle) {
                    throw "proof execution did not return a trace handle"
                }

                $unconfirmedSnapshotDir = Join-Path $runDir "admin\snapshot-unconfirmed"
                $unconfirmedBackup = Invoke-CapturedCommand `
                    -PackName "admin" `
                    -Label "backup-rejects-missing-quiescence-confirmation" `
                    -Command $pwshPath `
                    -Arguments @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $backupScriptPath, "-SnapshotDir", $unconfirmedSnapshotDir) `
                    -AllowFailure
                if ($unconfirmedBackup.ExitCode -eq 0) {
                    throw "backup without quiescence confirmation unexpectedly succeeded"
                }
                Assert-Contains $unconfirmedBackup.OutputText "ConfirmServiceStopped" "unconfirmed backup output"

                $hotSnapshotDir = Join-Path $runDir "admin\snapshot-hot"
                $hotBackup = Invoke-CapturedCommand `
                    -PackName "admin" `
                    -Label "backup-rejects-running-service" `
                    -Command $pwshPath `
                    -Arguments @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $backupScriptPath, "-SnapshotDir", $hotSnapshotDir, "-ConfirmServiceStopped") `
                    -AllowFailure
                if ($hotBackup.ExitCode -eq 0) {
                    throw "backup while the service was reachable unexpectedly succeeded"
                }
                Assert-Contains $hotBackup.OutputText "still reachable" "hot backup output"

                Stop-ServiceProcess $serviceHandle
                $serviceHandle = $null

                $snapshotDir = Join-Path $runDir "admin\snapshot"
                New-Item -ItemType Directory -Force -Path $snapshotDir | Out-Null
                $staleSnapshotFile = Join-Path $snapshotDir "stale-wal-placeholder"
                Set-Content -Path $staleSnapshotFile -Value "stale"
                $staleTargetBackup = Invoke-CapturedCommand `
                    -PackName "admin" `
                    -Label "backup-rejects-nonempty-snapshot-target" `
                    -Command $pwshPath `
                    -Arguments @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $backupScriptPath, "-SnapshotDir", $snapshotDir, "-ConfirmServiceStopped") `
                    -AllowFailure
                if ($staleTargetBackup.ExitCode -eq 0) {
                    throw "backup into a nonempty snapshot target unexpectedly succeeded"
                }
                Assert-Contains $staleTargetBackup.OutputText "must be empty" "stale snapshot-target output"
                Remove-Item -LiteralPath $staleSnapshotFile -Force

                $backup = Invoke-CapturedCommand `
                    -PackName "admin" `
                    -Label "backup-pilot-state" `
                    -Command $pwshPath `
                    -Arguments @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $backupScriptPath, "-SnapshotDir", $snapshotDir, "-ConfirmServiceStopped")
                $snapshotManifestPath = Join-Path $snapshotDir "manifest.json"
                Assert-PathExists $snapshotManifestPath "pilot snapshot manifest"
                $snapshotManifest = Get-Content -Path $snapshotManifestPath -Raw | ConvertFrom-Json
                if ($snapshotManifest.snapshot_mode -ne "quiesced_file_copy" -or -not $snapshotManifest.service_stopped_confirmed) {
                    throw "pilot snapshot manifest does not record the quiesced file-copy contract"
                }

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
                    -Arguments @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $restoreScriptPath, "-SnapshotDir", $snapshotDir, "-ConfirmServiceStopped")

                $serviceHandle = Start-ServiceProcess `
                    -PackName "admin" `
                    -WorkingDirectory $packageRoot `
                    -CommandPath $packageBinaryPath `
                    -Arguments @("--config", $packageConfigPath)
                Wait-ForHealth -BaseUrl $baseUrl

                $restoredToken = (Get-Content -Path $packageTokenPath -Raw).Trim()
                $candidateTokens = @($restoredToken, $newToken, $oldToken) | Where-Object { $_ } | Select-Object -Unique
                $restoredHistory = $null
                $restoredAuthorizedToken = $null
                foreach ($candidate in $candidateTokens) {
                    $attempt = Invoke-JsonRequest -Method "GET" -Url "$baseUrl/v1/history" -Token $candidate -Body $null
                    if ($attempt.StatusCode -eq 200) {
                        $restoredHistory = $attempt
                        $restoredAuthorizedToken = $candidate
                        break
                    }
                }
                if ($null -eq $restoredHistory) {
                    throw "no packaged token could read history after restore/restart"
                }
                if ($restoredHistory.Body.datoms.Count -ne 1) {
                    throw "expected one datom after restore/restart, found $($restoredHistory.Body.datoms.Count)"
                }
                $restoredSchema = Invoke-JsonRequest -Method "GET" -Url "$baseUrl/v1/schema" -Token $restoredAuthorizedToken -Body $null
                Assert-StatusCode $restoredSchema @(200) "/v1/schema after restore"
                if ($restoredSchema.Body.active.schema_ref.digest -ne $schemaBeforeBackup.Body.active.schema_ref.digest) {
                    throw "restored active schema digest does not match the snapshot"
                }
                $restoredReceipts = Invoke-JsonRequest -Method "GET" -Url "$baseUrl/v1/append/receipts" -Token $restoredAuthorizedToken -Body $null
                Assert-StatusCode $restoredReceipts @(200) "/v1/append/receipts after restore"
                if ($restoredReceipts.Body.Count -ne 1 -or $restoredReceipts.Body[0].batch_id -ne $receiptsBeforeBackup.Body[0].batch_id) {
                    throw "restored append receipts do not match the snapshot"
                }
                $restoredProof = Invoke-JsonRequest -Method "POST" -Url "$baseUrl/v1/explanations/resolve" -Token $restoredAuthorizedToken -Body @{
                    handle = $proofHandle
                    verify_replay = $true
                }
                Assert-StatusCode $restoredProof @(200) "trace handle after backup/restore"
                if (-not $restoredProof.Body.replay_verified) {
                    throw "restored trace handle did not pass replay verification"
                }

                [pscustomobject]@{
                    ArtifactPath = $build.OutputPath
                    Notes = "built package, expanded zip, rotated auth, and verified journal, schema, append-receipt, and execution-handle backup/restore via packaged restart"
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

            $missingSource = Get-Content -Path $packageConfigPath -Raw | ConvertFrom-Json
            Set-JsonProperty $missingSource.auth.tokens[0] "token" $null
            Set-JsonProperty $missingSource.auth.tokens[0] "token_env" $null
            Set-JsonProperty $missingSource.auth.tokens[0] "token_file" $null
            Set-JsonProperty $missingSource.auth.tokens[0] "token_command" $null
            $missingSourcePath = Join-Path $badConfigDir "missing-token-source.json"
            $missingSource | ConvertTo-Json -Depth 16 | Set-Content -Path $missingSourcePath

            $stalePath = Get-Content -Path $packageConfigPath -Raw | ConvertFrom-Json
            Set-JsonProperty $stalePath.auth.tokens[0] "token" $null
            Set-JsonProperty $stalePath.auth.tokens[0] "token_env" $null
            Set-JsonProperty $stalePath.auth.tokens[0] "token_file" "missing.token"
            Set-JsonProperty $stalePath.auth.tokens[0] "token_command" $null
            $stalePathConfig = Join-Path $badConfigDir "stale-token-path.json"
            $stalePath | ConvertTo-Json -Depth 16 | Set-Content -Path $stalePathConfig

            $badCommand = Get-Content -Path $packageConfigPath -Raw | ConvertFrom-Json
            Set-JsonProperty $badCommand.auth.tokens[0] "token" $null
            Set-JsonProperty $badCommand.auth.tokens[0] "token_env" $null
            Set-JsonProperty $badCommand.auth.tokens[0] "token_file" $null
            Set-JsonProperty $badCommand.auth.tokens[0] "token_command" @($pwshPath, "-NoProfile", "-Command", "Write-Error 'hardening token failure'; exit 9")
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
    if (-not (Test-WindowsHost)) {
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
