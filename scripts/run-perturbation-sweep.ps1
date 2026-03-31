param(
    [string]$HostManifestPath,
    [string[]]$HardeningPacks = @("admin", "operator", "user", "exec"),
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

$timestampDisplay = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$outputTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$hostManifest = Get-Content -Path $HostManifestPath | ConvertFrom-Json
$hostId = $hostManifest.host_id
$reportRoot = Join-Path $repoRoot "artifacts\performance\perturbation"
$runDir = Join-Path $reportRoot ("runs\" + $outputTimestamp)
$summaryPath = Join-Path $reportRoot ("perturbation-" + $outputTimestamp + ".md")
$jsonPath = Join-Path $reportRoot ("perturbation-" + $outputTimestamp + ".json")
$latestSummaryPath = Join-Path $reportRoot "latest.md"
$latestJsonPath = Join-Path $reportRoot "latest.json"
$steps = [System.Collections.Generic.List[object]]::new()
$stressResults = [System.Collections.Generic.List[object]]::new()
$driftResults = [System.Collections.Generic.List[object]]::new()
$observedBenchmarks = [System.Collections.Generic.List[object]]::new()
$conservativeProjections = [System.Collections.Generic.List[object]]::new()
$footprintModels = [System.Collections.Generic.List[object]]::new()
$takeaways = [System.Collections.Generic.List[string]]::new()
$hardeningJsonPath = Join-Path $repoRoot "artifacts\qa\hardening\latest.json"
$hardeningSummaryPath = Join-Path $repoRoot "artifacts\qa\hardening\latest.md"
$hardeningStatusCounts = @()
$hardeningWasReused = $false

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

function New-ArtifactPath([string]$Name, [string]$Extension = "txt") {
    $safeName = $Name -replace '[^A-Za-z0-9\-_]+', '-'
    Join-Path $runDir ("{0}.{1}" -f $safeName, $Extension)
}

function Invoke-CapturedCommand {
    param(
        [string]$Label,
        [string]$Command,
        [string[]]$Arguments,
        [string]$WorkingDirectory = $repoRoot,
        [switch]$AllowFailure
    )

    $outputPath = New-ArtifactPath $Label
    $commandText = Format-CommandText $Command $Arguments $WorkingDirectory

    Write-Host ""
    Write-Host $Label -ForegroundColor Cyan
    Write-Host "Running: $commandText"

    $stdoutPath = New-ArtifactPath ($Label + "-stdout")
    $stderrPath = New-ArtifactPath ($Label + "-stderr")
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
    $stdoutText = if (Test-Path $stdoutPath) { Get-Content -Path $stdoutPath -Raw } else { "" }
    $stderrText = if (Test-Path $stderrPath) { Get-Content -Path $stderrPath -Raw } else { "" }
    $combinedParts = [System.Collections.Generic.List[string]]::new()
    if ($stdoutText) {
        $combinedParts.Add($stdoutText.TrimEnd())
    }
    if ($stderrText) {
        $combinedParts.Add($stderrText.TrimEnd())
    }
    $outputText = $combinedParts -join "`r`n"
    Set-Content -Path $outputPath -Value $outputText

    if ($stdoutText) {
        Write-Host $stdoutText.TrimEnd()
    }
    if ($stderrText) {
        Write-Host $stderrText.TrimEnd()
    }

    $steps.Add([pscustomobject]@{
            label = $Label
            command = $commandText
            exit_code = $exitCode
            artifact_path = $outputPath
        })

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

function Convert-DurationToSeconds($Duration) {
    [double]$Duration.secs + ([double]$Duration.nanos / 1000000000.0)
}

function Parse-Decimal([string]$Value) {
    [double]::Parse($Value, [System.Globalization.CultureInfo]::InvariantCulture)
}

function Format-Rate([double]$Value) {
    if ($Value -ge 1000000) {
        return "{0:N2}M" -f ($Value / 1000000)
    }
    if ($Value -ge 1000) {
        return "{0:N2}K" -f ($Value / 1000)
    }
    "{0:N2}" -f $Value
}

function Format-Count([double]$Value) {
    "{0:N0}" -f $Value
}

function Format-Bytes([double]$Bytes) {
    if ($Bytes -ge 1GB) {
        return "{0:N2} GiB" -f ($Bytes / 1GB)
    }
    if ($Bytes -ge 1MB) {
        return "{0:N2} MiB" -f ($Bytes / 1MB)
    }
    if ($Bytes -ge 1KB) {
        return "{0:N2} KiB" -f ($Bytes / 1KB)
    }
    return "{0:N0} B" -f $Bytes
}

function Format-Seconds([double]$Seconds) {
    if ($Seconds -ge 1.0) {
        return "{0:N2}s" -f $Seconds
    }
    if ($Seconds -ge 0.001) {
        return "{0:N2}ms" -f ($Seconds * 1000.0)
    }
    return "{0:N2}us" -f ($Seconds * 1000000.0)
}

function Find-Measurement($Bundle, [string]$Workload, [string]$Scale) {
    foreach ($measurement in $Bundle.report.measurements) {
        if ($measurement.workload -eq $Workload -and $measurement.scale -eq $Scale) {
            return $measurement
        }
    }
    return $null
}

function Find-Footprint($Bundle, [string]$Workload, [string]$Scale) {
    foreach ($footprint in $Bundle.report.footprints) {
        if ($footprint.workload -eq $Workload -and $footprint.scale -eq $Scale) {
            return $footprint
        }
    }
    return $null
}

function Get-MetricValue($Metrics, [string]$Name) {
    foreach ($metric in $Metrics) {
        if ($metric.name -eq $Name) {
            return [double]$metric.value
        }
    }
    return $null
}

function Expected-ChainTupleCount([int]$ChainLen) {
    [double]$ChainLen * ([double]$ChainLen - 1.0) / 2.0
}

function New-Projection([string]$Family, [string]$Label, [double]$PerSecond, [string]$UnitLabel, [string]$Source, [string]$Scale) {
    [pscustomobject]@{
        family = $Family
        label = $Label
        scale = $Scale
        source = $Source
        throughput_per_second = $PerSecond
        unit_label = $UnitLabel
        per_minute = $PerSecond * 60.0
        per_hour = $PerSecond * 3600.0
        per_day = $PerSecond * 86400.0
    }
}

function Add-FormattedLine([System.Collections.Generic.List[string]]$Lines, [string]$Template, [object[]]$Values) {
    $Lines.Add([string]::Format([System.Globalization.CultureInfo]::InvariantCulture, $Template, $Values))
}

function Find-BaselinePath([string]$Suite) {
    $candidatePaths = @(
        (Join-Path $repoRoot ("artifacts\performance\baselines\{0}\{1}.json" -f $Suite, $hostId)),
        (Join-Path $repoRoot ("fixtures\performance\baselines\{0}\{1}.json" -f $Suite, $hostId))
    )
    foreach ($candidate in $candidatePaths) {
        if (Test-Path $candidate) {
            return $candidate
        }
    }
    return $null
}

function Add-StressMatches {
    param(
        [string]$Pattern,
        [scriptblock]$Mapper,
        [string]$OutputText
    )

    foreach ($match in [regex]::Matches($OutputText, $Pattern, [System.Text.RegularExpressions.RegexOptions]::Multiline)) {
        $stressResults.Add((& $Mapper $match))
    }
}

New-Item -ItemType Directory -Force -Path $runDir | Out-Null
New-Item -ItemType Directory -Force -Path $reportRoot | Out-Null

$cargo = Get-CommandPath "cargo"
$shell = Get-Command pwsh -ErrorAction SilentlyContinue
if ($shell) {
    $powerShell = $shell.Source
} else {
    $powerShell = Get-CommandPath "powershell"
}

Write-Host ""
Write-Host "AETHER Perturbation And Capacity Sweep"
Write-Host "====================================="
Write-Host "Started: $timestampDisplay"
Write-Host "Host:    $hostId"
Write-Host "Run dir: $runDir"
Write-Host ""

$hardeningSummary = $null
if (-not $SkipHardening) {
    $hardeningArgs = @(
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        (Join-Path $repoRoot "scripts\run-hardening-sweep.ps1")
    )
    $normalizedRequestedPacks = $HardeningPacks | ForEach-Object { $_.Trim().ToLowerInvariant() } | Where-Object { $_ }
    $defaultPacks = @("admin", "operator", "user", "exec")
    $useDefaultPackSet = $normalizedRequestedPacks.Count -eq $defaultPacks.Count
    if ($useDefaultPackSet) {
        for ($index = 0; $index -lt $defaultPacks.Count; $index++) {
            if ($normalizedRequestedPacks[$index] -ne $defaultPacks[$index]) {
                $useDefaultPackSet = $false
                break
            }
        }
    }
    if (-not $useDefaultPackSet) {
        $hardeningArgs += @("-Packs", ($normalizedRequestedPacks -join ","))
    }
    Invoke-CapturedCommand -Label "hardening-sweep" -Command $powerShell -Arguments $hardeningArgs | Out-Null
    if (Test-Path $hardeningJsonPath) {
        $hardeningSummary = Get-Content -Path $hardeningJsonPath -Raw | ConvertFrom-Json
        $hardeningStatusCounts = $hardeningSummary.results | Group-Object status | ForEach-Object {
            [pscustomobject]@{
                status = $_.Name
                count = $_.Count
            }
        }
    }
} elseif (Test-Path $hardeningJsonPath) {
    $hardeningSummary = Get-Content -Path $hardeningJsonPath -Raw | ConvertFrom-Json
    $hardeningStatusCounts = $hardeningSummary.results | Group-Object status | ForEach-Object {
        [pscustomobject]@{
            status = $_.Name
            count = $_.Count
        }
    }
    $hardeningWasReused = $true
}

$performanceBundlePath = Join-Path $runDir "performance-bundle.json"
$performanceReportPath = Join-Path $runDir "performance-report.md"
$capacityInputJsonPath = Join-Path $runDir "capacity-curves.json"
$capacityInputReportPath = Join-Path $runDir "capacity-curves.md"
$hostManifestResolvedPath = (Resolve-Path $HostManifestPath).Path
$performanceArgs = @(
    "run", "-p", "aether_api", "--example", "performance_report", "--release", "--",
    "--suite", "full_stack",
    "--host-manifest", $hostManifestResolvedPath,
    "--bundle-path", $performanceBundlePath,
    "--report-path", $performanceReportPath
)
Invoke-CapturedCommand -Label "performance-report" -Command $cargo -Arguments $performanceArgs | Out-Null
Copy-Item -Force $performanceBundlePath (Join-Path $repoRoot "artifacts\performance\latest.json")
Copy-Item -Force $performanceReportPath (Join-Path $repoRoot "artifacts\performance\latest.md")
$bundle = Get-Content -Path $performanceBundlePath -Raw | ConvertFrom-Json

$capacityArgs = @(
    "run", "-p", "aether_api", "--example", "performance_capacity_curves", "--release", "--",
    "--host-manifest", $hostManifestResolvedPath,
    "--samples", "1",
    "--output-json", $capacityInputJsonPath,
    "--output-report", $capacityInputReportPath
)
Invoke-CapturedCommand -Label "performance-capacity-curves" -Command $cargo -Arguments $capacityArgs | Out-Null

foreach ($suite in @("core_kernel", "service_in_process")) {
    $baselinePath = Find-BaselinePath $suite
    if (-not $baselinePath) {
        $driftResults.Add([pscustomobject]@{
                suite = $suite
                status = "missing_baseline"
                baseline_path = $null
                report_path = $null
                bundle_path = $null
            })
        continue
    }

    $driftBundlePath = Join-Path $runDir ("drift-" + $suite + "-bundle.json")
    $driftReportPath = Join-Path $runDir ("drift-" + $suite + ".md")
    $driftArgs = @(
        "run", "-p", "aether_api", "--example", "performance_drift_report", "--release", "--",
        "--suite", $suite,
        "--host-manifest", $hostManifestResolvedPath,
        "--baseline", (Resolve-Path $baselinePath).Path,
        "--bundle-path", $driftBundlePath,
        "--report-path", $driftReportPath
    )
    $driftRun = Invoke-CapturedCommand -Label ("performance-drift-" + $suite) -Command $cargo -Arguments $driftArgs -AllowFailure
    if (Test-Path $driftReportPath) {
        Copy-Item -Force $driftReportPath (Join-Path $repoRoot ("artifacts\performance\latest-drift-{0}.md" -f $suite))
        $driftReportText = Get-Content -Path $driftReportPath -Raw
    } else {
        $driftReportText = $driftRun.OutputText
    }
    $overallMatch = [regex]::Match($driftReportText, 'Gated overall:\s+`(?<status>[^`]+)`')
    $driftStatus = if ($overallMatch.Success) {
        $overallMatch.Groups["status"].Value
    } elseif ($driftRun.ExitCode -ne 0) {
        "failed"
    } else {
        "unknown"
    }
    $driftResults.Add([pscustomobject]@{
            suite = $suite
            status = $driftStatus
            baseline_path = (Resolve-Path $baselinePath).Path
            report_path = if (Test-Path $driftReportPath) { $driftReportPath } else { $null }
            bundle_path = if (Test-Path $driftBundlePath) { $driftBundlePath } else { $null }
        })
}

$stressArgs = @(
    "test", "-p", "aether_api", "--test", "performance_stress", "--release", "--",
    "--ignored", "--nocapture", "--test-threads=1"
)
$stressRun = Invoke-CapturedCommand -Label "performance-stress" -Command $cargo -Arguments $stressArgs

Add-StressMatches `
    -Pattern "runtime stress: chain=(?<scale>\d+) tuples=(?<units>\d+) elapsed=(?<elapsed>[\d\.Ee\+\-]+) estimated_bytes=(?<bytes>\d+)" `
    -OutputText $stressRun.OutputText `
    -Mapper {
        param($Match)
        $elapsed = Parse-Decimal $Match.Groups["elapsed"].Value
        $units = [double]$Match.Groups["units"].Value
        $throughput = if ($elapsed -gt 0.0) { $units / $elapsed } else { 0.0 }
        [pscustomobject]@{
            family = "runtime"
            label = "Recursive closure runtime"
            scale_label = "chain " + $Match.Groups["scale"].Value
            scale_value = [int]$Match.Groups["scale"].Value
            units = $units
            unit_label = "tuples/s"
            elapsed_seconds = $elapsed
            throughput_per_second = $throughput
            estimated_bytes = [double]$Match.Groups["bytes"].Value
        }
    }

Add-StressMatches `
    -Pattern "explain stress: chain=(?<scale>\d+) trace_tuples=(?<units>\d+) elapsed=(?<elapsed>[\d\.Ee\+\-]+) estimated_bytes=(?<bytes>\d+)" `
    -OutputText $stressRun.OutputText `
    -Mapper {
        param($Match)
        $elapsed = Parse-Decimal $Match.Groups["elapsed"].Value
        $units = [double]$Match.Groups["units"].Value
        $throughput = if ($elapsed -gt 0.0) { $units / $elapsed } else { 0.0 }
        [pscustomobject]@{
            family = "explain"
            label = "Tuple explanation runtime"
            scale_label = "chain " + $Match.Groups["scale"].Value
            scale_value = [int]$Match.Groups["scale"].Value
            units = $units
            unit_label = "trace-tuples/s"
            elapsed_seconds = $elapsed
            throughput_per_second = $throughput
            estimated_bytes = [double]$Match.Groups["bytes"].Value
        }
    }

Add-StressMatches `
    -Pattern "service stress: tasks=(?<scale>\d+) rows=(?<units>\d+) elapsed=(?<elapsed>[\d\.Ee\+\-]+)" `
    -OutputText $stressRun.OutputText `
    -Mapper {
        param($Match)
        $elapsed = Parse-Decimal $Match.Groups["elapsed"].Value
        $units = [double]$Match.Groups["units"].Value
        $throughput = if ($elapsed -gt 0.0) { $units / $elapsed } else { 0.0 }
        [pscustomobject]@{
            family = "service"
            label = "Kernel service coordination run"
            scale_label = "tasks " + $Match.Groups["scale"].Value
            scale_value = [int]$Match.Groups["scale"].Value
            units = $units
            unit_label = "rows/s"
            elapsed_seconds = $elapsed
            throughput_per_second = $throughput
            estimated_bytes = $null
        }
    }

Add-StressMatches `
    -Pattern "durable resolve stress: entities=(?<scale>\d+) datoms=(?<datoms>\d+) elapsed=(?<elapsed>[\d\.Ee\+\-]+)" `
    -OutputText $stressRun.OutputText `
    -Mapper {
        param($Match)
        $elapsed = Parse-Decimal $Match.Groups["elapsed"].Value
        $units = [double]$Match.Groups["scale"].Value
        $throughput = if ($elapsed -gt 0.0) { $units / $elapsed } else { 0.0 }
        [pscustomobject]@{
            family = "durable_resolve"
            label = "Durable restart current replay"
            scale_label = "entities " + $Match.Groups["scale"].Value
            scale_value = [int]$Match.Groups["scale"].Value
            units = $units
            unit_label = "entities/s"
            elapsed_seconds = $elapsed
            throughput_per_second = $throughput
            estimated_bytes = $null
            datoms = [int]$Match.Groups["datoms"].Value
        }
    }

Add-StressMatches `
    -Pattern "durable coordination stress: tasks=(?<scale>\d+) rows=(?<units>\d+) elapsed=(?<elapsed>[\d\.Ee\+\-]+)" `
    -OutputText $stressRun.OutputText `
    -Mapper {
        param($Match)
        $elapsed = Parse-Decimal $Match.Groups["elapsed"].Value
        $units = [double]$Match.Groups["units"].Value
        $throughput = if ($elapsed -gt 0.0) { $units / $elapsed } else { 0.0 }
        [pscustomobject]@{
            family = "durable_coordination"
            label = "Durable restart coordination replay"
            scale_label = "tasks " + $Match.Groups["scale"].Value
            scale_value = [int]$Match.Groups["scale"].Value
            units = $units
            unit_label = "rows/s"
            elapsed_seconds = $elapsed
            throughput_per_second = $throughput
            estimated_bytes = $null
        }
    }

$benchmarkDescriptors = @(
    @{ family = "append"; label = "Journal append throughput"; workload = "Journal append throughput"; scale = "50,000 datoms"; unit = "datoms/s" },
    @{ family = "resolver_current"; label = "Resolver current throughput"; workload = "Resolver current throughput"; scale = "1,000 entities"; unit = "entities/s" },
    @{ family = "resolver_as_of"; label = "Resolver as-of throughput"; workload = "Resolver as-of throughput"; scale = "1,000 entities"; unit = "entities/s" },
    @{ family = "runtime"; label = "Recursive closure runtime"; workload = "Recursive closure runtime"; scale = "chain 128"; unit = "tuples/s" },
    @{ family = "explain"; label = "Tuple explanation runtime"; workload = "Tuple explanation runtime"; scale = "chain 128"; unit = "trace-tuples/s" },
    @{ family = "service"; label = "Kernel service coordination run"; workload = "Kernel service coordination run"; scale = "128 tasks"; unit = "rows/s" },
    @{ family = "durable_resolve"; label = "Durable restart current replay"; workload = "Durable restart current replay"; scale = "1,000 entities"; unit = "entities/s" },
    @{ family = "durable_coordination"; label = "Durable restart coordination replay"; workload = "Durable restart coordination replay"; scale = "128 tasks"; unit = "rows/s" }
)

foreach ($descriptor in $benchmarkDescriptors) {
    $measurement = Find-Measurement $bundle $descriptor.workload $descriptor.scale
    if ($null -eq $measurement) {
        continue
    }
    $meanSeconds = Convert-DurationToSeconds $measurement.latency.mean
    $observedBenchmarks.Add([pscustomobject]@{
            family = $descriptor.family
            label = $descriptor.label
            scale = $descriptor.scale
            mean_seconds = $meanSeconds
            throughput_per_second = [double]$measurement.throughput_per_second
            unit_label = $measurement.unit_label
        })
}

foreach ($family in @("runtime", "explain", "service", "durable_resolve", "durable_coordination")) {
    $largest = $stressResults |
        Where-Object family -eq $family |
        Sort-Object scale_value -Descending |
        Select-Object -First 1
    if ($null -ne $largest) {
        $conservativeProjections.Add((New-Projection `
                    -Family $family `
                    -Label $largest.label `
                    -PerSecond $largest.throughput_per_second `
                    -UnitLabel $largest.unit_label `
                    -Source ("largest passing stress: " + $largest.scale_label) `
                    -Scale $largest.scale_label))
    }
}

foreach ($benchmark in $observedBenchmarks) {
    if ($benchmark.family -in @("append", "resolver_current", "resolver_as_of")) {
        $conservativeProjections.Add((New-Projection `
                    -Family $benchmark.family `
                    -Label $benchmark.label `
                    -PerSecond $benchmark.throughput_per_second `
                    -UnitLabel $benchmark.unit_label `
                    -Source "full_stack benchmark snapshot" `
                    -Scale $benchmark.scale))
    }
}

$largestRuntimeStress = $stressResults | Where-Object family -eq "runtime" | Sort-Object scale_value -Descending | Select-Object -First 1
if ($null -ne $largestRuntimeStress -and $largestRuntimeStress.units -gt 0) {
    $bytesPerTuple = $largestRuntimeStress.estimated_bytes / $largestRuntimeStress.units
    foreach ($chainLen in @(2048, 4096)) {
        $projectedTuples = Expected-ChainTupleCount $chainLen
        $footprintModels.Add([pscustomobject]@{
                family = "runtime"
                model = "derived-set footprint"
                source = $largestRuntimeStress.scale_label
                projected_scale = "chain $chainLen"
                projected_units = [math]::Round($projectedTuples)
                projected_unit_label = "tuples"
                estimated_bytes = [math]::Round($projectedTuples * $bytesPerTuple)
            })
    }
}

$largestExplainStress = $stressResults | Where-Object family -eq "explain" | Sort-Object scale_value -Descending | Select-Object -First 1
if ($null -ne $largestExplainStress -and $largestExplainStress.units -gt 0) {
    $bytesPerTraceTuple = $largestExplainStress.estimated_bytes / $largestExplainStress.units
    foreach ($chainLen in @(1024, 2048)) {
        $traceTupleCount = [double]($chainLen - 1)
        $footprintModels.Add([pscustomobject]@{
                family = "explain"
                model = "proof-trace footprint"
                source = $largestExplainStress.scale_label
                projected_scale = "chain $chainLen"
                projected_units = [math]::Round($traceTupleCount)
                projected_unit_label = "trace tuples"
                estimated_bytes = [math]::Round($traceTupleCount * $bytesPerTraceTuple)
            })
    }
}

$runtimeStress512 = $stressResults | Where-Object { $_.family -eq "runtime" -and $_.scale_value -eq 512 } | Select-Object -First 1
$runtimeStress1024 = $stressResults | Where-Object { $_.family -eq "runtime" -and $_.scale_value -eq 1024 } | Select-Object -First 1
if ($runtimeStress512 -and $runtimeStress1024 -and $runtimeStress512.throughput_per_second -gt 0.0) {
    $runtimeRatio = $runtimeStress1024.throughput_per_second / $runtimeStress512.throughput_per_second
    $takeaways.Add(
        "Recursive closure stays correct through chain 1,024, but throughput falls to {0:P0} of the chain-512 run as tuple volume grows quadratically." -f $runtimeRatio
    )
}

$serviceStress1024 = $stressResults | Where-Object { $_.family -eq "service" -and $_.scale_value -eq 1024 } | Select-Object -First 1
$serviceStress4096 = $stressResults | Where-Object { $_.family -eq "service" -and $_.scale_value -eq 4096 } | Select-Object -First 1
if ($serviceStress1024 -and $serviceStress4096 -and $serviceStress1024.throughput_per_second -gt 0.0) {
    $serviceRatio = $serviceStress4096.throughput_per_second / $serviceStress1024.throughput_per_second
    $takeaways.Add(
        "Claimability service throughput at 4,096 tasks is {0:P0} of the 1,024-task run, which is the clearest current signal for coordination-surface scaling efficiency under larger boards." -f $serviceRatio
    )
}

if ($hardeningSummary) {
    $failedCount = ($hardeningSummary.results | Where-Object status -ne "passed").Count
    $passedCount = ($hardeningSummary.results | Where-Object status -eq "passed").Count
    $takeaways.Add("The persona usability sweep completed with $passedCount passed checks and $failedCount non-passing checks before the performance layer was exercised.")
}

if ($largestRuntimeStress -and $largestRuntimeStress.estimated_bytes) {
    $runtimeFootprintTakeaway = "The largest observed recursive-closure stress run reached {0} derived tuples at {1} estimated footprint." -f `
        (Format-Count $largestRuntimeStress.units), `
        (Format-Bytes $largestRuntimeStress.estimated_bytes)
    $takeaways.Add($runtimeFootprintTakeaway)
}

if ($largestExplainStress -and $largestExplainStress.estimated_bytes) {
    $explainFootprintTakeaway = "Proof traces remain compact relative to closure state: the largest observed explanation stress run reconstructed {0} trace tuples in {1}." -f `
        (Format-Count $largestExplainStress.units), `
        (Format-Bytes $largestExplainStress.estimated_bytes)
    $takeaways.Add($explainFootprintTakeaway)
}

$summaryLines = [System.Collections.Generic.List[string]]::new()
$summaryLines.Add("# AETHER Perturbation And Capacity Sweep")
$summaryLines.Add("")
$summaryLines.Add("- Generated at: ``$timestampDisplay``")
$summaryLines.Add("- Host: ``$hostId``")
$summaryLines.Add("- Host manifest: ``$HostManifestPath``")
$summaryLines.Add("- Run directory: ``$runDir``")
$summaryLines.Add("")
$summaryLines.Add("This sweep combines the persona usability pass, the full-stack benchmark snapshot, release-mode stress workloads, and straight-line single-node capacity projections. Projections are planning estimates from the current host and should not be treated as production SLAs.")
$summaryLines.Add("")

$summaryLines.Add("## Persona Sweep")
$summaryLines.Add("")
if ($hardeningSummary) {
    if ($hardeningWasReused) {
        $summaryLines.Add("- Hardening summary reused from the latest completed sweep: ``$hardeningSummaryPath``")
    } else {
        $summaryLines.Add("- Hardening summary: ``$hardeningSummaryPath``")
    }
    $summaryLines.Add("- Hardening JSON: ``$hardeningJsonPath``")
    foreach ($statusCount in $hardeningStatusCounts) {
        Add-FormattedLine $summaryLines "- {0}: {1}" @($statusCount.status, $statusCount.count)
    }
} else {
    $summaryLines.Add("- Hardening step skipped for this run.")
}
$summaryLines.Add("")

$summaryLines.Add("## Benchmark Snapshot")
$summaryLines.Add("")
$summaryLines.Add("| Workload | Scale | Mean latency | Throughput |")
$summaryLines.Add("| --- | --- | ---: | ---: |")
foreach ($benchmark in $observedBenchmarks) {
    Add-FormattedLine $summaryLines "| {0} | {1} | {2} | {3}/{4} |" @(
        $benchmark.label,
        $benchmark.scale,
        (Format-Seconds $benchmark.mean_seconds),
        (Format-Rate $benchmark.throughput_per_second),
        $benchmark.unit_label
    )
}
$summaryLines.Add("")

$summaryLines.Add("## Capacity Curves")
$summaryLines.Add("")
$summaryLines.Add("- Capacity curve JSON: ``$capacityInputJsonPath``")
$summaryLines.Add("- Capacity curve report: ``$capacityInputReportPath``")
$summaryLines.Add("")

$summaryLines.Add("## Stress Results")
$summaryLines.Add("")
$summaryLines.Add("| Workload | Scale | Elapsed | Throughput | Footprint |")
$summaryLines.Add("| --- | --- | ---: | ---: | ---: |")
foreach ($result in ($stressResults | Sort-Object family, scale_value)) {
    $footprintText = if ($null -ne $result.estimated_bytes) { Format-Bytes $result.estimated_bytes } else { "-" }
    Add-FormattedLine $summaryLines "| {0} | {1} | {2} | {3}/{4} | {5} |" @(
        $result.label,
        $result.scale_label,
        (Format-Seconds $result.elapsed_seconds),
        (Format-Rate $result.throughput_per_second),
        $result.unit_label,
        $footprintText
    )
}
$summaryLines.Add("")

$summaryLines.Add("## Drift Checks")
$summaryLines.Add("")
$summaryLines.Add("| Suite | Status | Report |")
$summaryLines.Add("| --- | --- | --- |")
foreach ($drift in $driftResults) {
    $reportText = if ($drift.report_path) { "``$($drift.report_path)``" } else { "-" }
    Add-FormattedLine $summaryLines "| {0} | {1} | {2} |" @(
        $drift.suite,
        $drift.status,
        $reportText
    )
}
$summaryLines.Add("")

$summaryLines.Add("## Capacity Projections")
$summaryLines.Add("")
$summaryLines.Add("| Source | Workload | Scale | Per minute | Per hour | Per day |")
$summaryLines.Add("| --- | --- | --- | ---: | ---: | ---: |")
foreach ($projection in $conservativeProjections) {
    Add-FormattedLine $summaryLines "| {0} | {1} | {2} | {3}/{4} | {5}/{4} | {6}/{4} |" @(
        $projection.source,
        $projection.label,
        $projection.scale,
        (Format-Rate $projection.per_minute),
        $projection.unit_label,
        (Format-Rate $projection.per_hour),
        (Format-Rate $projection.per_day)
    )
}
$summaryLines.Add("")

$summaryLines.Add("## Footprint Projections")
$summaryLines.Add("")
$summaryLines.Add("| Model | Source | Projected scale | Units | Estimated footprint |")
$summaryLines.Add("| --- | --- | --- | ---: | ---: |")
foreach ($model in $footprintModels) {
    Add-FormattedLine $summaryLines "| {0} | {1} | {2} | {3} {4} | {5} |" @(
        $model.model,
        $model.source,
        $model.projected_scale,
        (Format-Count $model.projected_units),
        $model.projected_unit_label,
        (Format-Bytes $model.estimated_bytes)
    )
}
$summaryLines.Add("")

$summaryLines.Add("## Takeaways")
$summaryLines.Add("")
foreach ($takeaway in $takeaways) {
    $summaryLines.Add("- $takeaway")
}
if ($takeaways.Count -eq 0) {
    $summaryLines.Add("- No exceptional takeaways were generated; review the benchmark, stress, and drift tables directly.")
}
$summaryLines.Add("")
$summaryLines.Add("## Artifact Paths")
$summaryLines.Add("")
$summaryLines.Add("- Benchmark bundle: ``$performanceBundlePath``")
$summaryLines.Add("- Benchmark report: ``$performanceReportPath``")
$summaryLines.Add("- Capacity curve JSON: ``$capacityInputJsonPath``")
$summaryLines.Add("- Capacity curve report: ``$capacityInputReportPath``")
$summaryLines.Add("- Stress transcript: ``$($stressRun.OutputPath)``")
$summaryLines.Add("- JSON summary: ``$jsonPath``")

$summaryText = $summaryLines -join "`r`n"

$summaryObject = [pscustomobject]@{
    generated_at = $timestampDisplay
    host_id = $hostId
    host_manifest_path = (Resolve-Path $HostManifestPath).Path
    run_directory = $runDir
    hardening = [pscustomobject]@{
        included = (-not $SkipHardening)
        reused_latest = $hardeningWasReused
        summary_path = $hardeningSummaryPath
        json_path = $hardeningJsonPath
        status_counts = $hardeningStatusCounts
    }
    performance = [pscustomobject]@{
        bundle_path = $performanceBundlePath
        report_path = $performanceReportPath
        drift = $driftResults
    }
    capacity_inputs = [pscustomobject]@{
        json_path = $capacityInputJsonPath
        report_path = $capacityInputReportPath
    }
    stress = [pscustomobject]@{
        transcript_path = $stressRun.OutputPath
        results = $stressResults
    }
    benchmark_snapshot = $observedBenchmarks
    capacity_projections = $conservativeProjections
    footprint_models = $footprintModels
    takeaways = $takeaways
    steps = $steps
}

Set-Content -Path $summaryPath -Value $summaryText
$summaryObject | ConvertTo-Json -Depth 16 | Set-Content -Path $jsonPath
Copy-Item -Force $summaryPath $latestSummaryPath
Copy-Item -Force $jsonPath $latestJsonPath

Write-Host ""
Write-Host "Perturbation sweep completed successfully." -ForegroundColor Green
Write-Host "Summary: $summaryPath"
Write-Host "JSON:    $jsonPath"

Close-Runner 0
