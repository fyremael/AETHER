param(
    [string]$OutputDir,
    [string]$BindAddr = "127.0.0.1:3000",
    [switch]$SkipBuild,
    [switch]$RotateToken
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$defaultPackageRoot = Join-Path $repoRoot "artifacts\pilot\packages\aether-pilot-service-windows-x86_64"
$packageRoot = if ($OutputDir) {
    $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutputDir)
} else {
    $defaultPackageRoot
}
$zipPath = "$packageRoot.zip"
$binarySource = Join-Path $repoRoot "target\release\aether_pilot_service.exe"
$shellBinarySource = Join-Path $repoRoot "target\release\aetherctl.exe"
$templatePath = Join-Path $repoRoot "fixtures\deployment\pilot-service.template.json"
$deploymentDocSource = Join-Path $repoRoot "docs\PILOT_DEPLOYMENT.md"
$playbookDocSource = Join-Path $repoRoot "docs\PILOT_OPERATIONS_PLAYBOOK.md"
$binDir = Join-Path $packageRoot "bin"
$configDir = Join-Path $packageRoot "config"
$docsDir = Join-Path $packageRoot "docs"
$dataDir = Join-Path $packageRoot "data"
$logsDir = Join-Path $packageRoot "logs"
$tokenPath = Join-Path $configDir "pilot-operator.token"
$configPath = Join-Path $configDir "pilot-service.json"
$runPs1Path = Join-Path $packageRoot "run-pilot-service.ps1"
$runCmdPath = Join-Path $packageRoot "run-pilot-service.cmd"
$opsPs1Path = Join-Path $packageRoot "run-aether-ops.ps1"
$opsCmdPath = Join-Path $packageRoot "run-aether-ops.cmd"
$rotatePs1Path = Join-Path $packageRoot "rotate-pilot-token.ps1"
$rotateCmdPath = Join-Path $packageRoot "rotate-pilot-token.cmd"
$backupPs1Path = Join-Path $packageRoot "backup-pilot-state.ps1"
$backupCmdPath = Join-Path $packageRoot "backup-pilot-state.cmd"
$restorePs1Path = Join-Path $packageRoot "restore-pilot-state.ps1"
$restoreCmdPath = Join-Path $packageRoot "restore-pilot-state.cmd"

function New-SecureToken {
    $bytes = New-Object byte[] 48
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}

if (-not $SkipBuild) {
    Write-Host "Building release binary..." -ForegroundColor Cyan
    Push-Location $repoRoot
    try {
        cargo build -p aether_api --bin aether_pilot_service --release
    } finally {
        Pop-Location
    }

    Write-Host "Building Go operator shell..." -ForegroundColor Cyan
    Push-Location (Join-Path $repoRoot "go")
    try {
        go build -o $shellBinarySource ./cmd/aetherctl
    } finally {
        Pop-Location
    }
}

if (-not (Test-Path $binarySource)) {
    throw "Pilot service binary not found at $binarySource"
}
if (-not (Test-Path $shellBinarySource)) {
    throw "Go operator shell binary not found at $shellBinarySource"
}

if (Test-Path $packageRoot) {
    Remove-Item -Recurse -Force $packageRoot
}
New-Item -ItemType Directory -Force -Path $binDir, $configDir, $docsDir, $dataDir, $logsDir | Out-Null

Copy-Item -Path $binarySource -Destination (Join-Path $binDir "aether_pilot_service.exe")
Copy-Item -Path $shellBinarySource -Destination (Join-Path $binDir "aetherctl.exe")
Copy-Item -Path $deploymentDocSource -Destination (Join-Path $docsDir "PILOT_DEPLOYMENT.md")
Copy-Item -Path $playbookDocSource -Destination (Join-Path $docsDir "PILOT_OPERATIONS_PLAYBOOK.md")

$template = Get-Content -Path $templatePath -Raw | ConvertFrom-Json
$template.bind_addr = $BindAddr
$template.database_path = "../data/coordination.sqlite"
$template.audit_log_path = "../logs/audit.jsonl"
$template.auth.tokens[0].token_file = "pilot-operator.token"
$template | ConvertTo-Json -Depth 8 | Set-Content -Path $configPath

if ($RotateToken -or -not (Test-Path $tokenPath)) {
    $token = New-SecureToken
    Set-Content -Path $tokenPath -Value $token -NoNewline
}

@'
param(
    [string]$ConfigPath = (Join-Path $PSScriptRoot "config\pilot-service.json")
)

$ErrorActionPreference = "Stop"
$binary = Join-Path $PSScriptRoot "bin\aether_pilot_service.exe"
if (-not (Test-Path $binary)) {
    throw "Pilot service binary not found at $binary"
}
& $binary --config $ConfigPath
'@ | Set-Content -Path $runPs1Path

@'
@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-pilot-service.ps1" %*
'@ | Set-Content -Path $runCmdPath

@'
param(
    [string]$BaseUrl = "http://127.0.0.1:3000",
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$TuiArgs
)

$ErrorActionPreference = "Stop"
$binary = Join-Path $PSScriptRoot "bin\aetherctl.exe"
$token = Join-Path $PSScriptRoot "config\pilot-operator.token"

if (-not (Test-Path $binary)) {
    throw "Operator shell binary not found at $binary"
}
if (-not (Test-Path $token)) {
    throw "Pilot operator token file not found at $token"
}

& $binary --base-url $BaseUrl --token-file $token tui @TuiArgs
'@ | Set-Content -Path $opsPs1Path

@'
@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-aether-ops.ps1" %*
'@ | Set-Content -Path $opsCmdPath

@'
param(
    [string]$TokenPath = (Join-Path $PSScriptRoot "config\pilot-operator.token"),
    [switch]$BackupExisting = $true
)

$ErrorActionPreference = "Stop"

function New-SecureToken {
    $bytes = New-Object byte[] 48
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}

$resolvedTokenPath = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($TokenPath)
$parent = Split-Path -Parent $resolvedTokenPath
if ($parent) {
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
}

if ($BackupExisting -and (Test-Path $resolvedTokenPath)) {
    $backupPath = "$resolvedTokenPath." + (Get-Date -Format "yyyyMMdd-HHmmss") + ".bak"
    Copy-Item -Path $resolvedTokenPath -Destination $backupPath
    Write-Host "Backed up previous token to $backupPath"
}

$token = New-SecureToken
Set-Content -Path $resolvedTokenPath -Value $token -NoNewline
Write-Host "Rotated pilot token at $resolvedTokenPath"
Write-Host "Restart the pilot service to load the new token."
'@ | Set-Content -Path $rotatePs1Path

@'
@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0rotate-pilot-token.ps1" %*
'@ | Set-Content -Path $rotateCmdPath

@'
param(
    [string]$SnapshotDir
)

$ErrorActionPreference = "Stop"

function Resolve-ConfigPath([string]$BaseDir, [string]$PathValue) {
    $path = [System.IO.Path]::IsPathRooted($PathValue) ? $PathValue : (Join-Path $BaseDir $PathValue)
    $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($path)
}

$configPath = Join-Path $PSScriptRoot "config\pilot-service.json"
$config = Get-Content -Path $configPath -Raw | ConvertFrom-Json
$configDir = Split-Path -Parent $configPath
$databasePath = Resolve-ConfigPath $configDir $config.database_path
$databaseWalPath = "$databasePath-wal"
$databaseShmPath = "$databasePath-shm"
$sidecarPath = "$databasePath.sidecars.sqlite"
$sidecarWalPath = "$sidecarPath-wal"
$sidecarShmPath = "$sidecarPath-shm"
$auditPath = Resolve-ConfigPath $configDir $config.audit_log_path

if (-not $SnapshotDir) {
    $SnapshotDir = Join-Path $PSScriptRoot ("snapshots\pilot-" + (Get-Date -Format "yyyyMMdd-HHmmss"))
}

$snapshotDir = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($SnapshotDir)
$snapshotConfigDir = Join-Path $snapshotDir "config"
$snapshotDataDir = Join-Path $snapshotDir "data"
$snapshotLogsDir = Join-Path $snapshotDir "logs"
New-Item -ItemType Directory -Force -Path $snapshotConfigDir, $snapshotDataDir, $snapshotLogsDir | Out-Null

Copy-Item -Path $configPath -Destination (Join-Path $snapshotConfigDir "pilot-service.json")

$tokenFiles = @()
foreach ($token in $config.auth.tokens) {
    if ($token.token_file) {
        $source = Resolve-ConfigPath $configDir $token.token_file
        if (Test-Path $source) {
            $leaf = Split-Path -Leaf $source
            Copy-Item -Path $source -Destination (Join-Path $snapshotConfigDir $leaf)
            $tokenFiles += [pscustomobject]@{
                source = $source
                leaf = $leaf
            }
        }
    }
}

if (Test-Path $databasePath) {
    Copy-Item -Path $databasePath -Destination (Join-Path $snapshotDataDir (Split-Path -Leaf $databasePath))
}
if (Test-Path $databaseWalPath) {
    Copy-Item -Path $databaseWalPath -Destination (Join-Path $snapshotDataDir (Split-Path -Leaf $databaseWalPath))
}
if (Test-Path $databaseShmPath) {
    Copy-Item -Path $databaseShmPath -Destination (Join-Path $snapshotDataDir (Split-Path -Leaf $databaseShmPath))
}
if (Test-Path $sidecarPath) {
    Copy-Item -Path $sidecarPath -Destination (Join-Path $snapshotDataDir (Split-Path -Leaf $sidecarPath))
}
if (Test-Path $sidecarWalPath) {
    Copy-Item -Path $sidecarWalPath -Destination (Join-Path $snapshotDataDir (Split-Path -Leaf $sidecarWalPath))
}
if (Test-Path $sidecarShmPath) {
    Copy-Item -Path $sidecarShmPath -Destination (Join-Path $snapshotDataDir (Split-Path -Leaf $sidecarShmPath))
}
if (Test-Path $auditPath) {
    Copy-Item -Path $auditPath -Destination (Join-Path $snapshotLogsDir (Split-Path -Leaf $auditPath))
}

$manifest = [pscustomobject]@{
    generated_at = (Get-Date).ToString("o")
    config_path = $configPath
    database_path = $databasePath
    database_wal_path = $databaseWalPath
    database_shm_path = $databaseShmPath
    sidecar_path = $sidecarPath
    sidecar_wal_path = $sidecarWalPath
    sidecar_shm_path = $sidecarShmPath
    audit_log_path = $auditPath
    token_files = $tokenFiles
}
$manifest | ConvertTo-Json -Depth 8 | Set-Content -Path (Join-Path $snapshotDir "manifest.json")
Write-Host "Pilot snapshot exported to $snapshotDir"
'@ | Set-Content -Path $backupPs1Path

@'
@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0backup-pilot-state.ps1" %*
'@ | Set-Content -Path $backupCmdPath

@'
param(
    [Parameter(Mandatory = $true)]
    [string]$SnapshotDir,
    [switch]$BackupExisting = $true
)

$ErrorActionPreference = "Stop"

function Resolve-ConfigPath([string]$BaseDir, [string]$PathValue) {
    $path = [System.IO.Path]::IsPathRooted($PathValue) ? $PathValue : (Join-Path $BaseDir $PathValue)
    $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($path)
}

$snapshotDir = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($SnapshotDir)
$manifestPath = Join-Path $snapshotDir "manifest.json"
if (-not (Test-Path $manifestPath)) {
    throw "Snapshot manifest not found at $manifestPath"
}

$configPath = Join-Path $PSScriptRoot "config\pilot-service.json"
$config = Get-Content -Path $configPath -Raw | ConvertFrom-Json
$configDir = Split-Path -Parent $configPath
$databasePath = Resolve-ConfigPath $configDir $config.database_path
$databaseWalPath = "$databasePath-wal"
$databaseShmPath = "$databasePath-shm"
$sidecarPath = "$databasePath.sidecars.sqlite"
$sidecarWalPath = "$sidecarPath-wal"
$sidecarShmPath = "$sidecarPath-shm"
$auditPath = Resolve-ConfigPath $configDir $config.audit_log_path

function Restore-OptionalFile([string]$SnapshotPath, [string]$DestinationPath) {
    for ($attempt = 1; $attempt -le 40; $attempt++) {
        try {
            if (Test-Path $SnapshotPath) {
                Copy-Item -Path $SnapshotPath -Destination $DestinationPath -Force
            } elseif (Test-Path $DestinationPath) {
                Remove-Item -Force $DestinationPath
            }
            return
        } catch {
            if ($attempt -eq 40) {
                throw
            }
            Start-Sleep -Milliseconds 250
        }
    }
}

if ($BackupExisting) {
    $backupDir = Join-Path $PSScriptRoot ("restore-backup-" + (Get-Date -Format "yyyyMMdd-HHmmss"))
    $backupConfigDir = Join-Path $backupDir "config"
    $backupDataDir = Join-Path $backupDir "data"
    $backupLogsDir = Join-Path $backupDir "logs"
    New-Item -ItemType Directory -Force -Path $backupConfigDir, $backupDataDir, $backupLogsDir | Out-Null
    foreach ($path in @($configPath, $databasePath, $databaseWalPath, $databaseShmPath, $sidecarPath, $sidecarWalPath, $sidecarShmPath, $auditPath)) {
        if (Test-Path $path) {
            $destination = switch -Wildcard ($path) {
                "$configDir*" { $backupConfigDir }
                "$auditPath" { $backupLogsDir }
                default { $backupDataDir }
            }
            Copy-Item -Path $path -Destination (Join-Path $destination (Split-Path -Leaf $path))
        }
    }
    Write-Host "Backed up current pilot state to $backupDir"
}

Copy-Item -Path (Join-Path $snapshotDir "config\pilot-service.json") -Destination $configPath -Force
Get-ChildItem -Path (Join-Path $snapshotDir "config") -Filter *.token -File -ErrorAction SilentlyContinue | ForEach-Object {
    Copy-Item -Path $_.FullName -Destination (Join-Path (Split-Path -Parent $configPath) $_.Name) -Force
}

$snapshotDb = Join-Path $snapshotDir ("data\" + (Split-Path -Leaf $databasePath))
$snapshotDbWal = Join-Path $snapshotDir ("data\" + (Split-Path -Leaf $databaseWalPath))
$snapshotDbShm = Join-Path $snapshotDir ("data\" + (Split-Path -Leaf $databaseShmPath))
$snapshotSidecars = Join-Path $snapshotDir ("data\" + (Split-Path -Leaf $sidecarPath))
$snapshotSidecarWal = Join-Path $snapshotDir ("data\" + (Split-Path -Leaf $sidecarWalPath))
$snapshotSidecarShm = Join-Path $snapshotDir ("data\" + (Split-Path -Leaf $sidecarShmPath))
$snapshotAudit = Join-Path $snapshotDir ("logs\" + (Split-Path -Leaf $auditPath))

if (Test-Path $snapshotDb) {
    Copy-Item -Path $snapshotDb -Destination $databasePath -Force
}
Restore-OptionalFile $snapshotDbWal $databaseWalPath
Restore-OptionalFile $snapshotDbShm $databaseShmPath
if (Test-Path $snapshotSidecars) {
    Copy-Item -Path $snapshotSidecars -Destination $sidecarPath -Force
}
Restore-OptionalFile $snapshotSidecarWal $sidecarWalPath
Restore-OptionalFile $snapshotSidecarShm $sidecarShmPath
if (Test-Path $snapshotAudit) {
    Copy-Item -Path $snapshotAudit -Destination $auditPath -Force
}

Write-Host "Pilot snapshot restored from $snapshotDir"
'@ | Set-Content -Path $restorePs1Path

@'
@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0restore-pilot-state.ps1" %*
'@ | Set-Content -Path $restoreCmdPath

if (Test-Path $zipPath) {
    Remove-Item -Force $zipPath
}
Compress-Archive -Path (Join-Path $packageRoot "*") -DestinationPath $zipPath

Write-Host ""
Write-Host "AETHER pilot package ready" -ForegroundColor Green
Write-Host "Package root: $packageRoot"
Write-Host "Package zip:  $zipPath"
Write-Host "Config:       $configPath"
Write-Host "Token file:   $tokenPath"
Write-Host "Launch with:  $runCmdPath"
Write-Host "Operate with: $opsCmdPath"
Write-Host "Rotate with:  $rotateCmdPath"
Write-Host "Backup with:  $backupCmdPath"
Write-Host "Restore with: $restoreCmdPath"
