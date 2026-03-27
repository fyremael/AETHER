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
