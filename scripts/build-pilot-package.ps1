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
$templatePath = Join-Path $repoRoot "fixtures\deployment\pilot-service.template.json"
$readmeSource = Join-Path $repoRoot "docs\PILOT_DEPLOYMENT.md"
$binDir = Join-Path $packageRoot "bin"
$configDir = Join-Path $packageRoot "config"
$docsDir = Join-Path $packageRoot "docs"
$dataDir = Join-Path $packageRoot "data"
$logsDir = Join-Path $packageRoot "logs"
$tokenPath = Join-Path $configDir "pilot-operator.token"
$configPath = Join-Path $configDir "pilot-service.json"
$runPs1Path = Join-Path $packageRoot "run-pilot-service.ps1"
$runCmdPath = Join-Path $packageRoot "run-pilot-service.cmd"

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
}

if (-not (Test-Path $binarySource)) {
    throw "Pilot service binary not found at $binarySource"
}

if (Test-Path $packageRoot) {
    Remove-Item -Recurse -Force $packageRoot
}
New-Item -ItemType Directory -Force -Path $binDir, $configDir, $docsDir, $dataDir, $logsDir | Out-Null

Copy-Item -Path $binarySource -Destination (Join-Path $binDir "aether_pilot_service.exe")
Copy-Item -Path $readmeSource -Destination (Join-Path $docsDir "PILOT_DEPLOYMENT.md")

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
