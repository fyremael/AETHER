param(
    [string]$OutputPath
)

$ErrorActionPreference = "Stop"

$bytes = New-Object byte[] 48
[System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
$token = [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')

if ($OutputPath) {
    $resolvedPath = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutputPath)
    $parent = Split-Path -Parent $resolvedPath
    if ($parent) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    Set-Content -Path $resolvedPath -Value $token -NoNewline
    Write-Host "Wrote pilot token to $resolvedPath"
} else {
    Write-Output $token
}
