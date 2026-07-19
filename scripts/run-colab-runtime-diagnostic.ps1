param(
    [string]$CandidateSha,
    [ValidateSet("oauth2", "adc")]
    [string]$AuthProvider = "oauth2"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Path $PSScriptRoot -Parent

if (-not $CandidateSha) {
    $CandidateSha = (& git -C $repoRoot rev-parse HEAD).Trim()
}
if ($CandidateSha -notmatch "^[0-9a-f]{40}$") {
    throw "CandidateSha must be a full lowercase commit SHA"
}

$linuxScript = (& wsl.exe wslpath -a ((Join-Path $PSScriptRoot "run-colab-runtime-diagnostic.sh") -replace "\\", "/")).Trim()
if (-not $linuxScript) {
    throw "failed to resolve the WSL diagnostic launcher path"
}

& wsl.exe -e bash $linuxScript $CandidateSha $AuthProvider
if ($LASTEXITCODE -ne 0) {
    throw "Colab diagnostic failed with exit code $LASTEXITCODE"
}
