@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%run-perturbation-sweep.ps1" -PauseOnExit
exit /b %ERRORLEVEL%
