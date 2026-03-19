@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%run-performance-dashboard.ps1" -PauseOnExit
exit /b %ERRORLEVEL%
