@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%run-demo.ps1" -Demo 04 -PauseOnExit
exit /b %ERRORLEVEL%
