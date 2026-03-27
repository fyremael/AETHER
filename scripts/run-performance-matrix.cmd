@echo off
setlocal
pwsh -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-performance-matrix.ps1" %*
exit /b %ERRORLEVEL%
