@echo off
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-pilot-launch-validation.ps1" %* -PauseOnExit
exit /b %ERRORLEVEL%
