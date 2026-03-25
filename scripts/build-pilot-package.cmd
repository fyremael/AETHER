@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0build-pilot-package.ps1" %*
