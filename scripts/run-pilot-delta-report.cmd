@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-pilot-delta-report.ps1" %*
