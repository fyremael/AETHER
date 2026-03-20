@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-performance-drift.ps1" -PauseOnExit
