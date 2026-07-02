@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-performance-trends.ps1" %*
