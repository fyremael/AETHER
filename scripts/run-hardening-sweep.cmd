@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-hardening-sweep.ps1" %*
