# Scripts

Repository automation, fixture generation, release support scripts, and operator-facing demo launchers live here.

For non-technical operators on Windows:

- double-click `run-demo-01.cmd` to run the first AETHER demonstration
- double-click `run-demo-02.cmd` to run the multi-worker lease handoff demonstration

For technical users or automation:

- run `powershell -ExecutionPolicy Bypass -File scripts/run-demo.ps1 -Demo 01`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-demo.ps1 -Demo 02`

Demo reports are written to `artifacts/demos/`.
