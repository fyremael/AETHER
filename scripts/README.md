# Scripts

Repository automation, fixture generation, release support scripts, and operator-facing demo launchers live here.

For the operator-facing presentation flow and demo-selection guidance, read `docs/OPERATIONS.md` first.

For documentation publishing:

- run `python scripts/build_pages.py --out-dir artifacts/pages-preview` after `cargo doc --workspace --no-deps` to stage a local Pages preview bundle

For non-technical operators on Windows:

- double-click `run-demo-01.cmd` to run the first AETHER demonstration
- double-click `run-demo-02.cmd` to run the multi-worker lease handoff demonstration
- double-click `run-demo-03.cmd` to run the flagship coordination situation-room showcase
- double-click `run-performance-dashboard.cmd` to watch the live console performance dashboard
- double-click `run-performance-report.cmd` to generate the current release-mode performance report

For technical users or automation:

- run `powershell -ExecutionPolicy Bypass -File scripts/run-demo.ps1 -Demo 01`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-demo.ps1 -Demo 02`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-demo.ps1 -Demo 03`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-performance-dashboard.ps1`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-performance-report.ps1`
- run `cargo bench -p aether_api`
- run `cargo test -p aether_api --test performance_stress --release -- --ignored --nocapture`

Demo reports are written to `artifacts/demos/`.

Performance reports are written to `artifacts/performance/`.
