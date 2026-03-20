# Scripts

Repository automation, fixture generation, release support scripts, and operator-facing demo launchers live here.

For the operator-facing presentation flow and demo-selection guidance, read `docs/OPERATIONS.md` first.

For documentation publishing:

- run `python scripts/build_pages.py --out-dir artifacts/pages-preview` after `cargo doc --workspace --no-deps` to stage a local Pages preview bundle

For non-technical operators on Windows:

- double-click `run-demo-01.cmd` to run the first AETHER demonstration
- double-click `run-demo-02.cmd` to run the multi-worker lease handoff demonstration
- double-click `run-demo-03.cmd` to run the flagship coordination situation-room showcase
- double-click `run-pilot-report.cmd` to generate the current coordination pilot report artifacts
- double-click `run-pilot-launch-validation.cmd` to run the full launch-candidate validation pack
- double-click `run-performance-dashboard.cmd` to watch the live console performance dashboard
- double-click `run-performance-report.cmd` to generate the current release-mode performance report
- double-click `run-performance-baseline.cmd` to capture the current accepted performance baseline
- double-click `run-performance-drift.cmd` to compare the current build to that baseline

For technical users or automation:

- run `powershell -ExecutionPolicy Bypass -File scripts/run-demo.ps1 -Demo 01`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-demo.ps1 -Demo 02`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-demo.ps1 -Demo 03`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-pilot-report.ps1`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-pilot-launch-validation.ps1`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-performance-dashboard.ps1`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-performance-report.ps1`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-performance-baseline.ps1`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-performance-drift.ps1`
- run `cargo run -p aether_api --example pilot_coordination_report --release`
- run `cargo run -p aether_api --example capture_performance_baseline --release`
- run `cargo run -p aether_api --example performance_drift_report --release -- artifacts/performance/baseline.json`
- run `cargo run -p aether_api --example pilot_http_kernel_service --release`
- run `cargo bench -p aether_api`
- run `cargo test -p aether_api --test performance_stress --release -- --ignored --nocapture`

Demo reports are written to `artifacts/demos/`.

Pilot reports are written to `artifacts/pilot/reports/`.

Pilot launch-validation transcripts are written to `artifacts/pilot/launch/`.

Performance reports, baselines, and drift captures are written to `artifacts/performance/`.
