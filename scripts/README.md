# Scripts

Repository automation, fixture generation, release support scripts, and operator-facing demo launchers live here.

For the operator-facing presentation flow and demo-selection guidance, read `docs/OPERATIONS.md` first.

For documentation publishing:

- run `python scripts/build_pages.py --out-dir artifacts/pages-preview` after `cargo doc --workspace --no-deps` to stage a local Pages preview bundle

## Windows Operator Shortcuts

### Demos

- double-click `run-demo-01.cmd`
- double-click `run-demo-02.cmd`
- double-click `run-demo-03.cmd`
- double-click `run-demo-04.cmd`

### Pilot reports and validation

- double-click `run-pilot-report.cmd`
- double-click `run-pilot-delta-report.cmd`
- double-click `run-pilot-launch-validation.cmd`
- double-click `run-release-readiness.cmd`

### Deployment helpers

- double-click `build-pilot-package.cmd`
- double-click `new-pilot-token.cmd`

### Performance

- double-click `run-performance-dashboard.cmd`
- double-click `run-performance-report.cmd`
- double-click `run-performance-baseline.cmd`
- double-click `run-performance-drift.cmd`
- double-click `run-performance-matrix.cmd`

## Technical Commands

### Demos and pilot reporting

- run `powershell -ExecutionPolicy Bypass -File scripts/run-demo.ps1 -Demo 01`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-demo.ps1 -Demo 02`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-demo.ps1 -Demo 03`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-demo.ps1 -Demo 04`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-pilot-report.ps1`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-pilot-delta-report.ps1`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-pilot-launch-validation.ps1`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-pilot-launch-validation.ps1 -BaselinePath <accepted-baseline-path>`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-release-readiness.ps1`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-release-readiness.ps1 -BaselinePath <accepted-baseline-path>`

### Deployment and auth helpers

- run `powershell -ExecutionPolicy Bypass -File scripts/build-pilot-package.ps1`
- run `powershell -ExecutionPolicy Bypass -File scripts/new-pilot-token.ps1 -OutputPath <token-file>`
- run `cargo run -p aether_api --bin aether_pilot_service --release -- --config <path-to-config>`
- run `cargo run -p aether_api --example pilot_http_kernel_service --release`
- run `cargo run -p aether_api --example replicated_partition_http_service --release`

### Performance and stress

- run `powershell -ExecutionPolicy Bypass -File scripts/run-performance-dashboard.ps1`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-performance-report.ps1`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-performance-baseline.ps1`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-performance-drift.ps1`
- run `powershell -ExecutionPolicy Bypass -File scripts/run-performance-matrix.ps1`
- run `cargo run -p aether_api --example pilot_coordination_report --release`
- run `cargo run -p aether_api --example pilot_coordination_delta_report --release`
- run `cargo run -p aether_api --example capture_performance_baseline --release -- --suite core_kernel --host-manifest fixtures/performance/hosts/dev-chad-windows-native.json --output fixtures/performance/baselines/core_kernel/dev-chad-windows-native.json`
- run `cargo run -p aether_api --example performance_drift_report --release -- --suite core_kernel --host-manifest fixtures/performance/hosts/dev-chad-windows-native.json --baseline fixtures/performance/baselines/core_kernel/dev-chad-windows-native.json`
- run `cargo run -p aether_api --example performance_matrix_report --release -- --output-json artifacts/performance/matrix/latest.json --output-report artifacts/performance/matrix/latest.md <bundle-path-1> <bundle-path-2>`
- run `cargo bench -p aether_api`
- run `cargo test -p aether_api --test performance_stress --release -- --ignored --nocapture`

### Docs and presentation assets

- run `python scripts/build_pages.py --out-dir artifacts/pages-preview`
- run `python scripts/build_presentation_assets.py`

Demo reports are written to `artifacts/demos/`.

Pilot reports are written to `artifacts/pilot/reports/`.

Pilot launch-validation transcripts are written to `artifacts/pilot/launch/`.

Structured release-readiness transcripts and summaries are written to `artifacts/qa/release-readiness/`.

Packaged pilot-service bundles are written to `artifacts/pilot/packages/`.

Those bundles now include a package-local `rotate-pilot-token.cmd` helper plus both `PILOT_DEPLOYMENT.md` and `PILOT_OPERATIONS_PLAYBOOK.md` so deployment, rotation, and upgrade guidance can travel with the binary.

They now also include:

- `bin/aetherctl.exe`
- `run-aether-ops.cmd`
- `run-aether-ops.ps1`
- `backup-pilot-state.cmd`
- `backup-pilot-state.ps1`
- `restore-pilot-state.cmd`
- `restore-pilot-state.ps1`

That makes the packaged bundle self-contained for both the pilot service and the
read-only operator cockpit.

Performance reports, baselines, drift captures, and matrix summaries are written under `artifacts/performance/`, with timestamped run bundles in `artifacts/performance/runs/` and matrix summaries in `artifacts/performance/matrix/`.

The launch-validation runner now resolves host-aware, suite-specific baselines from `artifacts/performance/baselines/<suite>/<host>.json` first and then `fixtures/performance/baselines/<suite>/<host>.json`, and records the chosen source in the transcript.

Exportable presentation assets are written to `site/assets/presentation/` and exposed through the live Pages showcase in `site/showcase.html`.
