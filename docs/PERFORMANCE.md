# Performance Guide

This document defines the current AETHER benchmark discipline.

The theme is simple: exact local facts, explicit context, no overclaiming.

AETHER now tracks performance as a **host-aware benchmark matrix**. We do not
pretend that one machine, one OS, or one point-in-time baseline explains the
whole system. Instead we record:

- which suite ran
- which host it ran on
- what the runtime looked like
- which workloads are release-gated
- which workloads are measured but still exploratory

## What Exists Today

The performance program now has nine aligned layers:

1. a live console dashboard in `crates/aether_api/examples/performance_dashboard.rs`
2. a host-aware release-mode report example in `crates/aether_api/examples/performance_report.rs`
3. a host-aware baseline capture example in `crates/aether_api/examples/capture_performance_baseline.rs`
4. a host-aware drift comparison example in `crates/aether_api/examples/performance_drift_report.rs`
5. a host snapshot example in `crates/aether_api/examples/performance_host_snapshot.rs`
6. a matrix summary example in `crates/aether_api/examples/performance_matrix_report.rs`
7. a capacity-curve example in `crates/aether_api/examples/performance_capacity_curves.rs`
8. a capacity-report example in `crates/aether_api/examples/performance_capacity_report.rs`
9. Criterion benchmarks in `crates/aether_api/benches/kernel_perf.rs`

All of them share the same fixture builders, suite taxonomy, drift logic, and
run-catalog types in `crates/aether_api/src/perf.rs`.

## Suites

The suite ids are:

- `core_kernel`
- `service_in_process`
- `http_pilot_boundary`
- `replicated_partition`
- `full_stack`

`full_stack` is a composed run of the other groups. It is useful for matrix
artifacts and operator review. It is not the first release gate.

### Current release-gated suites

These are the suites that currently drive the accepted regression gate on the
canonical dev host:

- `core_kernel`
- `service_in_process`

### Current observational suites

These are captured from day one, but they are not fail-level release gates yet:

- `http_pilot_boundary`
- `replicated_partition`

That line is deliberate. We measure them now so we can learn their variance
before pretending they are stable enough to gate releases.

## Workload Groups

### `core_kernel`

- journal append throughput
- resolver `Current`
- resolver `AsOf`
- durable restart plus current replay
- compiler SCC planning
- recursive closure runtime
- tuple explanation runtime

### `service_in_process`

- kernel service coordination run
- durable restart plus coordination replay

### `http_pilot_boundary`

- `GET /health`
- `GET /v1/status`
- `GET /v1/history`
- `POST /v1/reports/pilot/coordination`
- `POST /v1/explain/tuple`
- `POST /v1/reports/pilot/coordination-delta`

### `replicated_partition`

- leader append batch admission
- follower replay and catch-up
- federated history read
- federated run/report latency
- manual promotion latency
- stale-leader append rejection

Imported-fact and federated measurements stay within the current exactness
contract. We benchmark only the supported multi-stream imported-fact path, not
arbitrary joined-row import.

## Host Manifests And Accepted Baselines

Tracked host manifests live in:

- `fixtures/performance/hosts/dev-chad-windows-native.json`
- `fixtures/performance/hosts/dev-chad-wsl-ubuntu.json`
- `fixtures/performance/hosts/github-windows-latest.json`
- `fixtures/performance/hosts/github-ubuntu-latest.json`

Tracked accepted baselines live in:

- `fixtures/performance/baselines/<suite-id>/<host-id>.json`

Today the canonical accepted baselines are:

- `fixtures/performance/baselines/core_kernel/dev-chad-windows-native.json`
- `fixtures/performance/baselines/service_in_process/dev-chad-windows-native.json`

That means the current formal accepted gate is the native Windows dev host:

- host id: `dev-chad-windows-native`
- machine: `CHAD`
- chassis: Dell G5 5090
- CPU: Intel i9-9900K
- cores: 8 physical / 16 logical
- RAM: 64 GB
- OS: Windows 11 Home `10.0.26200`

The GitHub runners and WSL host manifests are part of the matrix from the first
implementation slice, but they are not yet tracked accepted baselines in-repo.

## Run Artifacts

Raw run artifacts live under:

- `artifacts/performance/runs/<timestamp>-<suite-id>-<host-id>/bundle.json`
- `artifacts/performance/runs/<timestamp>-<suite-id>-<host-id>/report.md`
- `artifacts/performance/runs/<timestamp>-<suite-id>-<host-id>/drift.md`

The latest matrix surfaces live under:

- `artifacts/performance/matrix/latest.json`
- `artifacts/performance/matrix/latest.md`

The latest perturbation and capacity surfaces now also live under:

- `artifacts/performance/perturbation/latest.json`
- `artifacts/performance/perturbation/latest.md`
- `artifacts/performance/capacity/latest.json`
- `artifacts/performance/capacity/latest.md`

The local convenience copies still exist too:

- `artifacts/performance/latest.json`
- `artifacts/performance/latest.md`
- `artifacts/performance/latest-drift.md`
- `artifacts/performance/latest-drift-core_kernel.md`
- `artifacts/performance/latest-drift-service_in_process.md`

## Commands

### Live console dashboard

Windows operator path:

```text
double-click scripts/run-performance-dashboard.cmd
```

Technical path:

```bash
cargo run -p aether_api --example performance_dashboard --release
```

### Capture a host snapshot

```bash
cargo run -p aether_api --example performance_host_snapshot --release
```

Use this when you need the auto-discovered runtime facts without running the
full suite.

### Run a host-aware report

Windows operator path:

```text
double-click scripts/run-performance-report.cmd
```

Technical path:

```bash
cargo run -p aether_api --example performance_report --release -- \
  --suite full_stack \
  --host-manifest fixtures/performance/hosts/dev-chad-windows-native.json \
  --bundle-path artifacts/performance/runs/<timestamp>-full_stack-dev-chad-windows-native/bundle.json \
  --report-path artifacts/performance/runs/<timestamp>-full_stack-dev-chad-windows-native/report.md
```

The PowerShell runner writes the timestamped bundle/report pair and refreshes
`artifacts/performance/latest.{json,md}`.

### Capture an accepted or local baseline

Windows operator path:

```text
double-click scripts/run-performance-baseline.cmd
```

Technical path:

```bash
cargo run -p aether_api --example capture_performance_baseline --release -- \
  --suite core_kernel \
  --host-manifest fixtures/performance/hosts/dev-chad-windows-native.json \
  --output fixtures/performance/baselines/core_kernel/dev-chad-windows-native.json
```

Use repo-tracked fixture paths only when you are deliberately updating an
accepted reference. For local experiments, write to
`artifacts/performance/baselines/<suite-id>/<host-id>.json`.

### Run a same-host drift comparison

Windows operator path:

```text
double-click scripts/run-performance-drift.cmd
```

Technical path:

```bash
cargo run -p aether_api --example performance_drift_report --release -- \
  --suite core_kernel \
  --host-manifest fixtures/performance/hosts/dev-chad-windows-native.json \
  --baseline fixtures/performance/baselines/core_kernel/dev-chad-windows-native.json
```

The drift tool rejects suite mismatch and host-manifest mismatch by default. A
cross-host comparison belongs in the matrix report, not in the release gate.

### Run a local Windows plus WSL matrix

Windows operator path:

```text
double-click scripts/run-performance-matrix.cmd
```

Technical path:

```bash
powershell -ExecutionPolicy Bypass -File scripts/run-performance-matrix.ps1 -Suite full_stack
```

If WSL is unavailable, the matrix report stays honest and records that absence
explicitly instead of silently dropping the Linux row.

### Build a matrix summary from run bundles

```bash
cargo run -p aether_api --example performance_matrix_report --release -- \
  --output-json artifacts/performance/matrix/latest.json \
  --output-report artifacts/performance/matrix/latest.md \
  <bundle-path-1> <bundle-path-2> ...
```

### Run the perturbation sweep

Windows operator path:

```text
double-click scripts/run-perturbation-sweep.cmd
```

Technical path:

```bash
powershell -ExecutionPolicy Bypass -File scripts/run-perturbation-sweep.ps1 -SkipHardening
```

That sweep now refreshes:

- a fresh full-stack benchmark bundle
- accepted drift reports for `core_kernel` and `service_in_process`
- the ignored release-mode stress ladder
- the measured capacity-curve bundle used by the planner

### Build the capacity report

Windows operator path:

```text
double-click scripts/run-capacity-planner.cmd
```

Technical path:

```bash
powershell -ExecutionPolicy Bypass -File scripts/run-capacity-planner.ps1 -SkipHardening
```

That runner ensures perturbation and matrix prerequisites exist, then writes:

- `artifacts/performance/capacity/latest.json`
- `artifacts/performance/capacity/latest.md`
- timestamped siblings under `artifacts/performance/capacity/runs/`

The capacity layer is internal planning guidance for node classes, single-node
envelopes, and partition/federation triggers. It is not a public SLA surface.

## Launch And Release Gates

`run-pilot-launch-validation.ps1` and `run-release-readiness.ps1` now resolve
baselines by **suite + host id**, not by a single anonymous baseline file.

For the canonical local Windows host, the launch path resolves in this order:

1. explicit `-BaselinePath` for the `core_kernel` suite
2. local artifact baseline in `artifacts/performance/baselines/<suite>/<host>.json`
3. tracked fixture baseline in `fixtures/performance/baselines/<suite>/<host>.json`

The launch pack runs:

- `full_stack` report capture
- `core_kernel` drift
- `service_in_process` drift
- pilot report
- release-mode API tests
- soak suite
- stress suite

The current release gate treats:

- `core_kernel` and `service_in_process` as gated
- `http_pilot_boundary` and `replicated_partition` as measured but observational

## Reading The Numbers

Use the benchmark bundle as a context-bearing record, not a boast.

The right questions are:

- Did the same host regress on the same suite?
- Did a kernel change inflate derived-set or trace footprint?
- What changed between the dev host, WSL, and GitHub runners?
- Are HTTP or replicated-path costs stabilizing enough to become future gates?

The wrong question is:

- “What exact production capacity does this prove?”

This suite is still single-host instrumentation with host facts plus
kernel/runtime counters. It is not profiler-grade telemetry, multi-node
capacity certification, or production traffic replay.

## CI And Matrix Reporting

The current rollout is:

- native Windows dev host remains the first accepted release baseline
- `release-readiness` and `pilot-launch-validation` remain anchored to the mature Windows `core_kernel` and `service_in_process` slices
- the `Performance Matrix` workflow publishes Windows and Ubuntu run bundles plus a comparative summary
- HTTP and replicated-partition measurements are emitted into artifacts immediately, but they do not fail the release gate yet

## Current Gaps

The matrix is real, but it is not the end of the story.

- Historical trend storage is still artifact-based rather than a persistent benchmark database.
- GitHub-host accepted baselines are not yet promoted into tracked fixture references.
- HTTP and replicated-partition suites are measured, but they are still observational rather than fail-level release gates.
- Memory figures remain structural lower-bound estimates rather than allocator-exact telemetry.
- Telemetry stops at host facts plus benchmark counters; profiler-grade CPU or allocator tracing is still out of scope for this phase.
