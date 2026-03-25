# Performance Guide

This document defines the current AETHER performance harness: what it measures, how to run it, how drift is tracked, and how to read the numbers without overclaiming what they mean.

The current goal is disciplined early tracking, not synthetic bravado. We want stable local baselines, stress workloads that catch regressions, and a clear operator path for generating fresh numbers when people ask.

## What Exists Today

The performance suite now has six layers:

1. A live console dashboard in `crates/aether_api/examples/performance_dashboard.rs`
2. A release-mode markdown report example in `crates/aether_api/examples/performance_report.rs`
3. A machine-readable baseline capture example in `crates/aether_api/examples/capture_performance_baseline.rs`
4. A release-mode drift comparison example in `crates/aether_api/examples/performance_drift_report.rs`
5. Criterion benchmarks in `crates/aether_api/benches/kernel_perf.rs`
6. Ignored release-mode stress tests in `crates/aether_api/tests/performance_stress.rs`

All six layers share the same fixture builders and drift logic in `crates/aether_api/src/perf.rs`, so the dashboard, saved reports, baselines, drift comparisons, benchmarks, and stress tests stay aligned.

## Workloads

The current report and bench suite covers these kernel surfaces:

- journal append throughput
- resolver throughput for both `Current` and `AsOf`
- compiler SCC planning time
- recursive closure runtime over linear dependency chains
- tuple explanation runtime over the deepest recursive tuple in the chain
- end-to-end kernel service runs for a coordination-style claimability query

The footprint estimates currently track:

- derived-set structural size for recursive closure output
- derivation-trace structural size for a deep recursive proof

These footprint figures are intentionally conservative lower-bound estimates. They are useful for regression tracking. They are not allocator-exact memory telemetry.

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

That view streams sample-by-sample timing and throughput data while the suite is running, then leaves the collected measures and footprint estimates in the same console surface.

### Operator-facing report

Windows operator path:

```text
double-click scripts/run-performance-report.cmd
```

Technical path:

```bash
cargo run -p aether_api --example performance_report --release
```

The PowerShell runner writes reports to `artifacts/performance/`, including a timestamped report and `latest.md`.

### Baseline capture

Windows operator path:

```text
double-click scripts/run-performance-baseline.cmd
```

Technical path:

```bash
cargo run -p aether_api --example capture_performance_baseline --release
```

That command writes `artifacts/performance/baseline.json`. Treat that file as the current local reference point for drift comparison.

### Drift comparison

Windows operator path:

```text
double-click scripts/run-performance-drift.cmd
```

Technical path:

```bash
cargo run -p aether_api --example performance_drift_report --release -- artifacts/performance/baseline.json
```

The PowerShell runner writes a timestamped markdown capture plus `latest-drift.md` to `artifacts/performance/`.

For reproducible review on a fresh machine, the repository also carries a tracked accepted baseline at `fixtures/performance/accepted-baseline.windows-x86_64.json`.

By default, the drift comparison applies these budgets:

- throughput regression warning at `15%`
- throughput regression failure at `30%`
- footprint growth warning at `10%`
- footprint growth failure at `20%`

The example exits with code `2` when any workload crosses a fail-level threshold. That behavior is intentional so the same tool can become a future CI gate.

The repository now also runs this drift path through both:

- the mainline `CI` workflow as part of the required `pilot-launch-gate`
- the scheduled/manual `Pilot Validation` workflow, which uploads the generated performance and drift artifacts for review

### Pilot launch validation

Windows operator path:

```text
double-click scripts/run-pilot-launch-validation.cmd
```

Technical path:

```bash
powershell -ExecutionPolicy Bypass -File scripts/run-pilot-launch-validation.ps1
```

That validation pack runs the pilot report, performance report, drift comparison, release-mode `aether_api` tests, the ignored pilot soak suite, and the ignored performance stress suite, then writes a transcript to `artifacts/pilot/launch/`.

By default it prefers `artifacts/performance/baseline.json` when present and otherwise falls back to `fixtures/performance/accepted-baseline.windows-x86_64.json`. Pass `-BaselinePath <path>` when you need to pin the comparison to a different accepted reference.

### Criterion benchmarks

```bash
cargo bench -p aether_api
```

That suite exercises the same fixture set with Criterion’s statistical harness.

### Stress tests

```bash
cargo test -p aether_api --test performance_stress --release -- --ignored --nocapture
```

Those tests are intentionally heavier and are excluded from the normal CI loop.

## Reading The Numbers

Use the report as a baseline, not a promise.

Use the dashboard when you need live visibility into how the suite is progressing.

Use the baseline and drift tools together when you need to answer whether the kernel has materially slowed down since the last accepted pilot checkpoint.

- The numbers are single-node local measurements.
- The service benchmark includes parsing, compilation, resolution, runtime evaluation, and query execution.
- Throughput values are derived from mean latency over several samples.
- The stress tests are for correctness under load plus rough elapsed-time observation, not SLO certification.
- The drift report compares like-for-like workload keys only. Missing baseline entries are called out explicitly rather than guessed.

The right questions to ask of this suite are:

- Did throughput regress relative to the previous baseline?
- Did a code change inflate derived-set or trace footprint materially?
- Can the current machine still handle the coordination and recursive workloads we intend to demo?
- Are there workloads now large enough that the next optimization phase should become urgent?

The wrong question is:

- “What exact production capacity does this prove?”

The repository does not yet have durable storage, network fanout, multi-node execution, or production workload capture, so the current suite should not be presented as production capacity certification.

## Recommended Operating Rhythm

Use this rhythm unless the work is unusually narrow:

1. Capture a fresh baseline with `cargo run -p aether_api --example capture_performance_baseline --release` at every pilot checkpoint you want to preserve.
2. Run `cargo run -p aether_api --example performance_drift_report --release -- artifacts/performance/baseline.json` after major semantic or API changes.
3. Run `cargo run -p aether_api --example performance_report --release` when you need a shareable markdown artifact instead of only pass/fail drift output.
4. Run `cargo bench -p aether_api` when changing runtime, resolver, or compiler internals.
5. Run the ignored stress suite before milestone demos, RC tags, or architecture reviews that will invite scaling questions.

## Current Gaps

The current performance program is real, but not complete.

- Baseline comparison is point-in-time rather than a historical trend store.
- The launch/drift gate is now enforced in CI, but the project still lacks a historical benchmark trend store beyond point-in-time baselines and uploaded workflow artifacts.
- Memory tracking is structural rather than allocator-exact.
- The HTTP boundary is not benchmarked independently of the in-process kernel service yet.
- There is no fixture set derived from captured production-like workloads because the project is not at that deployment stage yet.

Those are reasonable next steps once the current harness starts catching real regressions.
