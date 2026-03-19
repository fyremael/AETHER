# Performance Guide

This document defines the current AETHER performance harness: what it measures, how to run it, and how to read the numbers without overclaiming what they mean.

The current goal is disciplined early tracking, not synthetic bravado. We want stable local baselines, stress workloads that catch regressions, and a clear operator path for generating fresh numbers when people ask.

## What Exists Today

The performance suite now has four layers:

1. A live console dashboard in `crates/aether_api/examples/performance_dashboard.rs`
2. A release-mode markdown report example in `crates/aether_api/examples/performance_report.rs`
3. Criterion benchmarks in `crates/aether_api/benches/kernel_perf.rs`
4. Ignored release-mode stress tests in `crates/aether_api/tests/performance_stress.rs`

All three layers share the same fixture builders in `crates/aether_api/src/perf.rs`, so the report, benchmarks, and stress tests stay aligned.

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

### Operator-facing report

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

- The numbers are single-node local measurements.
- The service benchmark includes parsing, compilation, resolution, runtime evaluation, and query execution.
- Throughput values are derived from mean latency over several samples.
- The stress tests are for correctness under load plus rough elapsed-time observation, not SLO certification.

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

1. Run `cargo run -p aether_api --example performance_report --release` after major semantic or API changes.
2. Run `cargo bench -p aether_api` when changing runtime, resolver, or compiler internals.
3. Run the ignored stress suite before milestone demos, RC tags, or architecture reviews that will invite scaling questions.

## Current Gaps

The current performance program is real, but not complete.

- There is no historical benchmark dashboard yet.
- There is no CI trend gate for benchmark drift.
- Memory tracking is structural rather than allocator-exact.
- The HTTP boundary is not benchmarked independently of the in-process kernel service yet.
- There is no fixture set derived from captured production-like workloads because the project is not at that deployment stage yet.

Those are reasonable next steps once the current harness starts catching real regressions.
