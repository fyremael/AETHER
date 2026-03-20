# Coordination Pilot

This document defines the current AETHER pilot program.

It is intentionally narrow. The goal is not to prove that AETHER is already a general platform. The goal is to prove that the kernel can carry one serious coordination story through durable storage, a network boundary, explanation, and operational review without semantic drift.

## Pilot Thesis

The pilot asks one question:

Can AETHER act as an authoritative coordination kernel for task readiness and lease authority, with durable replay and operator-legible answers?

For this phase, that question is enough.

## Scope

The pilot includes exactly two coordination workloads:

1. readiness and claimability across dependency graphs
2. lease authority, handoff, and stale-attempt fencing

The pilot system is intentionally constrained:

- single-node Rust service
- append-only durable journal
- `Current` and `AsOf` replay
- recursive derivation
- tuple explanation
- HTTP JSON service boundary

The pilot does not try to prove multi-node consensus, global scheduling, or a full workflow product surface.

## Current Pilot Baseline

This slice is now backed by executable contracts in the repository.

Implemented:

- durable SQLite-backed journal behind the existing `Journal` trait
- restart-safe history and inclusive-prefix replay
- kernel services that can run over either in-memory or durable journal backends
- restart-safe coordination contract tests at the service layer
- restart-safe coordination contract tests through the HTTP boundary
- bearer-token authentication with endpoint scope enforcement on the pilot HTTP path
- auditable request logging with in-memory inspection, semantic request context, and JSONL persistence on the pilot HTTP path
- a dedicated durable HTTP service example at `crates/aether_api/examples/pilot_http_kernel_service.rs`
- operator-grade coordination report generation in markdown and JSON
- durable pilot seed fixtures shared between service tests and report generation
- performance baseline capture for the pilot path
- performance drift comparison with warning and fail thresholds
- repeated authenticated restart-cycle drills that preserve both semantic answers and persisted audit context
- ignored release-mode soak and misuse drills for the authenticated pilot HTTP path
- a one-command launch validation pack that produces the current pilot report, performance report, drift check, soak output, and stress output

Those tests intentionally freeze the current answers for:

- `AsOf(e5)` authorization
- current authorization
- current claimability
- current stale-attempt rejection
- tuple explanation availability

## Commands

Run the durable pilot service locally:

```bash
cargo run -p aether_api --example pilot_http_kernel_service --release
```

Use a custom database path if needed:

```bash
cargo run -p aether_api --example pilot_http_kernel_service --release -- artifacts/pilot/my-coordination.sqlite
```

The default database path is:

```text
artifacts/pilot/coordination.sqlite
```

The default pilot bearer token is:

```text
pilot-operator-token
```

Override it with the `AETHER_PILOT_TOKEN` environment variable before starting the service.

Generate an operator-facing pilot report:

```bash
cargo run -p aether_api --example pilot_coordination_report --release
```

Windows operators can double-click:

```text
scripts/run-pilot-report.cmd
```

Capture the current local performance baseline:

```bash
cargo run -p aether_api --example capture_performance_baseline --release
```

Compare the current build against that baseline:

```bash
cargo run -p aether_api --example performance_drift_report --release -- artifacts/performance/baseline.json
```

For reproducible QA and launch validation on a fresh machine, the repo also carries `fixtures/performance/accepted-baseline.windows-x86_64.json` as a tracked accepted reference.

## Exit Gates

The pilot is only ready for external design-partner use when all of these are true:

1. the durable journal reproduces the same semantic answers before and after restart
2. the HTTP boundary reproduces the same semantic answers before and after restart
3. operator-facing explain output is sufficient to answer why a worker is authorized or fenced
4. benchmark baselines exist for the durable pilot paths and drift is tracked over time
5. operator-grade explain and incident-report artifacts exist for the pilot workloads

The current implementation closes those original gates for the present single-node pilot.

The current launch validation path is documented in `docs/PILOT_LAUNCH.md`.

## Next Required Work

The next pilot-critical steps after launch are:

- optional CI or scheduled automation adoption of the launch validation pack
- longer-duration soak drills beyond the current launch window
- richer operator-intent and semantic-diff context on top of the current audit fields
- service-hardening work beyond the current bearer-token and single-node posture

Those are the next things to do. They are the post-launch hardening road for the pilot, not blockers for the current single-node design-partner launch.

## Non-Goals

These are deliberately outside the current pilot:

- full canonical DSL completion
- bounded aggregation
- multi-tenant authorization semantics
- cluster coordination or replica consensus
- stable Go and Python clients
- production deployment claims

The discipline of the pilot is to keep the proof narrow enough that it can actually be finished.
