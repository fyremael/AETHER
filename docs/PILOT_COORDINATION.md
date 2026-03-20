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
- a dedicated durable HTTP service example at `crates/aether_api/examples/pilot_http_kernel_service.rs`

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

## Exit Gates

The pilot is only ready for external design-partner use when all of these are true:

1. the durable journal reproduces the same semantic answers before and after restart
2. the HTTP boundary reproduces the same semantic answers before and after restart
3. operator-facing explain output is sufficient to answer why a worker is authorized or fenced
4. authenticated service access exists with auditable principal identity
5. benchmark baselines exist for the durable pilot paths and drift is tracked over time

This slice closes the first two gates. The remaining gates stay open.

## Next Required Work

The next pilot-critical steps are:

- bearer-token authentication and endpoint authorization
- append/query/explain audit logging
- operator-grade explain and incident-report artifacts
- baseline capture plus benchmark drift budgets for the durable service path

Those are the next things to do. They are not optional polish.

## Non-Goals

These are deliberately outside the current pilot:

- full canonical DSL completion
- bounded aggregation
- multi-tenant authorization semantics
- cluster coordination or replica consensus
- stable Go and Python clients
- production deployment claims

The discipline of the pilot is to keep the proof narrow enough that it can actually be finished.
