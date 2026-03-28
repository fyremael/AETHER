# ADR 0006: Go Is A Shell, Not The Core Runtime

- Status: Accepted
- Date: 2026-03-28
- Deciders: AETHER maintainers
- Related:
  - `SPEC.md`
  - `IMPLEMENTATION_DECISION.md`
  - `INTERFACES.md`
  - `docs/OPERATIONS.md`

## Context

Go is valuable in AETHER for operational work:

- CLI surfaces
- typed HTTP clients
- service wrappers
- deployment ergonomics
- operator TUI surfaces

That value is real, but it creates a recurring architectural temptation:
if enough operator logic accumulates in Go, the shell can start to look like
the product center.

For AETHER, that would be the wrong center of gravity.
The semantic runtime, temporal model, and recursive derivation engine should not
be reimplemented or quietly migrated into the Go layer.

## Decision

Go remains a shell and boundary layer rather than the core runtime.

Go may own:

- CLI and TUI experiences
- HTTP client ergonomics
- packaging and operator-facing tooling
- thin service wrappers around kernel surfaces

Go does not own:

- authoritative rule evaluation
- deterministic temporal resolution
- provenance-bearing derivation semantics
- a shadow implementation of the kernel

The expected posture is boundary consumption, not semantic duplication.

## Consequences

Positive:

- operator tooling can move quickly without redefining the kernel
- one authoritative implementation remains responsible for semantic correctness
- the service and operator layers stay easier to reason about

Tradeoffs:

- some features require boundary expansion instead of a quick local Go-only fix
- Go contributors must treat Rust outputs as authoritative rather than
  re-deriving meaning locally

## Rejected Alternatives

### Dual Rust and Go runtimes

Rejected because duplicate semantic implementations drift and multiply the cost
of maintaining correctness.

### Moving rule evaluation into Go for service convenience

Rejected because service convenience should not dictate kernel architecture.
