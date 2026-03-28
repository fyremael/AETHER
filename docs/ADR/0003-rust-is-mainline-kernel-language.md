# ADR 0003: Rust Is The Mainline Kernel Language

- Status: Accepted
- Date: 2026-03-28
- Deciders: AETHER maintainers
- Related:
  - `SPEC.md`
  - `IMPLEMENTATION_DECISION.md`
  - `INTERFACES.md`
  - `docs/ARCHITECTURE.md`

## Context

AETHER's semantic center depends on a narrow set of properties:

- explicit semantic types
- deterministic replay
- recursive compilation and execution
- provenance-bearing derived tuples
- explainable results
- a library-first kernel that can stand alone under test

That center should not depend on a service shell, SDK ergonomics, or a
host-language callback model for its correctness.

The repository already uses multiple languages on purpose:

- Rust for the kernel
- Go for operator and service-shell work
- Python for SDK, fixture, and research ergonomics

The architectural question is which language owns the authoritative semantic
implementation.

## Decision

Rust is the mainline kernel language for AETHER.

Rust owns the authoritative implementation of:

- ASTs and semantic value types
- schema typing and validation
- journal/storage boundaries
- deterministic resolution for `History`, `Current`, and `AsOf`
- rule compilation, safety checks, stratification, and SCC planning
- semi-naive recursive execution
- derivation provenance and explanation
- kernel-facing API types

Go and Python remain boundary layers around that kernel rather than alternate
semantic centers.

## Consequences

Positive:

- semantic invariants can be modeled in types instead of scattered runtime
  conventions
- the kernel remains a standalone library and test target
- recursive runtime work stays in a language suited to explicit memory and IR
  control
- service, CLI, and SDK work can widen without silently redefining the core

Tradeoffs:

- cross-language boundaries must be maintained deliberately
- some operator and research conveniences arrive more slowly than they would in
  a scripting-first design
- Go and Python consumers must accept the Rust kernel as the source of truth

## Rejected Alternatives

### Go as the semantic center

Rejected because Go is well suited to service shells and operator tooling but
is not the strongest fit for the kernel's recursive, type-rich semantic center.

### Python as the semantic center

Rejected because it weakens the authority boundary, performance posture, and
library-level determinism expected from the kernel.

### Polyglot semantic ownership

Rejected because duplicate semantic implementations drift and make the most
important invariants impossible to defend cleanly.
