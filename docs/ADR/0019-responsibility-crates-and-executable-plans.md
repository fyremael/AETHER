# ADR 0019: Responsibility Crates Replace The API Catch-All

## Status

Accepted

## Context

`aether_api` accumulated semantic orchestration, HTTP, deployment, sidecars,
partition replication, product reports, and performance tooling. That obscured
dependency direction and allowed the runtime to reconstruct scheduling choices
that belonged in the compiled plan.

## Decision

- Extract `aether_service_core`, `aether_http`, `aether_sidecar`,
  `aether_partition`, `aether_perf`, and `aether_pilot` by responsibility.
- Retain `aether_api` as a temporary source-compatible re-export facade for
  existing Rust callers, binaries, examples, benches, and integration tests.
- Forbid owning crates from depending on the facade and forbid production
  crates from depending on `aether_perf`.
- Make `aether_plan` own a versioned executable SCC/stratum schedule,
  extensional bindings, delta anchors, aggregate nodes, and provenance
  requirements. Runtime executes that plan and fails closed on disagreement.
- Preserve semantic behavior with the existing policy, append, execution,
  HTTP, partition, pilot, and performance suites plus an architecture-direction
  test.

## Consequences

- Service semantics can be built and tested without HTTP, partition, pilot, or
  performance ownership.
- HTTP and product surfaces consume inward service contracts instead of owning
  semantic logic.
- Existing `aether_api` imports continue to work during migration, but new code
  must import the owning crate.
- Removing the facade is a later breaking change governed by capability/client
  migration evidence.
