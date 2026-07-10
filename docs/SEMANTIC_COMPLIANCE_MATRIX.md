# Semantic Compliance Matrix

This document records AETHER's v1 single-node compliance against sections
`1-11` of [SPEC.md](../SPEC.md). The unrestricted kernel slice remains
substantial; the policy-aware portion of semantic closure is reopened following
the July 2026 comprehensive audit.

The active external claim is:

> Controlled single-node alpha with a real Rust semantic kernel, limited to one
> visibility domain, trusted appenders, and explicitly supported deployment
> boundaries.

See `docs/COMPREHENSIVE_AUDIT_2026-07-09.md` for the reproduced failures and
`docs/REMEDIATION_PROGRAMME.md` for the binding R0-R7 gates.

The bar here is intentionally narrow and strict:

- exact local truth on a single-node kernel
- deterministic `History`, `Current`, and `AsOf`
- recursive derivation, stratified negation, bounded aggregation, provenance,
  policy plumbing, and sidecar subordination, with policy noninterference still
  open
- no implied claim of distributed runtime completion, multitenancy, or
  production platform completeness

Where the spec is broader than the current v1 interpretation, that narrowing is
called out explicitly instead of hidden.

## Summary

| SPEC section | Status | Evidence | v1 interpretation |
| --- | --- | --- | --- |
| `1. Objective` | Reopened for policy-aware paths | `crates/aether_storage`, `crates/aether_resolver`, `crates/aether_runtime`, `crates/aether_api/tests/semantic_closure.rs`, comprehensive audit | Unrestricted single-node acceptance exists; policy-scoped replay, derivation, and observable metadata remain open |
| `2. Implementation language strategy` | Complete | Rust kernel crates, Go client/shell, Python SDK, CI in `.github/workflows/ci.yml` | Rust remains authoritative; Go/Python are boundary layers |
| `3. Architectural thesis` | Complete | `docs/ARCHITECTURE.md`, `crates/aether_runtime`, `crates/aether_api/src/sidecar.rs` | Two-center kernel thesis is implemented locally |
| `4. Design stance on Janus` | Complete | Repo layout and implementation shape | Janus remains reference-only, not a compatibility target |
| `5. Core data model` | Reopened at append admission | `crates/aether_ast`, `crates/aether_resolver`, resolver tests, comprehensive audit | Resolver validation exists, but namespace-schema validity is not enforced transactionally before append |
| `6. Provenance model` | Kernel provenance complete; service proof identity reopened | `crates/aether_ast`, `crates/aether_runtime`, `crates/aether_explain`, sidecar tests, comprehensive audit | Datom and derived provenance exist, but a bare process-local tuple ID is not a durable service proof identity |
| `7. Temporal model` | Complete unrestricted; policy scope reopened | `crates/aether_storage`, `crates/aether_resolver`, API/service tests, comprehensive audit | Deterministic replay is exact for an unrestricted journal prefix; policy projection must move before replay |
| `8. Query and phase model` | Complete unrestricted; policy scope reopened | `crates/aether_rules`, `crates/aether_plan`, `crates/aether_api` | Phase-graph and query execution exist, but scoped program facts must be projected before compilation |
| `9. Rule model` | Complete unrestricted; policy scope reopened | `crates/aether_rules`, `crates/aether_runtime`, runtime and API tests, comprehensive audit | Recursion/negation/aggregation execute, but protected inputs must be projected before compilation and evaluation |
| `10. Coordination model` | Complete unrestricted; policy scope reopened | `crates/aether_api/src/pilot.rs`, pilot/report tests, comprehensive audit | Tasks, claims, leases, heartbeats, fencing, and outcomes exist, but policy-scoped readiness/reporting must be recomputed from visible inputs |
| `11. Sidecar model` | Complete unrestricted; policy scope reopened | `crates/aether_api/src/sidecar.rs`, sidecar federation tests, comprehensive audit | Sidecars remain journal-subordinated, but policy dependency closure and noninterference are not yet proven |

## Section Detail

### `1. Objective`

**Status:** Reopened for policy-aware service semantics; complete only for the
unrestricted single-node acceptance slice.

Implemented:

- append-only causal datom journal
- deterministic temporal replay
- cardinality-aware resolution across scalar, set, and sequence classes
- Datalog-native recursive rule execution
- provenance-bearing derivation and policy plumbing; policy noninterference is
  not yet satisfied
- sidecar federation for artifacts and vectors
- narrow Go/Python/API boundaries around the Rust kernel

Primary evidence:

- [crates/aether_api/tests/semantic_closure.rs](../crates/aether_api/tests/semantic_closure.rs)
- [crates/aether_api/tests/sidecar_federation.rs](../crates/aether_api/tests/sidecar_federation.rs)
- [crates/aether_api/tests/http_service.rs](../crates/aether_api/tests/http_service.rs)

### `2. Implementation language strategy`

**Status:** Complete.

Implemented:

- Rust as the authoritative semantic kernel
- Go operator shell and typed HTTP client
- broader typed Python SDK over the HTTP seam

Primary evidence:

- [go/cmd/aetherctl/main.go](../go/cmd/aetherctl/main.go)
- [go/internal/client/client.go](../go/internal/client/client.go)
- [python/aether_sdk/client.py](../python/aether_sdk/client.py)
- [python/aether_sdk/models.py](../python/aether_sdk/models.py)

### `3. Architectural thesis`

**Status:** Complete for exact local truth.

Implemented:

- authoritative semantic substrate: datoms, storage, resolver, sidecar anchors
- recursive semantic closure: rules, SCC planning, semi-naive runtime,
  explanation, and current policy filtering, which is not yet semantic input

Primary evidence:

- [docs/ARCHITECTURE.md](./ARCHITECTURE.md)
- [crates/aether_runtime/src/lib.rs](../crates/aether_runtime/src/lib.rs)
- [crates/aether_api/src/lib.rs](../crates/aether_api/src/lib.rs)

### `4. Design stance on Janus`

**Status:** Complete.

The implementation remains Rust-first and spec-governed without preserving Janus
compatibility as a product constraint.

### `5. Core data model`

**Status:** Complete for resolver-time operation/class interpretation; reopened
for transactional namespace-schema append admission.

Implemented:

- all v1 operation kinds are represented in the AST
- attribute classes drive deterministic resolver behavior
- op/class compatibility is validated during resolution, but invalid batches
  are not yet rejected atomically before append
- `InsertAfter` is anchored and replay-stable for `SequenceRGA`

Primary evidence:

- [crates/aether_ast/src/lib.rs](../crates/aether_ast/src/lib.rs)
- [crates/aether_resolver/src/lib.rs](../crates/aether_resolver/src/lib.rs)

Normalization note:

- v1 closure does not imply post-v1 counter/lattice classes.

### `6. Provenance model`

**Status:** Kernel provenance is complete for the current slice; durable
service-level proof identity is reopened.

Implemented:

- datom provenance fields on the core datom type
- source datom IDs on resolved facts, projected sidecar facts, and derived tuples
- derived tuple metadata: rule, predicate, stratum, SCC, iteration, parent tuple IDs
- explain traces over derived tuples

Primary evidence:

- [crates/aether_ast/src/lib.rs](../crates/aether_ast/src/lib.rs)
- [crates/aether_runtime/src/lib.rs](../crates/aether_runtime/src/lib.rs)
- [crates/aether_explain/src/lib.rs](../crates/aether_explain/src/lib.rs)

### `7. Temporal model`

**Status:** Complete for unrestricted replay; reopened for policy-scoped replay.

Implemented:

- `History`
- `Current`
- `AsOf(element_id)`
- deterministic replay under fixed journal prefix

Primary evidence:

- [crates/aether_storage/src/lib.rs](../crates/aether_storage/src/lib.rs)
- [crates/aether_resolver/src/lib.rs](../crates/aether_resolver/src/lib.rs)
- [crates/aether_api/tests/semantic_closure.rs](../crates/aether_api/tests/semantic_closure.rs)

### `8. Query and phase model`

**Status:** Complete unrestricted; reopened for policy-scoped program/query
execution.

Implemented:

- whole-document DSL parsing
- compiled plans with phase graphs and SCC metadata
- query execution over extensional and derived relations
- named query and explain sections

Primary evidence:

- [crates/aether_rules/src/parser.rs](../crates/aether_rules/src/parser.rs)
- [crates/aether_plan/src/lib.rs](../crates/aether_plan/src/lib.rs)
- [crates/aether_api/src/lib.rs](../crates/aether_api/src/lib.rs)

### `9. Rule model`

**Status:** Complete for the unrestricted v1 slice; reopened for policy-scoped
recursion, negation, and aggregation.

Implemented:

- extensional and intensional predicates
- safety and type validation
- dependency graph construction and SCC computation
- stratified negation
- semi-naive delta execution
- bounded aggregation within the current non-recursive grouped slice, including
  multiple aggregate terms per head

Primary evidence:

- [crates/aether_rules/src/lib.rs](../crates/aether_rules/src/lib.rs)
- [crates/aether_runtime/src/lib.rs](../crates/aether_runtime/src/lib.rs)
- [crates/aether_api/tests/semantic_closure.rs](../crates/aether_api/tests/semantic_closure.rs)

Normalization note:

- v1 closure freezes aggregation at the current non-recursive grouped slice.
- recursive aggregates and richer aggregate ergonomics remain post-v1 work.

### `10. Coordination model`

**Status:** Complete for the unrestricted pilot slice; reopened for
policy-scoped coordination and reporting.

Implemented:

- tasks
- claims and releases
- leases, renewals, and expiry semantics
- heartbeats
- stale fencing and execution outcomes

Primary evidence:

- [crates/aether_api/src/pilot.rs](../crates/aether_api/src/pilot.rs)
- [crates/aether_api/src/report.rs](../crates/aether_api/src/report.rs)
- [crates/aether_api/tests/pilot_contract.rs](../crates/aether_api/tests/pilot_contract.rs)
- [crates/aether_api/tests/semantic_closure.rs](../crates/aether_api/tests/semantic_closure.rs)

Normalization note:

- expiry is semantic-state-driven in v1; it is not yet clock-driven distributed
  failure detection.

### `11. Sidecar model`

**Status:** Complete unrestricted for the single-node slice; reopened for
policy-scoped sidecar projection and dependency closure.

Implemented:

- artifact references outside the inline journal payload
- vector metadata outside the inline journal payload
- journal-subordinated sidecar registration and `AsOf` visibility
- provenance-bearing semantic projection back into the rule layer
- policy-related sidecar fetch and search filtering, pending R1
  noninterference proof across anchors, projection, derivation, and metadata

Primary evidence:

- [crates/aether_api/src/sidecar.rs](../crates/aether_api/src/sidecar.rs)
- [crates/aether_api/tests/sidecar_federation.rs](../crates/aether_api/tests/sidecar_federation.rs)
- [crates/aether_api/tests/semantic_closure.rs](../crates/aether_api/tests/semantic_closure.rs)

Normalization note:

- single-node durability is complete on the SQLite-backed pilot path.
- replication, failover, and distributed sidecar control remain out of scope for
  this closure pass.

## Acceptance Evidence

The current acceptance path for the v1 semantic thesis is:

- [crates/aether_api/tests/semantic_closure.rs](../crates/aether_api/tests/semantic_closure.rs)
- [crates/aether_resolver/src/lib.rs](../crates/aether_resolver/src/lib.rs) unit tests
- [crates/aether_api/tests/sidecar_federation.rs](../crates/aether_api/tests/sidecar_federation.rs)
- [crates/aether_api/tests/http_service.rs](../crates/aether_api/tests/http_service.rs)
- [crates/aether_api/tests/pilot_contract.rs](../crates/aether_api/tests/pilot_contract.rs)

Together they cover:

- journal append
- `History`
- `Current`
- `AsOf`
- literal v1 operation semantics
- recursive closure
- stratified negation
- bounded aggregation
- policy-related derivation coverage that does not yet prove pre-evaluation
  projection or noninterference
- coordination fencing
- sidecar projection
- explanation

## Non-Claims

This matrix does **not** claim:

- distributed consensus closure
- multi-tenant service closure
- post-v1 DSL ergonomics
- replicated sidecar backends
- production platform completeness

Those remain important, but they are outside the unrestricted kernel slice.
Policy-scoped replay, proof identity, and append admission are inside the
reopened correctness boundary and must not be described as completed closure.
