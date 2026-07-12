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
  policy-scoped semantics, execution-scoped proof identity, and sidecar
  subordination, with immutable evidence tooling implemented locally and the
  official exact-candidate qualification run still open
- no implied claim of distributed runtime completion, multitenancy, or
  production platform completeness

Where the spec is broader than the current v1 interpretation, that narrowing is
called out explicitly instead of hidden.

## Summary

| SPEC section | Status | Evidence | v1 interpretation |
| --- | --- | --- | --- |
| `1. Objective` | Policy repair and immutable evidence verifier implemented locally | `crates/aether_resolver/tests/policy_scoped_replay.rs`, `crates/aether_runtime/tests/policy_scoped_execution.rs`, `crates/aether_api/tests/policy_noninterference.rs`, `python/tests/test_release_evidence.py` | Scoped semantics and fail-closed candidate evidence pass locally; official workflow/download verification plus R5 subjects remain open |
| `2. Implementation language strategy` | Complete | Rust kernel crates, Go client/shell, Python SDK, CI in `.github/workflows/ci.yml` | Rust remains authoritative; Go/Python are boundary layers |
| `3. Architectural thesis` | Complete | `docs/ARCHITECTURE.md`, `crates/aether_runtime`, `crates/aether_api/src/sidecar.rs` | Two-center kernel thesis is implemented locally |
| `4. Design stance on Janus` | Complete | Repo layout and implementation shape | Janus remains reference-only, not a compatibility target |
| `5. Core data model` | Transactional append admission implemented locally | `crates/aether_ast`, `crates/aether_resolver`, `crates/aether_api/tests/append_admission.rs`, storage race tests | Namespace schema, recursive type, operation, provenance, dependency, cut, and idempotency checks precede atomic append/receipt commit; immutable candidate qualification remains R4 |
| `6. Provenance model` | Execution-scoped proof identity implemented locally | `crates/aether_api/tests/execution_handles.rs`, execution-store unit tests, HTTP/federation tests | Kernel `TupleId` remains local; service proofs use authorization-checked opaque handles bound to immutable execution manifests and replay digests |
| `7. Temporal model` | Policy-scoped implementation complete locally | scoped resolver tests and API noninterference suite | Physical cut selection precedes policy projection; visible cuts and hidden/nonexistent errors are projection-local |
| `8. Query and phase model` | Policy-scoped implementation complete locally | `crates/aether_rules`, `crates/aether_plan`, `crates/aether_api/src/evaluation.rs` | Extensional facts are projected before compiler validation and planning; scoped query execution consumes one evaluation bundle |
| `9. Rule model` | Policy-scoped implementation complete locally | `crates/aether_runtime/tests/policy_scoped_execution.rs`, API projection-equivalence test | Recursion, negation, aggregates, tuple IDs, indexes, and iterations are computed inside the effective scope |
| `10. Coordination model` | Policy- and proof-scoped implementation complete locally | pilot/report tests, API noninterference suite, `execution_handles.rs` | Coordination documents and reports run from scoped snapshots and carry execution IDs plus durable trace handles |
| `11. Sidecar model` | Policy-scoped reads and dependency admission implemented locally | sidecar unit/federation tests, semantic closure suite, append admission tests | Sidecar cuts use projected journal catalogs, protected/absent reads share an opaque error, and new journal dependencies are admitted before commit; immutable candidate qualification remains R4 |

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
  explanation, with policy projected before replay, compilation, and execution

Primary evidence:

- [docs/ARCHITECTURE.md](./ARCHITECTURE.md)
- [crates/aether_runtime/src/lib.rs](../crates/aether_runtime/src/lib.rs)
- [crates/aether_api/src/lib.rs](../crates/aether_api/src/lib.rs)

### `4. Design stance on Janus`

**Status:** Complete.

The implementation remains Rust-first and spec-governed without preserving Janus
compatibility as a product constraint.

### `5. Core data model`

**Status:** Transactional namespace-schema append admission implemented locally;
immutable exact-candidate qualification remains R4.

Implemented:

- all v1 operation kinds are represented in the AST
- attribute classes drive deterministic resolver behavior
- op/class compatibility, recursive types, provenance, causal dependencies,
  active schema, expected cut, and idempotency are validated before atomic
  append and durable receipt creation
- `InsertAfter` is anchored and replay-stable for `SequenceRGA`
- existing prefixes receive immutable certified or quarantined baseline records

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

**Status:** Policy-scoped replay implemented locally; immutable release evidence pending.

Implemented:

- `History`
- `Current`
- `AsOf(element_id)`
- deterministic replay under fixed journal prefix
- cut-then-project scoped replay with visible-cut metadata
- deterministic policy-dependency certification for provenance, causal, and sequence anchors

Primary evidence:

- [crates/aether_storage/src/lib.rs](../crates/aether_storage/src/lib.rs)
- [crates/aether_resolver/src/lib.rs](../crates/aether_resolver/src/lib.rs)
- [crates/aether_api/tests/semantic_closure.rs](../crates/aether_api/tests/semantic_closure.rs)
- [crates/aether_api/tests/append_admission.rs](../crates/aether_api/tests/append_admission.rs)
- [crates/aether_resolver/tests/policy_scoped_replay.rs](../crates/aether_resolver/tests/policy_scoped_replay.rs)
- [crates/aether_api/tests/policy_noninterference.rs](../crates/aether_api/tests/policy_noninterference.rs)

### `8. Query and phase model`

**Status:** Policy-scoped program/query execution implemented locally;
exact-candidate evidence pending.

Implemented:

- whole-document DSL parsing
- compiled plans with phase graphs and SCC metadata
- query execution over extensional and derived relations
- named query and explain sections
- scoped fact projection before compiler validation and planning
- one typed snapshot/program/derived evaluation bundle per effective scope

Primary evidence:

- [crates/aether_rules/src/parser.rs](../crates/aether_rules/src/parser.rs)
- [crates/aether_plan/src/lib.rs](../crates/aether_plan/src/lib.rs)
- [crates/aether_api/src/lib.rs](../crates/aether_api/src/lib.rs)

### `9. Rule model`

**Status:** Policy-scoped recursion, negation, and aggregation implemented
locally; scheduled backend/performance evidence pending.

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
- [crates/aether_runtime/tests/policy_scoped_execution.rs](../crates/aether_runtime/tests/policy_scoped_execution.rs)
- [crates/aether_api/tests/policy_noninterference.rs](../crates/aether_api/tests/policy_noninterference.rs)

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

**Status:** Policy-scoped sidecar reads, projection, and append-time dependency
admission implemented locally; immutable exact-candidate qualification remains R4.

Implemented:

- artifact references outside the inline journal payload
- vector metadata outside the inline journal payload
- journal-subordinated sidecar registration and `AsOf` visibility
- provenance-bearing semantic projection back into the rule layer
- policy-scoped journal catalogs for search cuts and provenance source IDs
- hidden and nonexistent artifact reads share the same unknown-ID surface

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
- policy noninterference across replay, compilation, recursion, negation,
  aggregation, metadata, explanations, sidecars, and federation
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
Append admission remains inside the reopened correctness boundary. Policy and
proof-identity repairs are implemented locally but remain external non-claims
until immutable exact-candidate evidence and later operational gates pass.
