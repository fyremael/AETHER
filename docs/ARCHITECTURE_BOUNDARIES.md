# Rust Responsibility Boundaries

Status: R6 implementation contract

The Rust workspace remains the repository center. `aether_api` is now a
temporary compatibility facade, not an implementation catch-all. New code must
target the owning crate directly.

| Crate | Owns | May depend inward on |
| --- | --- | --- |
| `aether_ast` | canonical semantic data and DSL AST types | foundational libraries only |
| `aether_schema` | schema and type contracts | AST |
| `aether_storage` | journals, transactional append substrate, transport | AST |
| `aether_resolver` | deterministic replay and dependency certification | AST, schema |
| `aether_plan` | versioned executable SCC/stratum/rule plan | AST |
| `aether_rules` | DSL parsing, safety, stratification, compilation into `aether_plan` | AST, schema, plan |
| `aether_runtime` | execution of the supplied executable plan | AST, resolver, plan |
| `aether_explain` | derivation and plan explanation | AST, plan, runtime |
| `aether_sidecar` | artifact/vector contracts and local catalog | AST |
| `aether_service_core` | policy-scoped orchestration, schema/admission, receipts, execution stores, service traits | kernel and sidecar crates |
| `aether_pilot` | coordination DSL, reports, deltas, and product proof fixtures | service core |
| `aether_partition` | authority partitions, federation, imports, replication prototype | service core and kernel crates |
| `aether_http` | HTTP, auth, audit, namespace routing, deployment/status boundary | service, pilot, partition |
| `aether_perf` | benchmarks, drift, trends, host facts, capacity planning | measured crates |
| `aether_api` | compatibility re-exports, examples, binary, integration tests | all public surfaces during migration |

The forbidden direction is from any owning crate back to `aether_api`.
`aether_perf` is a leaf measurement crate; production crates must not depend on
it. `aether_http` cannot become a semantic compiler or runtime.

## Executable plan boundary

`aether_plan` publishes `aether-executable-plan-v1`. A compiled program contains:

- a topologically ordered SCC schedule with explicit strata
- extensional predicate bindings
- per-rule semi-naive delta anchors
- aggregate plan nodes
- complete provenance requirements
- a plan format version

`aether_runtime` consumes those records and rejects an unknown version, missing
rule/SCC entry, schedule mismatch, or malformed aggregate node. It no longer
reconstructs SCC order or recursive anchor selection from rule ASTs.

## Compatibility and measurements

At pre-R6 commit `0721e6a`, files under `crates/aether_api/src` totaled 783,307
bytes. After extraction the facade plus pilot binary total 25,460 bytes, a
96.7% reduction in catch-all source ownership. The implementation is now
independently testable as 21 HTTP, 12 partition, 10 performance, 5 sidecar, and
2 pilot unit tests, in addition to service-core and workspace suites.

On the 2026-07-12 Windows development host, after package-local Cargo cleans,
`cargo check -p aether_service_core` completed in 2.62 seconds and the
compatibility `cargo check -p aether_api` completed in 18.34 seconds. These are
local diagnostic timings, not performance claims; cold shared-dependency
compilation remains host/cache dependent. Their purpose is to establish that
the semantic service core has an isolated check target.

`python/tests/test_architecture_boundaries.py` enforces dependency direction,
facade-only ownership, and the executable-plan/runtime markers. Existing API,
policy, append, proof, HTTP, partition, pilot, and performance tests remain the
semantic compatibility proof.
