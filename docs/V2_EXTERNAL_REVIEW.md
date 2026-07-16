# AETHER V2 External Review

Date: 2026-07-04

## Scope

This review treats the current repository as a closed v1 semantic-kernel slice
plus growing post-v1 service, pilot, sidecar, performance, and product layers.
The purpose is to surface defects and reorganization pressure before v2 work
widens the system further.

Validation performed during this review:

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --all-targets`
- `go test ./...` from `go/`
- `python -m pytest -q python/tests`
- `git diff --check`

All validation commands completed successfully. The findings below are therefore
not basic build failures; they are semantic, architectural, and planning risks.

## Executive Assessment

The Rust mainline kernel exists and the v1 acceptance path is real: schema,
journal, resolver, parser/compiler, recursive semi-naive runtime, and
explanation surfaces are implemented and covered by tests.

The main risk is now boundary drift. `aether_api` has become the center of
service orchestration, HTTP, namespace handling, sidecars, replicated
partitions, pilot reports, deployment config, and performance/capacity tooling.
That was a practical way to finish v1 and pilot proof, but it is no longer the
right shape for v2.

Two issues should be treated as defects before broadening v2:

1. Policy contexts are applied too late for non-monotonic semantics.
2. Tuple explanation cache state is not tied to a journal cut, program, or
   policy context.

## Findings

### P0: Policy Contexts Are Presentation Filters, Not Semantic Input Cuts

Evidence:

- `crates/aether_api/src/lib.rs:423-428` resolves `current_state` over the full
  datom set and then filters the resolved state.
- `crates/aether_api/src/lib.rs:436-438` does the same for `AsOf`.
- `crates/aether_api/src/lib.rs:222-226` evaluates document views over the full
  resolved state.
- `crates/aether_api/src/lib.rs:591-594` filters response state/program/derived
  output after evaluation.
- `crates/aether_runtime/src/lib.rs:622-636` evaluates negative literals against
  all rows without policy-aware row filtering.

Impact:

- A hidden retract can erase a value that should remain visible to a caller
  without the hidden capability.
- A hidden fact can satisfy a negated predicate and suppress an otherwise
  visible derivation.
- This can leak protected information through absence of rows, especially in
  readiness, stale-rejection, denial, and policy-gated operational workflows.
- `docs/STATUS.md:52-54` currently overstates the strength of policy-context
  execution relative to this behavior.

Recommendation:

- Add failing regression tests for hidden retraction and hidden negation.
- Define policy-scoped replay as the semantic input to resolver/runtime, not as
  only a response filter.
- Either pass policy context into `MaterializedResolver` and relation building,
  or construct a visible journal prefix/program fact set before resolving and
  deriving.
- Treat policy context as part of any cache key for state, derivation, and
  explanation.

### P1: Tuple Explanation Cache Is Stale And Ambiguous

Evidence:

- `crates/aether_api/src/lib.rs:132-135` stores a single `last_derived` cache.
- `crates/aether_api/src/lib.rs:202-203` caches only the derived set.
- `crates/aether_api/src/lib.rs:406-411` appends new datoms without clearing or
  restamping the cache.
- `crates/aether_api/src/lib.rs:463-472` explains a tuple by `TupleId` against
  the last cached derivation only.

Impact:

- Appending after a document run leaves an explanation cache from an older cut.
- Running a different document overwrites the cache while tuple IDs restart from
  `t1`, so an old tuple ID can explain a different tuple from a newer program.
- The HTTP explanation endpoint can therefore return a trace not bound to the
  user's intended journal cut, DSL, program, or policy context.

Recommendation:

- Clear derived caches on append and other semantic mutations immediately.
- Replace tuple-only explanation requests with a trace handle containing at
  least namespace, journal cut, program/document hash, policy context, and tuple
  ID.
- Consider an explicit `run_document_with_explain` path that computes the trace
  in the same evaluation instead of relying on process-local mutable cache.

### P2: `aether_api` Is Now A Service And Product Catch-All

Evidence:

- `aether_api` contains about 23.9k Rust lines, while the other semantic crates
  are much smaller.
- `crates/aether_api/src/lib.rs:17-26` exposes deployment, HTTP, namespace,
  partitioning, performance, pilot, report, sidecar, and status modules.
- `crates/aether_api/Cargo.toml:17-25` pulls normal runtime dependencies for
  HTTP, SQLite, sysinfo, Tokio, and Tower.
- `crates/aether_api/src/perf.rs` is 4k+ lines and is compiled as a hidden module
  of the API crate.
- `crates/aether_api/src/partitioned.rs` is 2.9k+ lines and carries federated
  imports, replicated partitions, cache behavior, reports, and tests in one
  module.

Impact:

- Service/API concerns are now close enough to semantic orchestration that v2
  changes can accidentally make the API crate the effective kernel.
- Compile-time and dependency weight for the API crate grows with unrelated
  performance and product tooling.
- Review and ownership boundaries are too coarse for distributed truth,
  sidecars, and HTTP hardening to advance independently.

Recommendation:

- Keep Rust as the workspace root and authoritative kernel.
- Split outward from the kernel, not away from it:
  - `aether_service_core`: journal-backed document execution, policy-scoped
    replay, trace handles, and service traits without Axum.
  - `aether_http`: HTTP/auth/audit/namespace routing over service traits.
  - `aether_sidecar`: artifact/vector sidecar contracts and SQLite catalog.
  - `aether_partition`: authority partitions, federated cuts, imports,
    replication prototype.
  - `aether_perf`: benchmarks, reports, capacity planning, host manifests.
  - `aether_pilot`: coordination pilot DSL, reports, and demos.
- Preserve current public re-exports temporarily if needed, but make new v2
  work target the narrower crates.

### P2: `aether_plan` Is Metadata, Not A Planning Boundary

Evidence:

- `crates/aether_plan/src/lib.rs:5-46` contains data structs only.
- `crates/aether_rules/src/lib.rs:23-104` performs compilation, SCC building,
  stratification, extensional binding, and delta-plan construction.
- `crates/aether_runtime/src/lib.rs:96-310` recomputes SCC evaluation order and
  rule anchoring from `CompiledProgram`; `delta_plans` are not used by runtime.

Impact:

- The v2 compiler/runtime contract is underspecified.
- Optimizing semi-naive execution, adding richer explain metadata, or widening
  language features will require coordinated edits across parser, compiler,
  plan metadata, and runtime internals.

Recommendation:

- Make `aether_plan` own the executable logical plan:
  - SCC schedule.
  - stratum schedule.
  - extensional relation bindings.
  - delta-anchor strategy.
  - aggregate plan nodes.
  - provenance requirements.
- Keep parser AST in `aether_ast`/`aether_rules`, but make runtime consume only
  a stable plan representation.

### P2: DSL Binding And Parser Ergonomics Are Too Brittle For V2

Evidence:

- `crates/aether_rules/src/parser.rs:75-130` uses a line-oriented section
  collector.
- `crates/aether_rules/src/parser.rs:666-699` parses calls by ad hoc delimiter
  splitting.
- `crates/aether_rules/src/parser.rs:1045-1085` reports line-only errors without
  spans.
- `crates/aether_rules/src/lib.rs:704-739` infers extensional bindings from
  predicate names by converting underscores to dots.

Impact:

- Richer module imports, reusable rule libraries, aliases, and stronger editor
  diagnostics will be difficult to add safely.
- Name-inferred extensional bindings can become ambiguous as schemas grow.

Recommendation:

- Add explicit DSL syntax for extensional relation binding.
- Introduce source spans in AST and error types before adding modular DSL
  features.
- Keep the current parser as the v1 compatibility parser if a generated or
  tokenized parser is introduced for v2.

### P2: HTTP Namespace Execution Is Globally Serialized

Evidence:

- `crates/aether_api/src/http.rs:32-39` stores all namespace services behind one
  `Arc<Mutex<NamespaceServiceStore>>`.
- `crates/aether_api/src/http.rs:240-271` executes each service operation while
  holding that store lock. The Postgres path uses `std::thread::spawn` and then
  immediately `join`s the worker.
- `crates/aether_api/src/http.rs:881-895` opens or retrieves the namespace
  service and runs the operation inside the same store method.

Impact:

- A long-running document run, report, or sidecar search can block unrelated
  requests, including other namespaces.
- The Postgres path still blocks the async handler while waiting for the joined
  worker.

Recommendation:

- Move to per-namespace service handles and narrower locks.
- Use `tokio::task::spawn_blocking` or an async storage/service boundary where
  blocking storage is unavoidable.
- Keep exact replay semantics, but remove the global service-store bottleneck
  before treating Service v2 as a concurrent design-partner boundary.

### P3: Journal Interfaces Are Prefix-Oriented And Clone-Heavy

Evidence:

- `crates/aether_storage/src/lib.rs:13-16` defines `history` and `prefix` as
  `Vec<Datom>` return values.
- `crates/aether_storage/src/lib.rs:53-64` clones in-memory history and prefixes.
- `crates/aether_storage/src/lib.rs:235-264` reads full Postgres histories or
  prefixes into memory.
- `crates/aether_api/src/lib.rs:183-188` copies request datoms or full journal
  history into a vector.

Impact:

- This is acceptable for the current single-node evidence envelope, but it will
  put pressure on v2 imports, long-lived journals, and report generation.

Recommendation:

- Add cut descriptors, cursor/iterator APIs, and materialized snapshots without
  changing the append-only journal authority.
- Keep the current vector API as a simple compatibility layer.

## Suggested V2 Reorganization Sequence

1. Fix P0/P1 semantic defects before broadening interfaces.
2. Introduce `aether_service_core` and move `KernelServiceCore`, document
   execution, policy-scoped replay, and trace-handle semantics there.
3. Move HTTP/auth/audit into `aether_http` over service traits.
4. Move sidecars and partition/federation into separate crates with explicit
   contracts against service core.
5. Move performance/capacity tooling out of normal API runtime dependencies.
6. Promote `aether_plan` from metadata structs to the executable compiler/runtime
   contract.
7. Add explicit DSL extensional bindings and source spans before v2 language
   modularity.

## Decision Boundaries For V2

- Do not let Service v2 redefine kernel semantics.
- Do not make Postgres a rule engine or derived-state authority.
- Do not widen distributed truth until policy-scoped replay and trace handles
  are exact.
- Do not mature Go/Python beyond client layers until Rust service-core contracts
  are stable.
- Keep product/demo crates subordinate to the semantic kernel and evidence
  artifacts.
