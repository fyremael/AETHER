# AETHER Semantic Kernel

[![CI](https://github.com/fyremael/AETHER/actions/workflows/ci.yml/badge.svg)](https://github.com/fyremael/AETHER/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-pages-0f766e)](https://fyremael.github.io/AETHER/)
[![Rust 1.78+](https://img.shields.io/badge/rust-1.78%2B-93450a?logo=rust)](https://www.rust-lang.org/)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-4b5563)](./LICENSE-MIT)

AETHER is a semantic coordination runtime for distributed agent systems.

Its claim is simple, but not modest: the right center of gravity for agent coordination is not a queue, not a graph-walking convenience layer, not a pile of ad hoc service contracts, and not a host-language DSL masquerading as semantics. The right center of gravity is an authoritative semantic kernel: an append-only causal journal, a deterministic resolver, and a recursive rule engine that can state, derive, replay, and explain what a system believes.

This repository is the beginning of that kernel.

It is a Rust-first implementation workspace built from a spec-first package. The specification still matters. The prose still governs. But this is no longer only a bundle of architectural intent. The repository now contains a real Rust workspace, real crate boundaries, real tests, and the first end-to-end recursive runtime slice.

The documentation surface now has two layers: the source-controlled handbook in this repository and an automated GitHub Pages site that publishes the Rust API reference alongside a curated documentation portal.

If you are joining the project fresh, read [`docs/README.md`](./docs/README.md) and [`CONTRIBUTING.md`](./CONTRIBUTING.md) after this file and then walk through [`examples/demo-03-coordination-situation-room.md`](./examples/demo-03-coordination-situation-room.md). That is the fastest path from architectural stance to executable behavior.

## Thesis

AETHER is built around two internal centers.

The first is the authoritative semantic substrate:

- append-only datoms
- causal element identifiers
- temporal replay
- attribute-class-aware resolution
- provenance-bearing facts
- policy-aware semantic state

The second is the recursive semantic closure:

- predicates
- rules
- safety validation
- dependency-graph construction
- SCC-aware planning
- fixed-point evaluation
- explainable derived tuples

The combination matters. A coordination system that stores facts without recursive closure becomes a passive ledger. A rule system without a temporal semantic substrate becomes clever but forgetful. AETHER is meant to keep both halves intact.

## What AETHER Is

AETHER is:

- a semantic substrate for distributed coordination facts
- a Datalog-native recursive derivation core
- a temporal replay engine for deterministic `AsOf` views
- a provenance-carrying kernel for explainable results
- a Rust workspace with clear crate ownership boundaries
- a foundation for future Go operational tooling and Python research tooling

AETHER is not:

- merely a Datalog engine
- merely a database
- merely an orchestration shell
- a Janus fork with recursive features bolted on later
- a Python-first or Go-first semantic implementation

## Design Position

The repository takes a deliberately opinionated stance.

- Rust is the mainline implementation language for the semantic core.
- The AETHER DSL is the canonical semantics surface, even before the parser is complete.
- Go is an operational shell and service-wrapper language, not the semantic authority.
- Python is a research and experimentation layer, not a shadow kernel.
- Sidecars for artifacts, vectors, and streams remain subordinate to the semantic kernel.

This posture is not aesthetic. It is structural. If the semantic center of gravity drifts into deployment code, scripting glue, or host-language convenience APIs, the system stops being a kernel and becomes a bag of integrations.

## Current State

The workspace has moved beyond scaffolding.

Implemented today:

- foundational identifiers, values, datoms, rule/query ASTs, and provenance types
- schema registration and predicate arity validation
- append-only journal semantics across both in-memory and SQLite-backed durable storage
- deterministic `Current` and `AsOf` resolution across scalar, set, and sequence classes
- a whole-document DSL parser for `schema`, `predicates`, `facts`, `rules`, `materialize`, and `query` sections
- rule safety checks
- dependency-graph construction
- SCC decomposition and phase-graph lowering
- unstratified-negation rejection
- predicate-stratum computation for executable stratified negation
- semi-naive delta execution across recursive SCCs
- a first real recursive runtime slice for positive recursion and cross-stratum negation
- source datom provenance threaded from resolved facts into derived tuples
- derived tuple metadata with rule, SCC, stratum, iteration, parent tuple references, and source datom IDs
- an in-memory explainer that reconstructs recursive tuple traces
- a coordination acceptance slice for task readiness, claims, lease handoff, and stale-attempt rejection
- an in-memory kernel service over `aether_api` with end-to-end integration tests
- a minimal HTTP JSON kernel service boundary over `aether_api`
- a durable coordination-pilot HTTP service example over a SQLite journal
- bearer-token authentication and endpoint-scope enforcement on the pilot HTTP path
- auditable pilot request logging with semantic cut, query, tuple, and count context plus persisted JSONL output
- operator-grade coordination pilot report generation in markdown and JSON
- a release-mode performance report, Criterion benchmark suite, and ignored stress workloads for early regression tracking
- a live console performance dashboard for real-time and collected instrument views
- machine-readable performance baseline capture and point-in-time drift reporting for the pilot path

Deliberately still narrow:

- the DSL parser is still a focused initial slice rather than the full canonical language
- bounded aggregation is not implemented yet
- Go and Python remain boundary placeholders rather than active implementations
- sidecar integrations are specified, not yet implemented

## First Working Vertical Slice

The first meaningful semantic loop is now alive in the repo.

That slice looks like this:

1. extensional facts are written as datoms
2. the resolver materializes current state or a prefix-constrained historical state
3. the compiler validates rules, builds dependency structure, and records executable metadata
4. the runtime lifts extensional relations from resolved attributes
5. recursive rules are evaluated with semi-naive delta execution inside SCCs and stratified negation across strata
6. derived tuples are emitted with iteration, parent-tuple, and source-datom provenance metadata
7. the explainer can reconstruct a tuple-local proof trace from the derived graph

That smallest initial proof has now widened into a first coordination workload: tasks, active claims, lease state, readiness, and stale-attempt rejection can all be derived and queried from the same kernel.

## Semantic Invariants

Several invariants govern the project from the start.

- For a fixed schema, journal prefix, and compiled program, results must be deterministic.
- The Rust kernel is the authoritative semantic implementation.
- Derived tuples must be explainable.
- Temporal replay is not a debugging convenience; it is part of the semantic model.
- Non-Rust layers may consume results, but they must not silently redefine them.

## Repository Shape

The repository follows the crate boundaries declared in `REPO_LAYOUT.md`.

### Workspace root

- `Cargo.toml` defines the Rust workspace.
- `Cargo.lock` is checked in.
- spec and interface documents remain at the repository root because they still govern implementation direction.

### Rust crates

| Crate | Responsibility |
| --- | --- |
| `aether_ast` | IDs, values, datoms, rule/query ASTs, provenance, phase/explain structs |
| `aether_schema` | attribute classes, schema registry, predicate signatures, validation |
| `aether_storage` | journal trait, in-memory journal, history and prefix access |
| `aether_resolver` | deterministic `Current` and `AsOf` materialization |
| `aether_rules` | DSL parsing, rule validation, dependency graphs, SCC analysis, extensional binding inference |
| `aether_plan` | compiled-program planning structures, phase graphs, delta-plan metadata |
| `aether_runtime` | semi-naive recursive evaluation, stratified negation, iteration metadata, derived tuple production |
| `aether_explain` | derivation and plan explanation surface |
| `aether_api` | request/response boundary types, an in-memory kernel service, and a minimal HTTP JSON boundary |

### Non-Rust boundaries

- `go/` is reserved for operator tooling, service wrappers, and deployment ergonomics.
- `python/` is reserved for fixture generation, benchmarks, notebooks, and research workflows.
- `docs/` holds status, roadmap, ADR space, and known limitations.
- `examples/`, `fixtures/`, and `scripts/` exist to support real implementation work rather than theory alone.

## How To Read This Repository

If you are new to AETHER, the most useful reading order is:

1. `SPEC.md` for the system thesis and milestone structure
2. `RULES.md` for the recursive semantics stance
3. `INTERFACES.md` for crate ownership and trait boundaries
4. `REPO_LAYOUT.md` for structural expectations
5. the Rust crates themselves, starting with `aether_ast` and moving upward toward `aether_runtime`

If you want to understand what is already implemented rather than what is only specified, start in the code:

1. `crates/aether_storage`
2. `crates/aether_resolver`
3. `crates/aether_rules`
4. `crates/aether_runtime`

## Building And Verifying

The current development baseline is Rust on both Windows MSVC and WSL Ubuntu.

Recommended commands:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test
```

WSL verification uses the same workspace and the same commands via the Linux toolchain.

For performance tracking:

```bash
cargo run -p aether_api --example performance_dashboard --release
cargo run -p aether_api --example performance_report --release
cargo run -p aether_api --example capture_performance_baseline --release
cargo run -p aether_api --example performance_drift_report --release -- artifacts/performance/baseline.json
cargo bench -p aether_api
cargo test -p aether_api --test performance_stress --release -- --ignored --nocapture
```

The workspace is currently verified under:

- Windows stable MSVC toolchain
- WSL Ubuntu stable GNU toolchain

GitHub Actions now runs the same format, lint, and test gates on both Ubuntu and Windows so the public CI badge reflects the actual contributor contract.

## Development Posture

The intended implementation sequence is disciplined.

- Make the substrate correct before making it distributed.
- Make the rule engine semantically credible before making it feature-rich.
- Keep crate boundaries explicit until there is compelling evidence to collapse them.
- Add Go and Python only across a stable kernel boundary.
- Prefer deterministic, explainable behavior over cleverness that obscures semantics.

This matters because coordination systems age badly when their core semantics are implicit. AETHER is meant to age in the opposite direction: toward greater clarity, stronger replay guarantees, and more legible derivation.

## What The Runtime Does Today

The runtime crate now performs a genuine recursive evaluation for a narrow but important class of programs.

Supported today:

- positive rule bodies
- recursive intensional predicates
- stratified negation across strata
- extensional predicates lifted from resolved attributes
- extensional facts authored directly in the DSL
- derived tuple de-duplication
- iteration-by-iteration convergence tracking
- parent derived tuple linkage
- source datom provenance on derived tuples
- recursive tuple trace reconstruction
- query execution over `Current` and `AsOf` views
- coordination-style readiness and stale-attempt derivations

Not yet supported in the runtime:

- bounded aggregation
- optimizer-grade plan selection beyond the current semi-naive slice

This is intentional. The project is building from semantic bedrock upward. The right next steps are to preserve correctness while widening expressive power, not to rush into breadth and backfill meaning later.

## Roadmap

The milestone sequence remains the governing roadmap.

- `M0` Rust substrate core
- `M1` deterministic resolver core
- `M2` rule compiler and planning
- `M3` recursive runtime and derivation traces
- `M4` stable API boundary
- `M5` Go shell and Python SDK

In practical terms, the most immediate work now is:

- widening the DSL from the current focused slice to the full canonical language
- adding bounded aggregation and deeper runtime optimization
- widening explainability from tuple traces to richer operator-facing proof and incident surfaces
- hardening the API boundary from the current authenticated, audited, and reportable pilot service to richer operator-facing and production-credible integrations
- introducing more boundary-level examples and operator-facing demonstrations

## Why The README Is Long

This repository began as a specification package for implementation agents and human collaborators working from the same architectural text. In a repo like that, a short README would be a false kindness. It would save a few seconds at the top and cost hours everywhere else.

The job of this README is not to decorate the repository. Its job is to establish the center of gravity quickly and correctly:

- what AETHER is
- what it is trying not to become
- what already exists in code
- what remains deliberately deferred
- where each responsibility belongs

That clarity is part of the implementation.

## Related Documents

- `docs/README.md` is the documentation portal and reading-map entry point.
- `docs/ARCHITECTURE.md` is the current implementation architecture guide.
- `docs/DEVELOPER_WORKFLOW.md` explains the engineering loop and definition of done.
- `docs/OPERATIONS.md` explains the operator path, demo catalog, and report workflow.
- `docs/PERFORMANCE.md` explains the benchmark harness, stress tests, and performance-report path.
- `docs/GLOSSARY.md` defines canonical project vocabulary.
- `docs/DOCUMENTATION_STANDARD.md` defines the documentation quality bar and update rules.
- `SPEC.md` defines the system objective, architecture, data model, temporal model, and milestones.
- `RULES.md` defines the rule-language stance and recursive semantics expectations.
- `INTERFACES.md` defines crate responsibilities and trait-shape guidance.
- `IMPLEMENTATION_DECISION.md` records the fork-versus-own and language-split decisions.
- `REPO_LAYOUT.md` defines the required repository structure.
- `TESTPLAN.md` captures test intent and verification direction.
- `CONTRIBUTING.md` explains contributor expectations and the local verification contract.
- `examples/transitive-closure.md` walks through the first working recursive example.
- `docs/STATUS.md` tracks implementation status.
- `docs/ROADMAP.md` and `docs/KNOWN_LIMITATIONS.md` hold forward-looking operational documentation.

## Closing Position

AETHER is trying to do something exacting.

It is trying to give distributed agent systems a semantic core that is explicit, replayable, recursive, inspectable, and implementable without surrendering the center to shell code or fashionable abstraction. That is a narrow road. It asks for discipline at the language boundary, rigor in the data model, and honesty about what is implemented versus what is merely intended.

This repository is the first serious step onto that road.
