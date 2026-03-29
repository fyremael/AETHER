# AETHER Semantic Kernel

[![CI](https://github.com/fyremael/AETHER/actions/workflows/ci.yml/badge.svg)](https://github.com/fyremael/AETHER/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-pages-0f766e)](https://fyremael.github.io/AETHER/)
[![Rust 1.78+](https://img.shields.io/badge/rust-1.78%2B-93450a?logo=rust)](https://www.rust-lang.org/)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-4b5563)](./LICENSE-MIT)

AETHER is a full-v1 single-node semantic kernel with a launch-ready
design-partner pilot for agentic coordination.

The easiest way to understand it is as the fabric underneath a governed shared
workspace for agents and operators. Observations, candidate actions,
authorizations, handoffs, and stale attempts all live in one replayable
history, so the system can answer five practical questions: what is active now,
which action is actually ready, who may act, what changed since the last
handoff, and why the answer is true.

If you want the fastest product-facing path first, start with
[`docs/COMMERCIALIZATION/GOVERNED_INCIDENT_BLACKBOARD.md`](./docs/COMMERCIALIZATION/GOVERNED_INCIDENT_BLACKBOARD.md)
and then walk through
[`examples/demo-04-governed-incident-blackboard.md`](./examples/demo-04-governed-incident-blackboard.md).

Its long-range claim is simple, but not modest: the right center of gravity for agent coordination is not a queue, not a graph-walking convenience layer, not a pile of ad hoc service contracts, and not a host-language DSL masquerading as semantics. The right center of gravity is an authoritative semantic kernel: an append-only causal journal, a deterministic resolver, and a recursive rule engine that can state, derive, replay, and explain what a system believes.

This repository is the beginning of that kernel.

It is a Rust-first implementation workspace built from a spec-first package. The specification still matters. The prose still governs. But this is no longer only a bundle of architectural intent. The repository now contains a real Rust workspace, real crate boundaries, real tests, and the first end-to-end recursive runtime slice.

The documentation surface now has two layers: the source-controlled handbook in this repository and an automated GitHub Pages site that publishes the Rust API reference alongside a curated documentation portal.

That Pages surface now also includes a live showcase entrance for executive,
customer, and partner conversations, plus exportable social cards, slide
covers, and proof snapshots generated from the same presentation system.

If you are joining the project fresh, read [`docs/README.md`](./docs/README.md)
and [`CONTRIBUTING.md`](./CONTRIBUTING.md) after this file and then walk
through
[`examples/demo-04-governed-incident-blackboard.md`](./examples/demo-04-governed-incident-blackboard.md).
Follow with
[`examples/demo-03-coordination-situation-room.md`](./examples/demo-03-coordination-situation-room.md)
for the raw kernel-proof showcase. That is the fastest path from practical
utility to executable behavior.

If you need the exact claim we can defend against the governing thesis, read
[`docs/SEMANTIC_COMPLIANCE_MATRIX.md`](./docs/SEMANTIC_COMPLIANCE_MATRIX.md).
That document maps `SPEC.md` sections `1-11` to implementation and test
evidence for the current single-node semantic closure pass.

If you need the explicit release-position statement for that claim, read
[`docs/V1_CLOSEOUT.md`](./docs/V1_CLOSEOUT.md).

If you want the gentler, plain-language on-ramp first, start with
[`docs/EDUCATION/README.md`](./docs/EDUCATION/README.md). That set explains the
system with analogies, figures, and worked examples before it asks readers to
care about compiler stages or runtime strata.

If you want the interactive version of that onboarding path, start with
[`python/notebooks/README.md`](./python/notebooks/README.md).

If you are evaluating AETHER as a product category or design-partner platform,
start with [`docs/COMMERCIALIZATION/README.md`](./docs/COMMERCIALIZATION/README.md)
after this file. That pack explains the long-range vision, buyer story,
messaging discipline, and commercialization wedge while staying anchored to the
current pilot proof. It also now includes a reference-pattern note for
TupleSpace-style or semantic-blackboard coordination on top of AETHER, plus a
governed incident blackboard exemplar that packages that pattern in client
language,
a canonical use-case ladder from pilot proof to platform horizon, an executive
summary, a seed investor pitch for early market placement, and a technical
scaling plan for advisor and diligence review.

## Thesis

AETHER is built around two internal centers.

The first is the authoritative semantic substrate:

- append-only datoms
- causal element identifiers
- temporal replay
- attribute-class-aware resolution
- provenance-bearing facts
- policy-aware semantic visibility

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

- a semantic substrate for exact local coordination facts and explicit federated truth boundaries
- a single-node pilot kernel for exact local truth and replay
- a Datalog-native recursive derivation core
- a temporal replay engine for deterministic `AsOf` views
- a provenance-carrying kernel for explainable results
- a Rust workspace with clear crate ownership boundaries
- a live Go operator shell and TUI cockpit plus a typed Python boundary SDK over the stable HTTP seam

AETHER is not:

- merely a Datalog engine
- merely a database
- merely an orchestration shell
- a Janus fork with recursive features bolted on later
- a Python-first or Go-first semantic implementation

## How To Read This Repo Quickly

Three documents answer most first questions:

- [`docs/STATUS.md`](./docs/STATUS.md) says what exists today.
- [`docs/SEMANTIC_COMPLIANCE_MATRIX.md`](./docs/SEMANTIC_COMPLIANCE_MATRIX.md)
  says what part of the governing thesis is already closed.
- [`docs/KNOWN_LIMITATIONS.md`](./docs/KNOWN_LIMITATIONS.md) says where the
  current system still stops.

That trio is the clearest way to avoid confusing implemented kernel behavior
with later platform ambition.

## Commercial Frame

The shortest commercial description is:

**AETHER is the semantic coordination fabric for agentic operations.**

That means:

- operational memory with durable replay
- derived understanding of readiness and authority
- governed action through claims, lease heartbeats, handoff, outcome acceptance, and fencing
- proof-backed explanation for operators and auditors

The current design-partner pilot proves that story narrowly in the coordination
domain. The longer-range platform vision is documented in
[`docs/COMMERCIALIZATION/VISION.md`](./docs/COMMERCIALIZATION/VISION.md).
The distributed scale posture is now explicit too:
[`docs/COMMERCIALIZATION/DISTRIBUTED_TRUTH.md`](./docs/COMMERCIALIZATION/DISTRIBUTED_TRUTH.md).

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
- strict v1 operation/class validation across scalar, set, and sequence semantics
- anchored `InsertAfter` replay for `SequenceRGA`, with deterministic tie-breaking by committed element order
- a whole-document DSL parser for the current canonical v1 surface, including facts, repeated query sections, explain directives, temporal views, entity constants, and policy annotations
- rule safety checks
- dependency-graph construction
- SCC decomposition and phase-graph lowering
- unstratified-negation rejection
- predicate-stratum computation for executable stratified negation
- semi-naive delta execution across recursive SCCs
- bounded aggregation via non-recursive grouped head-term `count`, `sum`, `min`, and `max` rules, including multiple aggregate terms per head; this now closes the v1 bounded-aggregation requirement while leaving richer aggregate ergonomics as post-v1 work
- a first real recursive runtime slice for positive recursion and cross-stratum negation
- source datom provenance threaded from resolved facts into derived tuples
- conjunctive policy propagation from datoms and sidecar-projected facts into derived tuples, aggregates, explanations, and reports
- derived tuple metadata with rule, SCC, stratum, iteration, parent tuple references, and source datom IDs
- an in-memory explainer that reconstructs recursive tuple traces
- a coordination acceptance slice for task readiness, claims, lease heartbeats, execution outcomes, lease handoff, and stale-result rejection
- an in-memory kernel service over `aether_api` with end-to-end integration tests
- a minimal HTTP JSON kernel service boundary over `aether_api`
- a durable coordination-pilot HTTP service example over a SQLite journal
- bearer-token authentication, endpoint-scope enforcement, and token-bound semantic policy ceilings on the pilot HTTP path
- explicit policy-context filtering for datoms, DSL-authored extensional facts, and sidecar reads/searches, with request policy now allowed to narrow token-granted visibility but not widen it
- policy-matched explanation, visible-history filtering, and policy-aware coordination reports on the service/operator path
- a config-backed pilot service binary with secret-file/env/command token resolution, package-local rotation tooling, and packaged single-node deployment bundles
- a live service-status surface with explicit config/schema/service-mode identity plus config-backed auth reload for principal and revocation changes
- auditable pilot request logging with semantic cut, query, tuple, and count context plus persisted JSONL output
- operator-grade coordination pilot report generation in markdown and JSON
- coordination delta report generation in markdown and JSON for “what changed between cuts?” operator workflows
- a release-mode performance report, Criterion benchmark suite, durable restart/replay benchmarks, and ignored stress workloads for early regression tracking
- a live console performance dashboard for real-time and collected instrument views
- machine-readable performance baseline capture and point-in-time drift reporting for the pilot path
- a host-aware benchmark matrix with tracked host manifests, suite-specific accepted baselines, run bundles, and comparative matrix summaries across Windows, WSL, and GitHub runner surfaces
- a one-command pilot launch validation pack with soak, stress, and artifact capture
- a journal-anchored artifact/vector sidecar federation boundary with external artifact references, SQLite-backed durability for the pilot service, HTTP endpoints, and provenance-bearing semantic fact projection
- a required mainline CI launch/drift gate plus a packaged pilot-service artifact build on Windows
- a scheduled/manual GitHub Actions pilot-validation workflow that runs the launch pack and uploads report/drift artifacts
- a first real Go operator shell, pilot-focused TUI cockpit, and typed Go client over the HTTP API
- a broader typed Python SDK surface with fixture builders, policy-aware helpers, and live integration coverage against a Rust server
- a tracked semantic compliance matrix that maps `SPEC.md` sections `1-11` to the implemented v1 single-node semantic surface
- explicit partition IDs, partition-qualified cuts, and federated-cut types for the first distributed-truth implementation slice
- a single-process partition-aware in-memory service for exact per-partition append/history/state reads and honest federated-history reads
- imported-fact federation over explicit partition cuts, with source partition/cut provenance carried into derived tuples
- federated document execution, explain traces, and markdown reports over partition-local truth without inventing a fake global clock
- a SQLite-backed partition-aware service for durable per-partition replay plus restart-safe imported-fact and federated explain/report execution
- a single-host leader/follower replicated authority-partition prototype with manual promotion, leader epochs, stale-epoch fencing, follower replay, federated HTTP routes, and exact-response reuse for repeated federated run/report polling

Deliberately still narrow:

- the DSL now covers the canonical v1 surface, but broader post-v1 ergonomics and modular authoring are still open
- bounded aggregation is intentionally limited to non-recursive grouped aggregate rules, so richer aggregate syntax remains future work even though the v1 bounded-aggregation requirement is now covered
- the Go shell and Python SDK are now real, but they are still early boundary clients rather than mature multi-platform ecosystems
- sidecar federation is now journal-subordinated and temporally exact on the SQLite-backed pilot path, but it is not yet replicated, distributed, or policy-enforced end to end
- the first partition-aware service slice now includes imported-fact reasoning, federated explain/report surfaces, a SQLite-backed durable backend, and a single-host replicated authority-partition prototype; generalized multi-host consensus and failover remain future work
- imported-fact federation is currently constrained to single-goal tuple-producing query shapes so imported provenance stays semantically exact instead of pretending to justify arbitrary joined rows

Within that deliberately narrow bar, the current repository can honestly claim
**full v1 single-node semantic closure**. The remaining gaps are broader
platform, operability, and post-v1 ergonomics work, not missing core local
semantics.

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

That smallest initial proof has now widened into a first coordination workload: tasks, active claims, lease state, heartbeat-backed authority, accepted outcomes, rejected stale outcomes, and readiness can all be derived and queried from the same kernel.

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
python -m unittest discover python/tests -v
(cd go && go test ./...)
cargo build -p aether_api --bin aether_pilot_service --release
powershell -ExecutionPolicy Bypass -File scripts/run-release-readiness.ps1
```

WSL verification uses the same workspace and the same commands via the Linux toolchain.

For performance tracking:

```bash
cargo run -p aether_api --example performance_dashboard --release
cargo run -p aether_api --example performance_report --release -- --suite full_stack --host-manifest fixtures/performance/hosts/dev-chad-windows-native.json
cargo run -p aether_api --example capture_performance_baseline --release -- --suite core_kernel --host-manifest fixtures/performance/hosts/dev-chad-windows-native.json --output fixtures/performance/baselines/core_kernel/dev-chad-windows-native.json
cargo run -p aether_api --example performance_drift_report --release -- --suite core_kernel --host-manifest fixtures/performance/hosts/dev-chad-windows-native.json --baseline fixtures/performance/baselines/core_kernel/dev-chad-windows-native.json
cargo run -p aether_api --example performance_matrix_report --release -- --output-json artifacts/performance/matrix/latest.json --output-report artifacts/performance/matrix/latest.md <bundle-path-1> <bundle-path-2>
cargo bench -p aether_api
cargo test -p aether_api --test performance_stress --release -- --ignored --nocapture
```

The current tracked accepted release baselines live under `fixtures/performance/baselines/<suite-id>/<host-id>.json`. Today the canonical gated references are the native Windows dev-host suites `core_kernel` and `service_in_process`.

For packaged pilot deployment review on Windows:

```text
double-click scripts/build-pilot-package.cmd
```

The workspace is currently verified under:

- Windows stable MSVC toolchain
- WSL Ubuntu stable GNU toolchain

GitHub Actions now runs the same format, lint, and test gates on both Ubuntu and Windows so the public CI badge reflects the actual contributor contract.

For structured release preparation, use the dedicated QA runner described in [`docs/QA.md`](./docs/QA.md). It produces a saved transcript and summary, builds the Pages preview bundle, executes the pilot launch pack, and builds the packaged Windows pilot bundle from the same candidate tree.

For post-v1, multi-perspective defect hunting, use the hardening lane described
in [`docs/QA_HARDENING_PROGRAM.md`](./docs/QA_HARDENING_PROGRAM.md). That path
adds persona sweeps, disclosure guidance, and non-blocking hardening
automation without changing the stable kernel or release semantics.

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
- coordination-style readiness, heartbeat-backed authority, and outcome-fencing derivations

Not yet supported in the runtime:

- recursive or generalized aggregation beyond the current non-recursive head-term slice
- optimizer-grade plan selection beyond the current semi-naive slice

This is intentional. The project is building from semantic bedrock upward. The right next steps are to preserve correctness while widening expressive power, not to rush into breadth and backfill meaning later.

## Roadmap

The original milestone spine from `M0` through `M5` now functions mainly as a
historical scaffold. The current frontier is no longer “can the semantic kernel
exist?” It is:

- how far to harden the single-node pilot boundary
- how far to deepen operator-facing proof and reporting
- how to scale from exact local truth to replicated authority partitions
- which post-v1 ergonomics matter without blurring the semantic center

The active planning document for that work is
[`docs/ROADMAP.md`](./docs/ROADMAP.md).

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
- `docs/PILOT_DEPLOYMENT.md` explains the hardened packaged deployment path for the pilot service.
- `docs/PILOT_OPERATIONS_PLAYBOOK.md` explains deployment, rotation, upgrade, rollback, and restart/replay operations for the pilot service.
- `docs/PERFORMANCE.md` explains the benchmark harness, stress tests, and performance-report path.
- `docs/QA_HARDENING_PROGRAM.md` defines the internal-first hardening rubric, persona sweeps, and phased promotion policy for new QA checks.
- `docs/PILOT_LAUNCH.md` defines the current launch-readiness contract and validation pack for the design-partner pilot.
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
