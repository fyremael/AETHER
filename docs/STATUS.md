# STATUS

## Current state

The repository has advanced from a pure specification bundle to a functioning late-M4 single-node pilot semantic kernel workspace.

Completed:

- Rust workspace root created
- canonical Rust crates added under `crates/`
- Go and Python boundary directories created
- schema, storage, resolver, compiler, and runtime substrate implemented as an initial vertical slice
- durable SQLite journal implemented behind the `Journal` boundary with restart-safe replay coverage
- source datom provenance threaded through resolution and derivation
- first recursive tuple explainer implemented
- whole-document DSL parser implemented for the current canonical v1 surface: schema, attribute classes, facts, repeated queries, explain directives, temporal views, and policy annotations
- `Current` and `AsOf` query execution implemented
- policy annotations turned into executable behavior through explicit policy-context filtering on state resolution, document execution, and sidecar reads/searches
- authenticated HTTP tokens now bind maximum semantic policy visibility, with request policy contexts only allowed to narrow that bound
- explain, visible history, audit entries, and operator reports now all follow the same effective-policy cut instead of widening past the caller's semantic visibility
- semi-naive delta execution implemented for recursive SCC evaluation
- executable stratified negation implemented for stratified programs
- bounded aggregation implemented for non-recursive grouped head-term `count`, `sum`, `min`, and `max` rules, including multiple aggregate terms per head; this now covers the v1 bounded-aggregation requirement
- first coordination acceptance slice implemented for readiness, claims, leases, lease heartbeats, execution outcomes, and stale-result rejection
- in-memory kernel service implemented in `aether_api`
- minimal HTTP JSON kernel service implemented over `aether_api`
- kernel service generalized over in-memory and durable journal backends
- coordination pilot contract frozen in restart-safe service and HTTP tests
- bearer-token authentication and endpoint scope enforcement implemented on the pilot HTTP path
- auditable request logging implemented on the pilot HTTP path, including semantic cut/query/tuple context and persisted JSONL output
- operator-grade coordination report artifacts implemented in markdown and JSON for the pilot workload
- release-mode performance report example, Criterion benchmarks, and ignored stress workloads added for early performance tracking
- live console dashboard added for real-time and collected instrument views over the performance suite
- machine-readable performance baseline capture and point-in-time drift reporting implemented for the pilot path
- authenticated HTTP restart-cycle drills added to preserve semantic answers and persisted audit context across repeated service restarts
- ignored release-mode soak and misuse drills added for the authenticated pilot HTTP path
- a one-command pilot launch validation pack added to produce the current report, drift, soak, and stress evidence set
- artifact and vector sidecar federation implemented in `aether_api`, including journal-tail-anchored registration, journal-exact `AsOf` visibility, external artifact references, vector search, semantic fact projection with provenance, and SQLite-backed durability for the durable kernel service
- scheduled/manual GitHub Actions automation added for the pilot launch-validation and drift artifact pack
- launch validation and drift promotion completed into a required mainline CI gate
- packaged durable pilot-service bundles implemented with config-backed startup, package-local rotation tooling, restart/replay benchmark coverage, and secret-file/env/command token resolution
- first real Go operator shell implemented against the HTTP API with typed client coverage
- broader typed Python SDK surface implemented against the HTTP API with fixture builders and live integration coverage
- documentation portal, architecture guide, developer workflow guide, operator guide, glossary, and documentation standards now exist
- GitHub Pages publishing pipeline added for the documentation portal and generated Rust API reference
- unit tests added across the Rust core crates
- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test` verified on Windows and WSL
- GitHub CI added for Ubuntu and Windows
- repository front-door docs, contribution guidance, and worked examples now exist

Not yet completed:

- post-v1 DSL ergonomics and document modularity beyond the current canonical surface
- broader durable storage backends beyond the current SQLite journal
- production-hardened kernel service integrations beyond the current minimal HTTP boundary
- mature Go/Python client ecosystems beyond the current first real boundary clients
- historical benchmark dashboards and fully release-gated drift enforcement on the main CI path

## Immediate focus

The most immediate work now sits just beyond the launch-ready pilot slice and late `M4`:

- add longer-duration soak coverage beyond the current launch validation window
- decide how far to widen audit context from the current semantic cut/query/tuple fields into fuller operator intent and semantic diffs
- continue service-operability hardening beyond the current single-node Windows bundle, with deeper lifecycle management after the new startup-time secret-manager bridge
- decide which post-v1 ergonomic DSL extensions matter beyond the now-implemented canonical surface
- continue runtime optimization now that the current bounded-aggregation requirement is covered
