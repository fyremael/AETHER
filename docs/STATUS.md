# STATUS

## Current state

The repository has advanced from a pure specification bundle to a functioning early implementation workspace.

Completed:

- Rust workspace root created
- canonical Rust crates added under `crates/`
- Go and Python boundary directories created
- schema, storage, resolver, compiler, and runtime substrate implemented as an initial vertical slice
- durable SQLite journal implemented behind the `Journal` boundary with restart-safe replay coverage
- source datom provenance threaded through resolution and derivation
- first recursive tuple explainer implemented
- first whole-document DSL parser implemented for `schema`, `predicates`, `facts`, `rules`, `materialize`, and `query`
- `Current` and `AsOf` query execution implemented
- policy annotations supported on DSL-authored extensional facts
- semi-naive delta execution implemented for recursive SCC evaluation
- executable stratified negation implemented for stratified programs
- first coordination acceptance slice implemented for readiness, claims, leases, and stale-attempt rejection
- in-memory kernel service implemented in `aether_api`
- minimal HTTP JSON kernel service implemented over `aether_api`
- kernel service generalized over in-memory and durable journal backends
- coordination pilot contract frozen in restart-safe service and HTTP tests
- bearer-token authentication and endpoint scope enforcement implemented on the pilot HTTP path
- auditable request logging implemented on the pilot HTTP path, including persisted JSONL output
- operator-grade coordination report artifacts implemented in markdown and JSON for the pilot workload
- release-mode performance report example, Criterion benchmarks, and ignored stress workloads added for early performance tracking
- live console dashboard added for real-time and collected instrument views over the performance suite
- machine-readable performance baseline capture and point-in-time drift reporting implemented for the pilot path
- documentation portal, architecture guide, developer workflow guide, operator guide, glossary, and documentation standards now exist
- GitHub Pages publishing pipeline added for the documentation portal and generated Rust API reference
- unit tests added across the Rust core crates
- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test` verified on Windows and WSL
- GitHub CI added for Ubuntu and Windows
- repository front-door docs, contribution guidance, and worked examples now exist

Not yet completed:

- bounded aggregation
- full canonical DSL coverage beyond the current query/fact/policy slice
- broader durable storage backends beyond the current SQLite journal
- production-hardened kernel service integrations beyond the current minimal HTTP boundary
- stable Go and Python boundary clients
- historical benchmark dashboards and CI-enforced drift gates

## Immediate focus

The most immediate work now sits across the active coordination pilot slice and late `M4`:

- widen audit capture from endpoint-level access logs into richer semantic and operator-action context
- add longer-run restart, soak, and misuse drills around the durable pilot service
- decide when the current drift comparison should graduate into CI enforcement
- extend the DSL from the current focused authoring surface to the full canonical language
- add bounded aggregation and further runtime optimization
