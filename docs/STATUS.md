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
- bounded aggregation implemented for non-recursive head-term `count`, `sum`, `min`, and `max` rules
- first coordination acceptance slice implemented for readiness, claims, leases, and stale-attempt rejection
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
- documentation portal, architecture guide, developer workflow guide, operator guide, glossary, and documentation standards now exist
- GitHub Pages publishing pipeline added for the documentation portal and generated Rust API reference
- unit tests added across the Rust core crates
- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test` verified on Windows and WSL
- GitHub CI added for Ubuntu and Windows
- repository front-door docs, contribution guidance, and worked examples now exist

Not yet completed:

- full canonical DSL coverage beyond the current query/fact/policy slice
- broader durable storage backends beyond the current SQLite journal
- production-hardened kernel service integrations beyond the current minimal HTTP boundary
- stable Go and Python boundary clients
- historical benchmark dashboards and CI-enforced drift gates

## Immediate focus

The most immediate work now sits just beyond the launch-ready pilot slice and late `M4`:

- extend launch validation from local/manual execution into CI or scheduled automation when the team is ready
- add longer-duration soak coverage beyond the current launch validation window
- decide how far to widen audit context from the current semantic cut/query/tuple fields into fuller operator intent and semantic diffs
- decide when the current drift comparison should graduate into CI enforcement
- extend the DSL from the current focused authoring surface to the full canonical language
- widen aggregation beyond the current non-recursive head-term slice and continue runtime optimization
