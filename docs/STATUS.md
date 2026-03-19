# STATUS

## Current state

The repository has advanced from a pure specification bundle to a functioning early implementation workspace.

Completed:

- Rust workspace root created
- canonical Rust crates added under `crates/`
- Go and Python boundary directories created
- schema, storage, resolver, compiler, and runtime substrate implemented as an initial vertical slice
- source datom provenance threaded through resolution and derivation
- first recursive tuple explainer implemented
- first whole-document DSL parser implemented for `schema`, `predicates`, `facts`, `rules`, `materialize`, and `query`
- `Current` and `AsOf` query execution implemented
- policy annotations supported on DSL-authored extensional facts
- semi-naive delta execution implemented for recursive SCC evaluation
- executable stratified negation implemented for stratified programs
- first coordination acceptance slice implemented for readiness, claims, leases, and stale-attempt rejection
- in-memory kernel service implemented in `aether_api`
- documentation portal, architecture guide, developer workflow guide, operator guide, glossary, and documentation standards now exist
- unit tests added across the Rust core crates
- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test` verified on Windows and WSL
- GitHub CI added for Ubuntu and Windows
- repository front-door docs, contribution guidance, and worked examples now exist

Not yet completed:

- bounded aggregation
- full canonical DSL coverage beyond the current query/fact/policy slice
- durable storage backends
- process-boundary kernel service integrations
- stable Go and Python boundary clients

## Immediate focus

The most immediate work now sits between late `M3` and early `M4`:

- widen tuple explanation into richer proof and operator-facing surfaces
- extend the DSL from the current focused authoring surface to the full canonical language
- add bounded aggregation and further runtime optimization
- expand worked examples, API-level scenarios, and acceptance-style tests
