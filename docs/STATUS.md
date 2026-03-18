# STATUS

## Current state

The repository has advanced from a pure specification bundle to a functioning early implementation workspace.

Completed:

- Rust workspace root created
- canonical Rust crates added under `crates/`
- Go and Python boundary directories created
- schema, storage, resolver, compiler, and runtime substrate implemented as an initial vertical slice
- unit tests added across the Rust core crates
- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test` verified on Windows and WSL
- GitHub CI added for Ubuntu and Windows
- repository front-door docs, contribution guidance, and worked examples now exist

Not yet completed:

- canonical DSL parser
- executable stratified negation
- bounded aggregation
- durable storage backends
- full derivation provenance threading from source datoms into runtime outputs
- stable Go and Python boundary clients

## Immediate focus

The most immediate work now sits between late `M2` and early `M3`:

- preserve the current recursive runtime slice while tightening it toward true semi-naive execution
- thread `source_datom_ids` through resolution and derivation
- build the canonical DSL parser
- expand worked examples and acceptance-style tests
