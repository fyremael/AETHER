# ROADMAP

## Planned milestones

### M0 - Rust substrate core
- strengthen IDs and value model
- complete schema registry behavior
- implement append-only in-memory journal
- add deterministic temporal replay coverage

### M1 - Rust resolver core
- current-state materialization
- `AsOf` materialization
- merge semantics by attribute class

### M2 - Rust rule compiler
- canonical v1 DSL parsing
- safety checks
- type validation
- dependency graph construction
- SCC decomposition
- stratification

### M3 - Rust recursive runtime
- semi-naive evaluation
- recursive SCC iteration
- materialized intensional relations
- derivation metadata

### M4 - API boundary
- stable request/response types
- serialized boundary contracts
- process-boundary service shape

### M5 - Go shell and Python SDK
- operator shell
- SDK ergonomics
- benchmark and fixture harnesses

## Next architecture track

The next concrete architecture slice after the current pilot is:

- authority partitions
- federated cuts
- imported facts with provenance
- partition-aware operator and explain surfaces

The governing decision is
`docs/ADR/0001-authority-partitions-and-federated-cuts.md`.
The execution plan is
`docs/FEDERATED_TRUTH_IMPLEMENTATION_PLAN.md`.
