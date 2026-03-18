# REPO_LAYOUT.md

## 1. Required repository structure

```text
.
├── Cargo.toml
├── crates/
│   ├── aether_ast/
│   ├── aether_schema/
│   ├── aether_storage/
│   ├── aether_resolver/
│   ├── aether_rules/
│   ├── aether_plan/
│   ├── aether_runtime/
│   ├── aether_explain/
│   └── aether_api/
├── go/
│   ├── cmd/
│   ├── internal/
│   └── README.md
├── python/
│   ├── aether_sdk/
│   ├── tests/
│   └── README.md
├── docs/
│   ├── ADR/
│   ├── STATUS.md
│   ├── ROADMAP.md
│   └── KNOWN_LIMITATIONS.md
├── examples/
├── fixtures/
└── scripts/
```

## 2. Crate responsibilities

### `aether_ast`
- foundational identifiers
- values
- query AST
- rule AST
- provenance types

### `aether_schema`
- attribute classes
- schema registry
- predicate signatures
- type validation support

### `aether_storage`
- journal abstractions
- in-memory journal
- future durable journal adapters

### `aether_resolver`
- current-state materialization
- `AsOf` materialization
- merge semantics by attribute class

### `aether_rules`
- DSL parsing
- AST builders
- safety validation
- stratification checks

### `aether_plan`
- dependency graph
- SCC decomposition
- phase plans
- phase graphs
- delta-plan lowering

### `aether_runtime`
- semi-naive evaluator
- recursive SCC iteration
- derived set maintenance

### `aether_explain`
- derivation traces
- plan explanations
- renderers/serializers

### `aether_api`
- stable request/response structs
- serialization schema
- service-boundary definitions

## 3. Initial implementation order

1. `aether_ast`
2. `aether_schema`
3. `aether_storage`
4. `aether_resolver`
5. `aether_rules`
6. `aether_plan`
7. `aether_runtime`
8. `aether_explain`
9. `aether_api`
10. `go/` and `python/` boundary clients

## 4. What not to do

- do not place core rule execution in `go/`
- do not place authoritative semantics in `python/`
- do not merge all Rust crates into one giant crate in v1 unless a later ADR justifies it
- do not add network/distributed concerns before the library core passes acceptance tests
