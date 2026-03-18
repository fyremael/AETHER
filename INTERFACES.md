# INTERFACES.md

## 1. Boundary philosophy

The authoritative interfaces are Rust library interfaces.

Go and Python interfaces must target a stable boundary exposed by the Rust kernel. In v1, prefer a process boundary or serialized API boundary over deep in-process FFI.

## 2. Canonical Rust crates

### `aether_ast`
Owns:
- identifiers,
- value enums,
- query AST,
- rule AST,
- provenance structs,
- phase/explain structs.

### `aether_schema`
Owns:
- attribute classes,
- schema registry,
- type validation,
- predicate signatures.

### `aether_storage`
Owns:
- journal traits,
- in-memory journal,
- snapshot/prefix access,
- serialization of journal entries.

### `aether_resolver`
Owns:
- current-state materialization,
- `AsOf` materialization,
- deterministic merge semantics by attribute class.

### `aether_rules`
Owns:
- DSL parser or AST builders,
- safety checks,
- type checks,
- stratification logic,
- dependency-graph construction.

### `aether_plan`
Owns:
- phase plans,
- phase graphs,
- SCC decomposition outputs,
- delta-plan lowering.

### `aether_runtime`
Owns:
- semi-naive execution,
- recursive SCC iteration,
- materialized intensional facts,
- runtime iteration metadata.

### `aether_explain`
Owns:
- derivation traces,
- proof trees or compact provenance traces,
- human-readable and machine-readable explanations.

### `aether_api`
Owns:
- stable kernel-facing request/response structs,
- serialization contracts,
- optional process-boundary service definitions.

## 3. Suggested Rust trait boundaries

These signatures are normative in shape, not mandatory in exact syntax.

```rust
pub trait Journal {
    fn append(&mut self, datoms: &[Datom]) -> Result<(), JournalError>;
    fn history(&self) -> Result<Vec<Datom>, JournalError>;
    fn prefix(&self, at: &ElementId) -> Result<Vec<Datom>, JournalError>;
}
```

```rust
pub trait Resolver {
    fn current(&self, schema: &Schema, datoms: &[Datom]) -> Result<ResolvedState, ResolveError>;
    fn as_of(&self, schema: &Schema, datoms: &[Datom], at: &ElementId) -> Result<ResolvedState, ResolveError>;
}
```

```rust
pub trait RuleCompiler {
    fn compile(&self, schema: &Schema, program: &RuleProgram) -> Result<CompiledProgram, CompileError>;
}
```

```rust
pub trait RuleRuntime {
    fn evaluate(
        &self,
        state: &ResolvedState,
        program: &CompiledProgram,
    ) -> Result<DerivedSet, RuntimeError>;
}
```

```rust
pub trait Explainer {
    fn explain_tuple(&self, id: &TupleId) -> Result<DerivationTrace, ExplainError>;
    fn explain_plan(&self, plan: &PhaseGraph) -> Result<PlanExplanation, ExplainError>;
}
```

## 4. Go shell responsibilities

The Go layer may expose:

- CLI commands,
- admin HTTP/gRPC surfaces,
- service lifecycle wrappers,
- deployment-oriented config loading,
- integration adapters.

The Go layer must not:

- own the authoritative rule engine,
- silently fork semantic behavior,
- duplicate materialization logic.

## 5. Python SDK responsibilities

The Python SDK may expose:

- fixture builders,
- benchmark runners,
- notebook helpers,
- high-level query/rule submission clients,
- result explain/visualization helpers.

The Python layer must not become a shadow implementation of the kernel.

## 6. ABI policy

v1 recommendation:

- keep Rust as a library with strong tests,
- expose a narrow serialization API,
- let Go/Python consume it over a process boundary first,
- delay in-process FFI until requirements justify it.

## 7. Invariants

1. Rust kernel semantics are authoritative.
2. For a fixed journal prefix and compiled program, results are deterministic.
3. All derived tuples are explainable.
4. Sidecar-originating tuples must carry provenance.
5. Non-Rust layers must not alter semantic results.
