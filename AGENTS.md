# AGENTS.md — Codex Implementation Instructions

This repository is designed for coding agents. Follow these rules strictly.

## 1. Primary objective

Build the **Rust mainline semantic kernel** first.

Do not begin by fleshing out the Go shell or the Python SDK beyond stubs and interface declarations.

## 2. Non-negotiable architectural constraints

### 2.1 Language boundaries
- Rust is the authoritative kernel.
- Go is a shell/gateway/tooling layer.
- Python is an SDK/harness layer.
- The AETHER DSL is the canonical semantics surface.

### 2.2 No semantic collapse
Do not collapse rule semantics into host-language callbacks.
Do not encode recursive derivation as bespoke graph traversal utilities.
Do not let service/API concerns dictate the internal rule engine architecture.

### 2.3 Mainline repository center
The repository root must be a Rust workspace.

## 3. Initial repository creation order

Create these top-level items first:

1. `Cargo.toml` workspace
2. `crates/aether_ast`
3. `crates/aether_schema`
4. `crates/aether_storage`
5. `crates/aether_resolver`
6. `crates/aether_rules`
7. `crates/aether_plan`
8. `crates/aether_runtime`
9. `crates/aether_explain`
10. `crates/aether_api`
11. `go/` stubs
12. `python/` stubs

Do not start with RPC servers, databases, or distributed deployment.

## 4. First acceptance target

A library-level acceptance target is complete when all of the following work:

- define schema with attribute merge classes,
- append datoms to an in-memory journal,
- materialize current state,
- replay `AsOf`,
- parse a minimal rule program,
- safety-check and stratify it,
- compile a recursive SCC,
- run semi-naive closure,
- return derivation traces.

## 5. Coding style directives

### Rust
- Prefer explicit types over clever macros in v1.
- Avoid unsafe code unless formally justified and documented.
- Keep `no_std` ambitions out of v1 unless required later.
- Model invariants in types where practical.
- Keep alloc/copy behavior observable in benchmarks.

### Go
- Keep the Go layer thin.
- Do not re-implement rule evaluation in Go.
- Prefer RPC/process boundaries to deep FFI in v1.

### Python
- Use typed Python where possible.
- Treat Python as a client, not the authoritative runtime.

## 6. Required internal documents during implementation

Agents must maintain and update:

- `docs/STATUS.md`
- `docs/ADR/`
- `docs/ROADMAP.md`
- `docs/KNOWN_LIMITATIONS.md`

## 7. Mandatory ADRs to write early

1. Why Rust is the mainline kernel language
2. Why AETHER uses a DSL rather than host-language rule authoring
3. Why recursion is compiled through SCC/semi-naive execution
4. Why Go is a shell rather than the core runtime
5. Why sidecars remain subordinate to semantic control

## 8. Forbidden shortcuts

Do not:

- embed arbitrary SQL as the core rule language,
- implement recursion only for one hand-picked reachability case,
- bypass provenance tracking for derived tuples,
- store blobs or embeddings inline in the datom journal,
- erase temporal replay requirements in order to simplify runtime design,
- adopt Janus wholesale without an explicit ADR and spike evidence.

## 9. Preferred development loop

1. implement small typed core,
2. add focused tests,
3. run golden examples,
4. record ADRs,
5. only then widen interfaces.

## 10. Definition of done for v1 core

The v1 core is done when a Rust library can deterministically reproduce recursive derivations from a journal prefix and explain the provenance of each derived tuple.
