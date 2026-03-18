# TESTPLAN.md

## 1. Test philosophy

The Rust kernel is the authority target. All semantic correctness tests must pass at the Rust library layer before Go or Python wrappers are considered meaningful.

## 2. Rust unit tests

### `aether_ast`
- IDs are deterministic and serializable
- values round-trip correctly
- provenance structs preserve required fields

### `aether_schema`
- attribute classes are validated
- predicate signatures reject mismatched types
- schemas are immutable or versioned as specified

### `aether_storage`
- append preserves journal order
- history returns all entries
- prefix selection for `AsOf` is correct

### `aether_resolver`
- scalar LWW resolves deterministically
- add-wins set preserves concurrent add/remove behavior
- sequence resolution is stable and deterministic
- `Current()` equals `AsOf(max_id)`

### `aether_rules`
- parser or AST builders preserve semantics
- unsafe rules are rejected
- unstratified v1 programs are rejected
- dependency graph is correct

### `aether_plan`
- SCC decomposition matches expected graphs
- phase graphs preserve recursive boundaries
- delta plans are stable for golden examples

### `aether_runtime`
- transitive closure reaches least fixed point
- no new tuples after convergence
- iteration counts are exposed
- derived tuples carry provenance metadata

### `aether_explain`
- derivation traces are complete
- plan explanations are serializable
- tuple explanation references rule IDs and parent tuples

## 3. Integration tests

### Core semantic integration
- schema + journal + resolver + rules + runtime work together end to end

### Sidecar integration
- artifact references remain external
- vector search results re-enter the semantic layer with provenance metadata

### Coordination integration
- stale fenced lease holder is rejected
- task readiness updates correctly when lease state changes

## 4. Wrapper tests

### Go wrapper
- Go shell can submit facts/rules and retrieve results through the stable boundary
- Go shell does not alter semantic outcomes

### Python SDK
- Python fixture builders can construct journals and programs
- Python client receives deterministic results from the Rust kernel

## 5. Acceptance scenario

A minimal v1 acceptance run must:

1. define a schema,
2. append tasks and dependencies,
3. materialize current state,
4. compile and run recursive dependency closure,
5. explain a derived tuple,
6. open a lease and verify task readiness changes,
7. replay `AsOf` a prior element ID,
8. show identical results across repeated runs.

## 6. Benchmark targets for early tracking

Track at least:

- journal append throughput,
- resolver throughput,
- SCC compile time,
- recursive closure runtime on DAG fixtures,
- derivation trace memory overhead.
