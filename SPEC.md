# SPEC.md — AETHER Semantic Kernel

## 1. Objective

Build a semantic coordination runtime for distributed agent systems.

The runtime must combine:

- an append-only causal datom journal,
- deterministic temporal replay,
- CRDT/cardinality-aware state materialization,
- a Datalog-native recursive rule engine,
- provenance and policy-aware derivation,
- sidecar federation for artifacts and vectors,
- operational integration through narrow API boundaries.

## 2. Implementation language strategy

### 2.1 Canonical semantic surface
The AETHER DSL is the canonical expression language for:

- schema,
- attribute classes,
- facts,
- queries,
- rules,
- temporal operators,
- visibility/policy annotations.

### 2.2 Mainline kernel language
The **mainline kernel implementation language is Rust**.

Rust owns the authoritative implementation of:

- ASTs,
- schema typing,
- storage abstractions,
- resolver logic,
- rule compilation,
- predicate dependency graphs,
- SCC decomposition,
- semi-naive execution,
- derivation explanation,
- kernel API.

### 2.3 Secondary languages
- **Go** is used for service wrappers, CLI/admin tooling, and deployment shell functionality.
- **Python** is used for research harnesses, fixtures, and experimentation against the Rust API.

### 2.4 Boundary rule
The Rust kernel must be able to stand alone as a library and test target.
Go and Python must not be required for correctness of the semantic core.

## 3. Architectural thesis

AETHER has two internal centers.

### 3.1 Authoritative semantic substrate
This layer owns:

- append-only datoms,
- causal element IDs,
- replica-aware provenance,
- temporal replay,
- CRDT/cardinality-driven resolution,
- coordination facts,
- sidecar references.

### 3.2 Recursive semantic closure
This layer owns:

- predicates,
- rules,
- safety checks,
- stratification,
- SCC-aware planning,
- semi-naive fixed-point execution,
- materialized intensional relations,
- proof/derivation traces.

## 4. Design stance on Janus

Janus informs the substrate layer in spirit:

- datom-oriented storage,
- temporal views,
- explicit phase contracts,
- host-language-embedded ergonomics.

Mainline implementation policy:

- do not hard-fork Janus as the main backbone,
- do not preserve Janus compatibility at the expense of the recursive core,
- use Janus as a reference and benchmark target only.

## 5. Core data model

Primitive unit:

\[
d = (e, a, v, op, t, r, c, p, \sigma)
\]

Where:

- `e`: entity identifier
- `a`: attribute identifier
- `v`: typed value
- `op`: operation kind
- `t`: causal element ID
- `r`: replica identifier
- `c`: causal context summary
- `p`: provenance record
- `σ`: optional policy/capability envelope

### 5.1 Operation kinds

v1 minimum:

- `Assert`
- `Retract`
- `Add`
- `Remove`
- `InsertAfter`
- `LeaseOpen`
- `LeaseRenew`
- `LeaseExpire`
- `Claim`
- `Release`
- `Annotate`

### 5.2 Attribute classes

Each attribute declares one merge class:

- `ScalarLWW`
- `SetAddWins`
- `SequenceRGA`
- `RefScalar`
- `RefSet`

Future classes may add counters and lattice-valued accumulators.

## 6. Provenance model

Every datom must carry:

- `author_principal`
- `agent_id`
- `tool_id`
- `session_id`
- `source_ref`
- `parent_datom_ids`
- `confidence`
- `trust_domain`
- `schema_version`

Every derived tuple must additionally carry:

- `rule_id`
- `predicate_id`
- `stratum`
- `scc_id`
- `iteration`
- `parent_tuple_ids`

## 7. Temporal model

The kernel must expose:

- `History()` — append-only journal
- `Current()` — resolved present state
- `AsOf(element_id)` — resolved state at journal prefix

### 7.1 Replay invariant
For fixed schema, rule set, and journal prefix, `AsOf(t)` and derived views must be deterministic.

## 8. Query and phase model

Non-recursive query fragments compile into ordered phases:

\[
Q \rightsquigarrow (P_1, P_2, \dots, P_k)
\]

Each phase must expose:

- `Available`
- `Provides`
- `Keep`

Recursive programs compile into **phase graphs** where recursive SCCs are explicit iterative subgraphs.

## 9. Rule model

### 9.1 v1 semantics
Support:

- extensional predicates,
- intensional predicates,
- monotone recursion,
- stratified negation,
- bounded aggregation within a stratum.

### 9.2 Compilation pipeline

1. parse DSL or construct AST,
2. validate safety,
3. validate schema/types,
4. build predicate dependency graph,
5. compute SCCs,
6. verify stratification,
7. lower to semi-naive delta plans,
8. lower executable units to phase graphs,
9. register materialization descriptors.

### 9.3 Evaluation model

For immediate consequence operator `T`:

\[
I_{k+1} = I_k \cup \Delta I_{k+1}
\]

with

\[
\Delta I_{k+1} = T(I_k) \setminus I_k
\]

Termination occurs when `ΔI` is empty.

## 10. Coordination model

The semantic substrate must natively represent:

- tasks,
- claims,
- leases,
- heartbeats,
- expiries,
- fences,
- execution outcomes.

### 10.1 Lease fencing invariant
A stale holder must be unable to commit fenced actions under an expired lease epoch.

## 11. Sidecar model

### 11.1 Artifact sidecar
The journal stores artifact references and metadata, not raw blobs.

### 11.2 Vector sidecar
The journal stores embedding metadata and provenance, not dense embedding payloads inline.

### 11.3 Sidecar subordination
Sidecars are subordinate to semantic control. The semantic kernel remains the source of truth for identity, provenance, policy, and orchestration.

## 12. Repository requirements

The implementation repository must be organized as a Rust workspace.

Required top-level structure:

- `Cargo.toml`
- `crates/`
- `go/`
- `python/`
- `docs/`
- `examples/`
- `fixtures/`

Detailed structure is specified in `REPO_LAYOUT.md`.

## 13. Milestones

### M0 — Rust substrate core
- element IDs
- schema typing
- datom journal
- in-memory store
- temporal replay

### M1 — Rust resolver core
- current-state materialization
- attribute merge classes
- deterministic replay tests

### M2 — Rust rule compiler
- AST
- safety
- type validation
- SCC planning
- stratification

### M3 — Rust recursive runtime
- semi-naive execution
- materialized intensional relations
- derivation traces

### M4 — API boundary
- stable Rust library API
- serialization types
- narrow service boundary for Go/Python clients

### M5 — Go shell + Python SDK
- operator tooling
- Python fixture/benchmark SDK

## 14. Non-goals for v1

- full distributed replica protocol,
- deep differential-maintenance integration across arbitrary long-lived streams,
- probabilistic logic,
- weighted fixpoint semantics,
- policy theorem proving,
- optimizer-grade cost modeling.
