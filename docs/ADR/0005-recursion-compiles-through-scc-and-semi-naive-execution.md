# ADR 0005: Recursion Compiles Through SCCs And Semi-Naive Execution

- Status: Accepted
- Date: 2026-03-28
- Deciders: AETHER maintainers
- Related:
  - `SPEC.md`
  - `RULES.md`
  - `docs/ARCHITECTURE.md`
  - `docs/SEMANTIC_COMPLIANCE_MATRIX.md`

## Context

Recursive derivation is part of AETHER's semantic center, not an optional add-on
for one narrow query pattern.

The system needs a recursive execution model that is:

- explicit
- deterministic
- explainable
- compatible with replay and provenance
- general enough to support real recursive programs instead of one-off graph
  helpers

The tempting shortcuts are all structurally wrong:

- bespoke graph traversals for hand-picked reachability cases
- recursive SQL or service code paths that bypass the kernel model
- procedural fixed-point logic without explicit compilation structure

Those paths may solve isolated examples, but they do not yield a defensible
semantic runtime.

## Decision

AETHER compiles recursion through dependency graphs, SCC decomposition,
stratification, and semi-naive delta execution.

The compilation and execution shape is:

1. validate rule safety and schema compatibility
2. build predicate dependency structure
3. compute strongly connected components
4. verify stratification for negation
5. lower recursive SCCs to semi-naive delta plans
6. execute to a fixed point with explicit iteration tracking

Derived tuples and explain traces should preserve metadata such as:

- predicate
- rule
- stratum
- SCC
- iteration
- parent tuple linkage

## Consequences

Positive:

- recursive behavior has one general execution story instead of many special
  cases
- explainability and provenance can align with actual runtime structure
- replay invariants remain compatible with recursive derivation
- the runtime can widen carefully without changing its governing shape

Tradeoffs:

- the compiler and runtime are more explicit than a shortcut implementation
- recursive behavior is constrained to the SCC/semi-naive model instead of
  hidden procedural escape hatches
- optimization work must respect the compiled recursive structure

## Rejected Alternatives

### Hand-written graph traversal utilities

Rejected because they solve only isolated recursive cases and do not generalize
to the rule model.

### Recursion hidden inside storage or service layers

Rejected because it blurs the semantic center and makes proofs and replay
harder to defend.

### Naive full re-evaluation as the governing design

Rejected because semi-naive execution is the correct architectural center for
the intended recursive runtime, even if implementation details widen later.
