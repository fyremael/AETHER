# ADR 0010: Policy-Scoped Semantic Snapshots

## Status

Accepted. Implementation is staged across the R1 remediation series.

## Context

AETHER attaches policy envelopes to datoms and extensional facts. The original
service implementation resolved the complete journal and evaluated the complete
rule program before filtering state, derived tuples, queries, and explanations.
That ordering is not authorization-safe for non-monotonic semantics. A hidden
retract can erase a public assertion, a hidden fact can suppress a rule through
negation, and hidden rows can change aggregates, recursive fixed points, tuple
allocation, iteration counts, and provenance before the response is filtered.

Policy is therefore part of the semantic input, not presentation metadata. The
kernel needs one exact contract that applies to current replay, temporal replay,
program facts, recursive execution, explanations, federation, reports, and
caches without making service or host-language callbacks authoritative for rule
semantics.

## Decision

### Canonical scope

The kernel represents an effective authorization domain with a normalized,
non-optional `PolicyScope`. Capabilities and visibilities are sorted and
deduplicated before scope equality, hashing, caching, or audit use. An absent or
empty wire context maps to `PolicyScope::public()`; missing policy never means
unrestricted access.

Trusted policy-neutral library operations may retain explicitly named
unrestricted entry points for compatibility, tests, and migration tooling.
Authenticated service paths must use scoped entry points. Internal debug output
for scopes is redacted and does not print capability or visibility names.

### Cut, project, replay

Every scoped temporal replay follows this order:

1. Select the physical authority prefix requested by `Current` or `AsOf(e)`.
2. Verify that an `AsOf` target is visible to the effective scope.
3. Project datoms in that prefix through the policy scope.
4. Resolve only the projected datoms.
5. Report the last visible element as the visible cut, or no visible cut when
   the projection is empty.

The physical authority cut and visible cut are distinct concepts. A physical
tail may be retained privately for exact recomputation, but lower-privilege
responses and audit summaries expose only the visible cut. A hidden `AsOf`
element and a nonexistent element have the same public error surface.

### Policy-closed dependencies

A visible record must not require a hidden record to replay or explain it. Each
provenance parent, causal-frontier reference, sequence anchor, sidecar anchor,
and imported-fact dependency must precede the dependent record. The dependent
record's policy requirements must include every requirement of the dependency.
Thus, a protected child may depend on a public parent, but a public child may
not depend on a protected parent.

Scoped replay fails closed on dependency violations. Its public error identifies
only the visible dependent element and dependency class; hidden versus missing,
forward, policy-inverted, and structurally invalid targets share that error
surface. The trusted `certify_history_dependencies` scanner separately returns
a stable, serializable list of exact violations without modifying history. That
report is suitable for deciding whether an existing generation can be certified
or must be quarantined.

The first implementation covers every dependency encoded directly by a datom:
`DatomProvenance.parent_datom_ids`, `CausalContext.frontier`, and sequence
anchors. Append admission will later enforce the same invariant before commit;
sidecar/import dependencies must adopt it when their admission contracts are
migrated. Existing histories require certification before mixed-policy writes
are enabled.

### Scoped programs and execution

Policy-bearing extensional program facts are projected before validation and
compilation. Invisible facts cannot affect compiler success, predicate
population, SCC construction, stratification, or executable planning. Rules are
policy-neutral in the current DSL; if rule policies are introduced, rule
projection must also precede safety checking and stratification.

The security-bearing runtime path consumes a policy-scoped resolved snapshot
and a program compiled for the same scope. Semi-naive closure, recursion,
negation, aggregation, tuple allocation, provenance, iteration metadata,
queries, and explanations are all computed inside that domain. Output filtering
may remain only as a fail-closed assertion; it cannot construct semantics.

Because AETHER supports negation, widening a scope does not imply that result
sets grow monotonically. The required noninterference property is instead:

> If two authority histories and programs have equal projections for a scope,
> their observable semantic results for that scope are equal.

### Cache and explanation identity

Evaluation identity includes namespace, normalized scope, requested temporal
view, visible-prefix digest, schema digest, scoped-program/document digest,
imported cuts and leader epochs where applicable, and an engine-semantics
version. Tuple identifiers alone are not cache or explanation identities.

Physical hidden tails, total unfiltered counts, hidden iteration shape, and raw
scope contents are excluded from public handles and metadata. Caches are
bounded and partitioned by namespace and semantic scope. An append that does not
change a scope's visible projection may reuse that scope's immutable evaluation;
it must not cause cross-scope cache reuse.

### Timing boundary

This decision establishes semantic and response-level noninterference. It does
not claim constant-time replay, resistance to traffic analysis, or elimination
of all storage/cache timing side channels. Those remain explicit non-claims and
must not be implied by product or readiness documentation.

## Consequences

- Hidden retracts, negated facts, aggregate inputs, and recursive edges cannot
  change public truth.
- `Current` and `AsOf` retain deterministic replay while exposing scope-local
  cut metadata.
- Resolver, compiler, runtime, query, explanation, report, federation, and
  cache APIs require typed scoped inputs instead of optional late filters.
- Existing unrestricted resolver/runtime APIs remain compatibility surfaces,
  not authenticated authorization boundaries.
- Some previously accepted histories may fail policy-dependency certification
  and must remain read-only or be migrated into a new certified generation.
- Projection and digest construction add work, but correctness takes priority;
  performance work may optimize the projection without moving policy filtering
  after semantic evaluation.
- Mixed-policy and commercial-beta claims remain blocked until the full R1
  adversarial suite, backend parity, metadata checks, and performance gates are
  green.

## Rejected Alternatives

### Filter only responses

Rejected because non-monotonic operations have already changed truth before
the filter runs.

### Teach only query matching about policy

Rejected because negation, aggregation, recursion, and tuple allocation occur
before query matching.

### Maintain one privileged materialization and redact it for every caller

Rejected because a privileged fixed point is not generally projectable into a
lower-privilege fixed point when retracts, negation, and aggregation exist.

### Treat missing policy as unrestricted

Rejected because omission at an authenticated boundary would become an access
escalation and would make cache identity ambiguous.
