# ADR 0001: Authority Partitions And Federated Cuts

- Status: Accepted
- Date: 2026-03-23
- Deciders: AETHER maintainers
- Related:
  - `docs/COMMERCIALIZATION/DISTRIBUTED_TRUTH.md`
  - `docs/FEDERATED_TRUTH_IMPLEMENTATION_PLAN.md`
  - `docs/ARCHITECTURE.md`

## Context

AETHER's semantic center is now credible in a single-node pilot shape:

- append-only journal truth
- deterministic `Current` and `AsOf`
- recursive derivation
- provenance and explanation
- coordination semantics
- journal-subordinated sidecar memory

The next architectural question is how this model should widen into a
distributed or partitioned system without weakening the semantic invariants that
give the kernel its value.

The wrong answer is a single global truth log.
That approach would centralize coordination, blur trust boundaries, and make
cross-domain reads appear simpler than they really are.

The other wrong answer is to let every partition invent local meaning without a
common model for provenance, temporal cuts, and imported facts.

AETHER needs a scaling shape that preserves exact replay and safe coordination
while admitting broader organizational and operational scope.

## Decision

AETHER will scale around authority partitions and federated cuts.

### 1. Authority partitions are the unit of semantic authority

An authority partition is the smallest domain within which one committed journal
must be able to answer:

- what is true
- what follows from it
- who may act
- why that action is authorized

Typical partition boundaries include:

- tenant
- workspace
- incident
- case
- task family
- regional operational domain

Partition boundaries follow semantic authority, not incidental infrastructure
layout.

### 2. Consensus is local to a partition

Consensus governs source order inside a partition.

That includes:

- append admission
- committed element order
- leader epoch and fencing-token issuance
- any source fact that changes action authority

Consensus does not govern derived tuples, explain traces, reports, caches, or
sidecar indexes.

### 3. `Current` and `AsOf` remain exact inside a partition

Within one authority partition:

- `Current` means the latest committed prefix for that partition
- `AsOf(eN)` means replay to a specific committed element in that partition

There is no claim that a single scalar element can represent a coherent global
cut across unrelated partitions.

### 4. Multi-partition reads use federated cuts

When a read spans partitions, it must identify the cut used for each authority
domain.

The governing abstraction is a federated cut, conceptually:

- `tenant-a@e910`
- `tenant-b@e144`
- `incident-west@e77`

This preserves honesty about temporal scope and avoids fake global
serializability.

### 5. Cross-partition truth crosses as imported fact

When a fact is consumed outside its authority partition, it must be represented
as imported fact with provenance.

That means the receiving system can answer:

- which partition produced the fact
- at which cut it was imported
- what trust boundary it crossed
- how later derivations depend on it

### 6. Coordination hot paths stay local whenever possible

Claims, leases, heartbeats, outcomes, and fencing facts for a piece of work
should live inside the same authority partition whenever possible.

This keeps safe-action decisions local and prevents routine coordination from
turning into broad distributed transactions.

### 7. Sidecars remain subordinate to partition-local journal truth

Artifact and vector stores may widen operationally or become replicated, but
their visibility and semantic effect remain anchored to committed journal cuts
inside the relevant authority partition.

## Consequences

### Positive

- preserves exact replay and deterministic derivation inside a partition
- gives AETHER a clear answer to distributed consensus
- keeps safe coordination local
- enables explicit provenance on cross-domain reasoning
- scales without forcing all truth through one global bottleneck

### Tradeoffs

- multi-partition reads are more explicit and more complex than a fake global
  snapshot model
- imported-fact machinery becomes a first-class part of the architecture
- operator surfaces must present federated time honestly
- product language must resist overpromising global simplicity

## Non-Goals

This ADR does not commit AETHER to:

- a specific consensus algorithm
- multi-region deployment in the near term
- global distributed transactions
- consensus over derived state
- one universal partitioning scheme for every workload

It commits only to the semantic shape the system must preserve as it scales.

## Architectural Implications

The architecture will need explicit representations for:

- partition identifiers
- partition-local element identifiers or partition-qualified cuts
- federated cutsets
- imported facts and their provenance
- local versus federated query and explain flows

The first implementation target is not "planet-scale AETHER."
It is a disciplined partitioned model that can be exercised, tested, and
explained before more ambitious service work appears.

## Rejected Alternatives

### One global append-only log

Rejected because it centralizes unrelated authority domains, creates a large
consensus surface, and encourages misleading product semantics.

### Eventually consistent local stores with opportunistic merge

Rejected because it weakens `Current`, `AsOf`, and explainability exactly where
AETHER must be strongest.

### Consensus over derived state and reports

Rejected because derived state is rebuildable from committed journal prefixes
and should not become an independent authority surface.

## Notes

This ADR is the architectural anchor for the next concrete implementation slice.
The execution sequence and crate-level workstreams are in
`docs/FEDERATED_TRUTH_IMPLEMENTATION_PLAN.md`.
