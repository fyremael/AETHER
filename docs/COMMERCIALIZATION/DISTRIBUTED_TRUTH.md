# Distributed Truth Strategy

Not one giant truth.
A fabric of exact local truths.

This is the distributed-systems stance for AETHER.

It answers a specific question: how should a semantic coordination fabric scale
without dissolving the very properties that make it valuable?

The short answer is:

- consensus governs source order inside an authority domain
- deterministic replay governs derived meaning
- federation carries truth across domains with explicit provenance

AETHER should not try to make every important fact globally consensus-managed.
It should make each local truth exact, then make cross-domain truth legible.

## 1. The Core Principle

Consensus is for authoritative history, not for every computed answer.

In AETHER terms:

- authoritative truth is the committed datom journal of a partition
- derived truth is the deterministic result of rules over a committed prefix
- federated truth is imported fact from another partition, carried with
  provenance and trust context
- sidecar truth is external artifact or vector material anchored to committed
  journal cuts

This is the line that keeps the system coherent.

If we attempt to run global consensus over derived tuples, reports, explain
artifacts, and sidecar payloads, we will create a distributed system that is
slower, more brittle, and semantically confused.

## 2. The Unit Of Distribution

The right unit of distribution is an authority domain.

An authority domain is a semantic boundary inside which one journal should be
able to answer:

- what is true
- what follows from it
- who may act
- why that action is authorized

Typical authority domains include:

- tenant
- workspace
- incident
- case
- task family
- regional operational domain

The important rule is not "shard evenly first."
It is "keep coordination semantics together."

Claims, leases, heartbeats, outcomes, and the readiness logic that governs them
should remain inside the same authority partition whenever possible.

## 3. What Requires Consensus

Consensus should cover the small set of facts that define semantic authority.

That includes:

- append admission
- element ordering within a partition
- leader epoch and fencing-token issuance
- schema or rule-version activation boundaries, if mutable at runtime
- any source fact that changes who is allowed to act

This is what makes `Current` and `AsOf` exact instead of approximate.

## 4. What Should Not Require Consensus

These surfaces should remain rebuildable from committed journal prefixes:

- resolved state
- derived tuples
- query results
- explain traces
- operator reports
- caches and projections
- sidecar indexes and payload storage

They may be persisted, replicated, cached, or externalized for performance.
They should not become independent semantic authorities.

## 5. Partition-Local Truth

Inside one partition, the model is straightforward:

1. a leader-admitted append becomes part of the committed journal
2. `Current` means the latest committed prefix in that partition
3. `AsOf(eN)` means replay to a specific committed element inside that partition
4. resolver, runtime, explain, and reports derive from that exact prefix

That gives AETHER its strongest property:

exact local truth with deterministic replay.

## 6. Federated Truth Across Partitions

Across partitions, AETHER should not pretend there is one magical global cut.

Instead, it should operate over federated cuts.

Examples:

- `tenant-a@e910`
- `tenant-b@e144`
- `incident-west@e77`

A cross-partition read should say which cuts it used.

That matters because it keeps the system honest:

- no fake global serializability
- no hidden snapshot illusion
- no ambiguity about which authority domains contributed to an answer

## 7. Imported Facts And Trust Boundaries

When truth crosses partitions, it should cross as imported fact with provenance.

That means:

- the source partition is named
- the source cut is named
- the imported fact is attributable
- downstream derivations can explain where that fact came from

This is how AETHER distributes truth without collapsing authority boundaries.

The system does not silently merge worlds.
It composes them explicitly.

## 8. Leases, Heartbeats, Outcomes, And Fencing

The coordination model is where distribution discipline matters most.

The right rule is:

- if a fact changes who may act, it belongs in the authoritative partition for
  that work

For a task or work item, that usually means:

- claim facts
- lease facts
- heartbeat facts
- outcome facts
- fencing or stale-attempt facts

all live in one authority journal.

That keeps the hot path for safe action inside one consensus group and avoids
turning every coordination step into cross-partition negotiation.

## 9. Sidecars

Sidecars may widen operationally.
They may not outrank the journal.

The rule set is:

- artifact and vector registrations anchor to committed journal cuts
- sidecar visibility follows journal order
- sidecar search results re-enter the semantic layer with provenance
- sidecar data may be replicated or externalized, but never promoted to primary
  truth authority

This keeps memory subordinate to truth instead of allowing memory indexes to
silently redefine it.

## 10. The Wrong Shape

AETHER should not become:

- one global consensus log for all truth
- a system that runs consensus over derived state
- a platform that hides cross-partition ambiguity behind a false global `AsOf`
- a sidecar-driven architecture where vector or artifact stores outrank the
  journal
- a distributed transaction fabric that forces every coordination step through
  broad two-phase commit

Those approaches sound powerful, but they would destroy the operating simplicity
and semantic legibility that make AETHER useful.

## 11. Execution Sequence

The correct scale-up order is deliberate:

1. exact single-partition truth
2. leader-replicated journal durability inside a partition
3. read scaling and follower replay
4. federated cuts across partitions
5. imported-fact reasoning with explicit provenance
6. broader sidecar replication and policy enforcement
7. richer multi-partition operator surfaces

This sequence keeps the semantics stable while widening the system.

## 12. The Strategic Payoff

This approach gives AETHER a clean answer to distributed consensus:

we do not distribute truth by pretending the world is one giant ordered log.
We distribute truth by making each semantic domain exact, replayable, and
governed, then federating those domains with provenance.

That is what allows AETHER to scale without becoming a bag of eventually
consistent guesses.

It is also what makes the platform commercially interesting.

Most systems offer fragments:

- event ordering
- workflow state
- memory retrieval
- policy checks
- audit logs

AETHER can offer a stronger control plane:

- exact local truths
- derived operational meaning
- governed action
- explicit cross-domain federation
- replayable proof

That is how truth scales in AETHER:

not as one giant truth,
but as a fabric of exact local truths.
