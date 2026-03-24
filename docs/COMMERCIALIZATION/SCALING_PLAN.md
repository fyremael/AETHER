# Technical Scaling And Execution Plan

This document is for technical advisors, design-partner diligence, and internal
planning.

Its purpose is simple: show how AETHER scales without losing the semantic
properties that make it valuable in the first place.

The core rule is:

**We do not scale by weakening the kernel. We scale by preserving semantic
invariants while widening operational capacity.**

## 1. Current Baseline

AETHER today is a narrow but credible single-node pilot system.

What is already real:

- append-only journal with deterministic `Current` and `AsOf`
- recursive and stratified rule evaluation
- bounded aggregation for the current slice
- tuple explanation and operator-grade report artifacts
- authenticated HTTP service with audit context
- journal-anchored artifact/vector sidecar federation
- local performance baseline, drift, soak, and stress harnesses

What this means:

- the semantic kernel is no longer speculative
- the remaining scaling work is mostly systems work, service hardening, and
  operationalization

## 2. What “Scaling” Means For AETHER

Scaling here is not only about throughput.

Technical advisors should think about five scaling dimensions:

1. **Semantic scale**
   More rules, more derived tuples, broader coordination workloads, richer
   explain surfaces.

2. **State scale**
   Larger journals, deeper historical replay, more sidecar metadata, bigger
   derivation sets.

3. **Service scale**
   More concurrent requests, longer-lived services, stronger auth, and
   production-style operational controls.

4. **Organizational scale**
   Multi-tenant isolation, policy boundaries, and clearer external contracts for
   non-Rust consumers.

5. **Go-to-market scale**
   The ability to deploy repeatedly into real partner environments without
   heroics.

If we optimize only raw speed and ignore the other four, we will create a fast
system that is strategically weak.

## 3. Non-Negotiable Scaling Invariants

Every scaling phase must preserve these properties:

- deterministic answers for a fixed journal cut and program
- exact `Current` and `AsOf` semantics
- explainable derivations
- journal-subordinated sidecar memory
- explicit authority and fencing semantics
- clear boundaries between semantic authority and boundary clients

Any scaling move that compromises those is not scaling. It is semantic debt.

## 4. Distributed Truth Model

The scaling posture is:

**not one giant truth, but a fabric of exact local truths.**

That means AETHER should scale by replicating authoritative journals inside
semantic domains, then federating across those domains explicitly.

In practice:

- each authority partition owns a committed journal
- `Current` and `AsOf` are exact inside that partition
- derived state is replayed deterministically from that committed prefix
- cross-partition reads use federated cuts, not a fake global element ID
- imported facts cross partitions with provenance
- sidecars stay subordinate to journal truth

The concise rule is:

- consensus governs source order
- deterministic replay governs derived meaning

This is how we scale without turning AETHER into a distributed system that is
technically impressive but semantically vague.

## 5. Phase Plan

## Phase 1: Single-Node Scale-Up

### Objective

Take the current design-partner pilot from “credible prototype” to “repeatable
single-node product surface.”

### Scope

- packaged durable deployment
- stronger configuration and secret handling
- required launch validation in CI
- historical performance trend storage
- improved operator report and HTML incident surfaces
- sidecar durability hardening and recovery drills
- plan caching and repeated-query efficiency work
- snapshotting / checkpointing for faster restart and replay on larger journals

### What this phase is trying to prove

- the pilot can be run repeatedly and confidently
- the service can survive realistic single-node operational pressure
- performance evidence becomes a release discipline, not just a local tool

### Exit gates

- launch validation runs as a required gate for the pilot branch or release path
- benchmark history is captured over time, not just as point-in-time files
- restart time and replay time are measured and tracked for larger fixtures
- operator reports are strong enough to support real incident review

## Phase 2: Production-Credible Service Plane

### Objective

Move from a durable pilot service to a more production-credible service
boundary.

### Scope

- namespace or tenant isolation
- stronger auth and scoped authorization
- policy-envelope-aware access decisions at the service layer
- background jobs for report generation and sidecar maintenance
- service metrics, tracing, health semantics, and upgrade playbooks
- packaging for repeatable deployment in partner environments

### What this phase is trying to prove

- AETHER can operate as a dependable system boundary, not just a local kernel
- the service can host more than one workload cleanly
- operators can understand and recover the system without kernel developers in
  the loop

### Exit gates

- tenant or namespace separation is enforced
- deployment and restart procedures are documented and exercised
- service metrics exist for request, replay, report, and sidecar workloads
- advisor-grade architecture review passes on auth, recovery, and observability

## Phase 3: Partitioned Operational Scale

### Objective

Introduce broader state and workload scale without turning AETHER into an
incoherent distributed system.

### Scope

- journal partitioning by tenant / workspace / operational domain
- read-side scaling and asynchronous projection where semantically safe
- externalized or replicated sidecar backends
- compaction, archival, and backup strategy
- workload-aware scheduling for expensive explain/report operations

### Guiding principle

Partition by semantic domain, not by accidental infrastructure boundary.

The journal is the authority surface. Partitioning should follow real
coordination boundaries such as workspace, tenant, or incident domain rather
than arbitrary sharding first.

### What this phase is trying to prove

- AETHER can scale operationally without turning replay and explanation into
  best-effort features
- sidecars can widen beyond single-node durability while remaining subordinate
  to semantic truth

### Exit gates

- partition boundaries are explicit and documented
- replay and explain semantics remain deterministic inside a partition
- sidecar consistency guarantees are stated and tested
- backup, restore, and migration procedures exist

## Phase 4: Control-Plane Expansion

### Objective

Turn the kernel and service into a broader product surface.

### Scope

- richer operator dashboards
- semantic diffs between cuts
- stronger builder workflows and SDKs
- connectors and integration surfaces
- broader coordination templates beyond the initial pilot

### What this phase is trying to prove

- AETHER is not just a technically interesting runtime
- it is becoming a usable, repeatable control layer for governed autonomous work

## 6. Sidecar-Specific Scale Plan

The sidecar seam deserves its own section because it is one of the easiest ways
to accidentally break the thesis.

### Current state

- sidecar registrations are anchored to real journal cuts
- sidecar search visibility follows journal order
- artifact/vector projections can re-enter semantic reasoning with provenance
- durability exists on the SQLite-backed path

### Next steps

1. Durable recovery drills for sidecar replay and repair
2. Background indexing and maintenance jobs that do not redefine semantic cuts
3. Replicated or external backends with explicit consistency contracts
4. Stronger policy enforcement on sidecar retrieval and projection
5. Broader projection shapes beyond the current three-field extensional fact

### The rule

Sidecars may widen operationally, but they may not become an independent truth
authority.

## 7. Performance And Capacity Discipline

We already have the beginnings of a scaling culture:

- baseline capture
- drift comparison
- stress workloads
- launch validation

The next maturity step is to institutionalize that evidence.

### Immediate improvements

- make drift a required release or pilot gate
- retain historical trend data instead of only the latest accepted baseline
- add restart-time and replay-time benchmarks
- add service-boundary benchmarks distinct from in-process kernel benchmarks

### Advisor message

We should talk about measured progression, not abstract “hyperscale.”

The right claim is:

- “AETHER has a disciplined capacity-and-regression program, and it will widen
  by proof.”

## 8. Team And Execution Shape

Scaling AETHER is not just a code problem. It is a sequencing problem.

The execution model should separate four responsibilities:

1. **Kernel authority**
   runtime, resolver, planner, semantic invariants
2. **Service and storage**
   durability, auth, deployment, sidecars, observability
3. **Operator plane**
   reports, dashboards, incident surfaces, explain UX
4. **Design-partner solutions**
   workload modeling, pilot delivery, field feedback

The mistake would be to let all four blur together into opportunistic work.

## 9. What We Will Not Do Prematurely

To scale credibly, we need explicit non-goals.

We should not:

- claim multi-region or planetary scale before single-node and partitioned scale
  are proven
- generalize into every agent use case before the coordination wedge is
  undeniable
- build broad SDK surfaces before the core service contract is steady
- chase distributed sophistication at the expense of replay and explainability
- centralize all truth into one global consensus log just to simplify language
- run consensus over derived state, reports, or sidecar indexes

## 10. Why This Is Executable

The key execution argument is that the hard semantic pieces are already real.

That changes the nature of the remaining work.

What remains is not “invent whether the kernel thesis works.”
It is:

- hardening
- packaging
- scaling service and storage boundaries
- improving operator surfaces
- broadening workload coverage in a disciplined order

That is still significant execution work, but it is more tractable than trying
to prove the kernel from scratch.

## 11. The Message For Technical Advisors

The concise answer is:

**AETHER scales in layers.**

First we scale the single-node product surface.
Then we scale the service plane.
Then we scale partitions and sidecars.
Then we widen the control-plane surface.

At each layer, the semantic invariants stay fixed.

That is how we preserve the thing that makes AETHER valuable while still
growing it into a serious systems product.

The longer-form distributed-systems statement for that posture lives in
`docs/COMMERCIALIZATION/DISTRIBUTED_TRUTH.md`.
The concrete next-step build plan is in
`docs/FEDERATED_TRUTH_IMPLEMENTATION_PLAN.md`.
