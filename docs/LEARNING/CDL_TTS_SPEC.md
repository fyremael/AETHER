# Coordinated Distributed Learning over Typed Tuple Spaces

## Status

This document establishes AETHER-Learn as a formal Grand Challenge Labs project under the AETHER purview.

The project is intentionally staged. The first proof is **no-regret routing over typed learning tuples**. Distributed gradient training is out of scope until the learning-control-plane claim is demonstrated.

## Thesis

Learning is not only parameter motion. Learning is governed semantic change.

AETHER-Learn makes learning network-native by allowing learning artifacts to circulate as typed, replayable, provenance-bearing tuples inside a governed semantic fabric. A system learns when its weights, memories, routing policies, evaluation ledgers, and governance rules change under explicit evidence.

## Non-claims

AETHER-Learn does not claim that AETHER is already:

- a full ML orchestration platform;
- a production multi-host cluster manager;
- a replacement for numerical ML frameworks;
- a global consensus log for all learning artifacts;
- a vector store promoted to semantic authority.

AETHER-Learn is a semantic coordination layer around learning. The numerical work can remain in specialized systems.

## Formal model

Let authority domains be:

```text
D = {D_1, ..., D_n}
```

Each domain maintains an AETHER journal:

```text
J_i = (tau_i1, tau_i2, ..., tau_it)
```

A committed prefix defines an exact local cut:

```text
C_i^t = J_i[1:t]
```

Resolution and rules derive local semantic state:

```text
R_i(C_i^t) -> T_i^t
```

A federated learning view is not a fake global snapshot. It is an explicit vector of cuts:

```text
F = {D_a@C_a^p, D_b@C_b^q, ...}
```

A learner consumes local state plus optional imported facts:

```text
q_j = Q_j(T_i^t, F)
```

It emits a learning claim:

```text
LearningClaim(artifact, evidence, scope, risk, rollback)
```

A governance operator decides:

```text
G(claim, evals, visibility, budget, risk) ->
  accepted_local | accepted_federated | sandboxed | rejected | rollback_required | escalated
```

The effective learning state is:

```text
S = (J, R, theta, M, rho, pi, Gamma)
```

where:

- `J` is the journal state;
- `R` is the rule and resolver state;
- `theta` is model/adaptor state;
- `M` is memory/retrieval state;
- `rho` is routing policy;
- `pi` is operational policy;
- `Gamma` is the governance/evaluation rule set.

## Architectural principle

Do not synchronize everything. Coordinate what learns.

AETHER-Learn keeps learning authority local where possible and federates claims with provenance where necessary. A model delta produced in one domain does not become universal truth. It becomes an imported learning claim that other domains may evaluate, sandbox, accept locally, or reject.

## First proof obligation

The first obligation is to show that a learning loop can be represented and improved through typed tuples without touching model weights.

The no-regret routing proof must show:

- task arrival as typed tuples;
- worker proposals as typed tuples;
- routing decisions as replayable tuples;
- evaluated outcomes as evidence tuples;
- router updates as learning deltas;
- promotion/retention decisions as governance tuples;
- measurable improvement under distribution shift.

## Later proof obligations

After routing is proven:

1. Memory promotion: candidate memories become promoted memories only after evaluation.
2. Federated learning claims: local model deltas cross authority domains as provenance-bearing claims.
3. MODULUS integration: deltas carry norm, boundary, and risk metadata.
4. SPLICE/SPINDLE integration: routing decisions carry operator-splitting and adaptive-depth diagnostics.
5. Distributed gradient training: only after semantic promotion, rollback, and evaluation discipline are established.
