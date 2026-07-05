# AETHER-POL Semantic Layer

AETHER-POL is the first institutional semantic layer over the AETHER kernel.

The point is not to rename the current coordination pilot. The point is to make
the long-range system form explicit:

> AETHER is for the POLITY.

AETHER supplies the authoritative semantic substrate: append-only facts,
causal element identifiers, replay, policy visibility, recursive closure,
provenance, explanation, and reports.

POL supplies the first-class institutional vocabulary: Polity, Guild, Charter,
Commons, AgentContract, WorkObject, Claim, EvidenceBundle, Critique,
Verification, Decision, RouteProposal, RouteDecision, RouterUpdate, and Ledger.

## Why this layer exists

The repository already proves that AETHER can journal facts, replay exact cuts,
derive recursive truths, explain them, and serve them through authenticated
boundaries. The POL layer gives those abilities a higher-level institutional
object model.

Without POL, AETHER can coordinate agents through facts and rules.

With POL, AETHER can coordinate governed machine institutions through durable
objects that are visible to operators, auditors, routers, and review agents.

## Non-goals

AETHER-POL is not an agent runtime.

It is not a scheduler.

It is not a model router implementation.

It is not a queue, graph runner, planner, or host-language DSL.

It is a semantic vocabulary and projection layer over the existing kernel.

## First implementation slice

The first implementation slice lives in `crates/aether_pol`.

It defines:

- typed string IDs for the POL object model
- serializable Rust structs for core institutional objects
- stable predicate IDs and predicate names
- a predicate catalog
- a `ToPolFacts` trait for projecting typed POL objects into
  `aether_ast::ExtensionalFact`
- unit tests for predicate stability, policy-bearing fact projection, and router
  update disposition handling

The intended architectural flow is:

```text
POL typed object
  -> ToPolFacts
  -> aether_ast::ExtensionalFact
  -> existing AETHER runtime / policy / replay / explanation surfaces
```

## Vocabulary

The first POL vocabulary is deliberately small.

`Polity` names the governed machine society.

`Guild` names a specialized sub-organization inside the polity.

`AgentContract` names a bounded role: capabilities, obligations, permissions,
and trust domain.

`WorkObject` names a unit of institutional work.

`Claim` names an asserted proposition tied to a work object.

`EvidenceBundle` names the evidence attached to a claim.

`Critique` names an objection or review note against a claim.

`Verification` names an explicit support/refutation/inconclusive result.

`Decision` names an accepted institutional outcome.

`RouteProposal` and `RouteDecision` name the allocation path for work.

`RouterUpdate` names the learning signal for no-regret routing: accepted,
retained, or rejected.

## First predicates

The crate emits the following extensional predicates:

```text
polity_declared
guild_declared
agent_contracted
work_object_declared
claim_posted
evidence_attached
critique_posted
verification_posted
decision_posted
route_proposed
route_decided
router_update_posted
```

These predicates are intentionally ordinary. The goal is to let the existing
kernel reason over institutional state without forcing a new runtime substrate.

## Acceptance standard

A first AETHER-POL slice is acceptable when:

1. POL objects are typed and serializable.
2. POL objects project into stable extensional facts.
3. Predicate names and arities are explicit and tested.
4. Policy envelopes propagate into the emitted facts.
5. Router updates distinguish accepted, retained, and rejected outcomes.
6. The layer remains subordinate to the AETHER kernel.

## Next build step

The next build step should add a small runtime integration demo that loads POL
facts and derives minimal institutional readiness:

```text
claim_has_evidence(claim)
claim_supported(claim)
work_has_claim(work)
work_decision_ready(work)
route_update_accepted(update)
```

That is the first point where POL becomes visibly more than a typed data model:
it becomes a governed institutional workspace that AETHER can replay and explain.
