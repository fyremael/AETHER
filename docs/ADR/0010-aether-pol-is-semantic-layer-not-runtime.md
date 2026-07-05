# ADR 0010: AETHER-POL Is a Semantic Layer, Not a Runtime

## Status

Accepted.

## Context

AETHER has reached a full-v1 single-node semantic kernel posture with a
launch-ready pilot surface. The next platform direction needs to express the
larger system form without dissolving the current center of gravity.

The adopted governing line is:

> AETHER is for the POLITY.

This line should not turn AETHER into an agent runtime, queue, graph runner,
scheduler, or model-serving layer. The repository's existing stance remains that
AETHER is the authoritative semantic kernel: append-only facts, causal element
identifiers, temporal replay, policy-aware visibility, recursive closure,
provenance, explanation, and reports.

## Decision

AETHER-POL is introduced as a semantic vocabulary layer over the kernel.

The first implementation slice lives in `crates/aether_pol` and defines typed
institutional objects plus stable predicate projections into
`aether_ast::ExtensionalFact`.

The initial object vocabulary is:

- Polity
- Guild
- AgentContract
- WorkObject
- Claim
- EvidenceBundle
- Critique
- Verification
- Decision
- RouteProposal
- RouteDecision
- RouterUpdate

The first predicate vocabulary is:

- `polity_declared`
- `guild_declared`
- `agent_contracted`
- `work_object_declared`
- `claim_posted`
- `evidence_attached`
- `critique_posted`
- `verification_posted`
- `decision_posted`
- `route_proposed`
- `route_decided`
- `router_update_posted`

## Consequences

The POL layer may name institutional objects and project them into facts.

The POL layer must remain subordinate to the kernel's storage, replay, policy,
explanation, and rule-evaluation surfaces.

Future routing, governance, and verification work should emit and consume POL
facts instead of burying institutional state in logs, comments, or ad hoc JSON
payloads.

No-regret routing can use `RouteDecision` and `RouterUpdate` as stable semantic
objects, but this ADR does not accept a router implementation.

Autonomous agent execution remains outside this decision. POL records
institutional action; it does not perform that action.

## Acceptance Criteria

This decision is satisfied by a slice that:

1. Adds an AETHER-POL crate to the Rust workspace.
2. Defines typed institutional objects.
3. Defines stable predicate IDs, names, and arities.
4. Projects typed objects into `aether_ast::ExtensionalFact`.
5. Preserves policy envelopes on projected facts.
6. Documents the semantic-layer boundary.
7. Avoids claiming that a full polity runtime exists.
