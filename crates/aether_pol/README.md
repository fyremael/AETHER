# aether_pol

`aether_pol` introduces the first typed AETHER-POL semantic layer.

The crate does not replace the AETHER kernel. It names the institutional objects
that should be represented over the kernel: polities, guilds, charters, commons,
agent contracts, work objects, claims, evidence, critiques, verifications,
decisions, route proposals, route decisions, and router updates.

The governing line is:

> AETHER is for the POLITY.

In implementation terms, this means AETHER remains the authoritative semantic
substrate while POL supplies a stable institutional vocabulary that can be
serialized, projected into extensional facts, replayed, queried, explained, and
reported through the existing kernel surfaces.

## Design boundary

This crate is intentionally a semantic vocabulary layer, not a workflow engine,
agent runtime, scheduler, router implementation, or hot-path tensor system.

It provides:

- typed object IDs for POL concepts
- typed object structs for the first POL vocabulary
- stable predicate IDs and predicate names
- `ToPolFacts` projection into `aether_ast::ExtensionalFact`
- unit coverage for predicate stability, fact projection, policy propagation, and
  router update dispositions

It does not provide:

- storage
- recursive rule execution
- network service surfaces
- model/tool invocation
- autonomous governance enforcement

Those remain subordinate to the existing AETHER kernel and service layers.

## First predicates

The first vocabulary exposes these extensional predicates:

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

These predicates are deliberately boring. They are meant to be durable facts that
existing AETHER replay, policy, explanation, and reporting paths can consume.

## Intended next slice

The next slice should add a small integration example that loads POL facts into
the current runtime and derives basic institutional readiness predicates such as:

- work object has at least one claim
- claim has evidence
- claim has non-blocking verification
- work object is decision-ready
- router update is accepted, retained, or rejected
