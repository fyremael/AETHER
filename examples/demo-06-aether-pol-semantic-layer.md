# Demo 06: AETHER-POL Semantic Layer

This demo describes the first AETHER-POL vocabulary slice.

The existing demos show AETHER as a governed coordination kernel for support and
incident workflows. AETHER-POL names the broader institutional object model that
those workflows point toward.

The governing line is:

> AETHER is for the POLITY.

## Scenario

A machine polity has one architecture guild and one review guild.

A router receives a research-architecture work object: introduce an AETHER-POL
semantic layer.

The router proposes candidate guilds. A route decision assigns the work to the
architecture guild. An architect posts a claim. Evidence is attached. A reviewer
posts a verification. A lead posts a decision. The router receives an accepted
update.

The important part is not that agents chatted. The important part is that every
institutional action becomes a typed fact suitable for replay, policy filtering,
explanation, and later no-regret routing.

## Minimal object flow

```text
Polity
  -> Guild
  -> AgentContract
  -> WorkObject
  -> RouteProposal
  -> RouteDecision
  -> Claim
  -> EvidenceBundle
  -> Verification
  -> Decision
  -> RouterUpdate
```

## Fact projection

The `aether_pol` crate projects this flow into the following extensional facts:

```text
polity_declared(...)
guild_declared(...)
agent_contracted(...)
work_object_declared(...)
route_proposed(...)
route_decided(...)
claim_posted(...)
evidence_attached(...)
verification_posted(...)
decision_posted(...)
router_update_posted(...)
```

These are deliberately regular AETHER facts. They are not a side protocol.

## Why this is the right first slice

This slice is small enough to keep the v1 semantic kernel clean, but large enough
to make the next platform direction concrete.

It introduces institutional nouns without pretending that the repository already
contains a full autonomous polity runtime.

It gives future runtime work a stable target:

- derive readiness from claims, evidence, critique, and verification
- explain why a work object is ready or blocked
- preserve policy envelopes on institutional facts
- record route decisions and accepted/retained/rejected router updates
- feed no-regret allocation without burying the learning signal in logs

## Expected next demo

The next demo should execute these facts through the current runtime and derive:

```text
claim_has_evidence(claim)
claim_supported(claim)
work_has_claim(work)
work_decision_ready(work)
route_update_accepted(update)
```

That next step converts the POL vocabulary from typed fact projection into a
replayable institutional proof surface.
