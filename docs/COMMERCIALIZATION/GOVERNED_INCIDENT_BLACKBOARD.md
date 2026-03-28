# Governed Incident Blackboard

This document is the source-of-truth product narrative for the **AETHER
incident blackboard** exemplar.

It exists to answer the question many first-time readers actually have:

**What is AETHER useful for in a real agentic workflow?**

The answer should be legible before anyone has to care about kernel vocabulary.

## Plain-Language Opener

The easiest way to understand AETHER is as the fabric underneath a governed
incident board.

Monitoring agents, planner agents, and operators share one workspace for live
observations, candidate remediation actions, approvals, claims, handoffs, and
fenced stale attempts. The board does not just list activity. It can show what
is active now, which action is truly ready, who may act, what changed since the
last handoff, and why the current answer is true.

## The Ordinary Failure Mode

Without a shared semantic center, incident coordination fragments quickly:

- alerts say one thing
- tickets say another
- chat carries informal approvals
- automation runs on stale assumptions
- operators reconstruct the story after the fact from partial logs

That is where trust breaks. Teams stop arguing about one action and start
arguing about what the system even believed.

## The AETHER Answer

AETHER turns that fragmented workflow into one governed operational surface.

In business terms, the incident blackboard provides:

- **shared memory**
  all relevant board facts live in one append-only history instead of scattered side systems
- **derived readiness**
  the system works out which action is truly ready from the full dependency and signal chain
- **governed action**
  authority, claims, handoff, and stale-attempt rejection are derived from live semantic state
- **replay**
  operators can inspect what the board believed at an exact prior cut of history
- **proof**
  the board can explain why an action was ready, authorized, or fenced

## What Lives On The Board

Client-facing board objects:

- observations
- candidate actions
- approvals
- claims
- handoffs
- fenced stale actions

In the current exemplar:

- observer agents publish live incident signals
- planner agents publish candidate remediation actions
- AETHER derives which action is ready
- remediator authority moves through lease epochs
- stale attempts remain in history but are rejected in the live semantic view

## The Five Operator Questions

This exemplar is meant to answer five questions cleanly:

1. **What is active now?**
   Which live observations and candidate actions are on the board?
2. **What action is actually ready?**
   Which candidate has the needed signals, approvals, and completed dependencies?
3. **Who may act now?**
   Which worker or agent currently holds valid authority?
4. **What changed since the last handoff?**
   How did the authorized actor change between one semantic cut and the next?
5. **Why was a prior action rejected?**
   Which stale attempt no longer matched live authority, and why?

## Why This Exemplar Works

The governed incident blackboard is a good front door because it keeps the
story concrete:

- the audience can picture the workspace immediately
- the operator questions sound real
- the current pilot proof already supports the key semantics underneath
- the future agentic direction becomes believable without pretending it is already finished

This is why the incident blackboard is the recommended **adjacent-next**
product exemplar for design-partner conversations.

## Truth Boundary

This exemplar is intentionally disciplined.

It does **not** mean:

- AETHER is already a finished multi-agent control plane
- AETHER now exposes a stable tuple-space product API
- AETHER should be marketed primarily as a Linda clone

It **does** mean:

- the current kernel can already support a governed shared workspace story
- the story is honest because it reuses live proof surfaces that exist today
- the incident blackboard is product packaging over current semantics, not a new semantic claim

## How To Present It

Use this order in live conversations:

1. show the active observations
2. show the candidate actions
3. show which action is ready
4. show who is authorized now
5. show the prior `AsOf` cut and the fenced stale attempt
6. show the proof trace

That sequence keeps business utility ahead of terms like recursion, strata, or
semi-naive execution.

## Run The Exemplar

Documentation walkthrough:

- `examples/demo-04-governed-incident-blackboard.md`

Runnable example:

- `cargo run -p aether_api --example demo_04_governed_incident_blackboard`

Windows operator path:

- `scripts/run-demo-04.cmd`

Related proof anchors:

- `examples/demo-03-coordination-situation-room.md`
- `docs/PILOT_COORDINATION.md`
- `docs/COMMERCIALIZATION/CANONICAL_USE_CASES.md`

## Technical Appendix

Only after the story lands, map the board language back to AETHER terms.

| Board language | AETHER meaning |
| --- | --- |
| governed incident board | semantic coordination fabric over one append-only history |
| active observations | extensional facts visible at `Current` or `AsOf` |
| candidate action | a published action entity plus its semantic state |
| actually ready | a derived fact closed over dependencies, signals, approvals, and suppression |
| who may act now | live lease-backed authority |
| handoff | a change in current authority across semantic cuts |
| fenced stale action | an execution attempt preserved in history but no longer authorized in live state |
| why is this true | provenance-bearing explanation trace |

TupleSpace / Linda note:

- for technical audiences, this exemplar is compatible with a TupleSpace-style reference pattern on top of AETHER
- for client-facing conversations, the preferred term is **governed incident blackboard**

That distinction matters because the reference pattern is real, but it is not
the primary product identity.
