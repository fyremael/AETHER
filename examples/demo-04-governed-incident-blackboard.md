# Demo 04: Governed Incident Blackboard

This is the product-facing exemplar for AETHER.

It is designed for design partners, sponsors, and first-time evaluators who do
not want to learn kernel vocabulary before they understand the value.

The story is simple:

- monitoring agents publish live observations onto one incident board
- planner agents publish candidate remediation actions onto the same board
- AETHER derives which action is actually ready
- lease semantics determine who may act now
- stale attempts are fenced after handoff
- operators can replay a prior cut and inspect the proof trail for the current answer

## Why This Demo Exists

The current repository already proves readiness, authority, handoff, replay,
and explanation.

What many new readers still struggle with is utility. They meet terms like
semantic kernel, recursive closure, and semi-naive runtime before they meet the
business question.

Demo 04 flips that order.

It starts with a governed shared workspace for agents and operators, then shows
that AETHER is the fabric underneath it.

## Story

We model one live incident board with two candidate remediation actions.

- the board receives live observations `latency_spike` and `saturation_alert`
- `action/202` is a prerequisite step and is already complete
- `action/201` is the preferred remediation: `shift-read-traffic`
- `action/201` depends on the completed prerequisite and requires the latency signal
- `action/201` is approved and not suppressed, so it becomes ready
- `action/203` is a more disruptive remediation: `restart-primary`
- `action/203` is approved but explicitly suppressed, so it stays off the live ready path
- `remediator-a` first holds the lease for `action/201` at epoch `1`
- later the claim moves to `remediator-b` and the lease epoch advances to `2`
- stale execution attempts are preserved in history but fenced in the current semantic view

## Run It

Simplest Windows operator path:

```text
double-click scripts/run-demo-04.cmd
```

Technical path:

```bash
cargo run -p aether_api --example demo_04_governed_incident_blackboard
```

The Windows launcher writes a timestamped report to `artifacts/demos/demo-04/`.

## Screen-Share Flow

Use the walkthrough in this order:

1. show the active observations on the board
2. show the candidate actions and their board state
3. show which action is actually ready at `AsOf(e15)`
4. show who may act now at `Current`
5. show the prior authorized actor at `AsOf(e18)` and the fenced stale attempts at `Current`
6. show the proof trace for the current authorized action

That ordering keeps the product question first:

- what is active
- what is ready
- who may act
- what changed
- why the answer is true

## What You Should See

The demo prints:

- the board history as append-only semantic facts
- the active observations on the incident board
- the current candidate-action board with approval and suppression state
- the single ready action at `AsOf(e15)`: `action/201, shift-read-traffic`
- the single currently authorized execution at `Current`: `action/201, shift-read-traffic, remediator-b, 2`
- the prior authorization at `AsOf(e18)`: `action/201, shift-read-traffic, remediator-a, 1`
- the fenced stale attempts that no longer match live authority
- a proof trace for why the current authorization holds

## Why It Matters

This demo is deliberately adjacent-next rather than over-claimed.

It does not pretend that AETHER is already a finished multi-agent control
plane. It shows something narrower and more believable:

- one governed shared workspace
- one replayable operating history
- one derivation surface for readiness and authority
- one explanation path for skeptical operators

That is enough to make AETHER legible as a product, not only as a kernel.

## Source

The runnable example lives at:

- `crates/aether_api/examples/demo_04_governed_incident_blackboard.rs`

The commercialization source-of-truth document for this exemplar lives at:

- `docs/COMMERCIALIZATION/GOVERNED_INCIDENT_BLACKBOARD.md`
