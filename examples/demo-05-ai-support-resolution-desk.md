# Demo 05: AI Support Resolution Desk

This is the flagship ML-facing application pack for AETHER.

It is designed for focus-group evaluators, design partners, and buyers who want
to see a relevant end-user workflow before they learn kernel vocabulary.

The story is simple:

- a customer issue lands on one governed case desk
- retrieval sidecars surface prior-case or runbook evidence
- planner logic publishes candidate resolutions and escalation options
- AETHER derives which path is actually ready
- assignment authority moves through a semantic handoff
- stale recommendation attempts are fenced
- operators can replay a prior cut and inspect the proof trail for the chosen path

## Why This Demo Exists

The current repository already proves replay, readiness, authority, handoff,
sidecar memory, and explanation.

What ML-oriented readers still struggle with is relevance. They hear semantic
kernel, recursive closure, or provenance before they hear a workflow they would
actually buy.

Demo 05 flips that order.

It starts with an AI-assisted support desk, then shows that AETHER is the
governed fabric underneath it.

## Story

We model one high-priority support case with two candidate responses.

- `case/501` is a duplicate-charge complaint after plan migration
- one prerequisite support step is already complete
- a sidecar search retrieves a migration-credit runbook as live evidence
- `resolution/901` is the preferred path: `apply-migration-credit`
- `resolution/901` requires retrieved evidence, approval, clear suppression, and a complete prerequisite
- `resolution/902` is the fallback path: `escalate-to-billing-specialist`
- `resolution/902` is approved but suppressed, so it stays off the live ready path
- `triage-agent` first owns the case at epoch `1`
- later the assignment moves to `lead-ana` at epoch `2`
- stale assignment attempts remain in history but are fenced in the live semantic view

## Run It

Simplest Windows operator path:

```text
double-click scripts/run-demo-05.cmd
```

Technical path:

```bash
cargo run -p aether_api --example demo_05_ai_support_resolution_desk
```

The Windows launcher writes a timestamped report to `artifacts/demos/demo-05/`.

## Screen-Share Flow

Use the walkthrough in this order:

1. show the active support case
2. show the retrieved evidence and candidate resolutions
3. show which resolution is actually ready at `AsOf(e20)`
4. show who owns the case now at `Current`
5. show the prior owner at `AsOf(e23)` and the fenced stale recommendations at `Current`
6. show the proof trace for the current selected resolution

That ordering keeps the buyer question first:

- what is active
- what evidence matters
- what is ready
- who owns it
- what changed
- why the answer is true

## What You Should See

The demo prints:

- the support-case history as append-only semantic facts
- the active case desk view
- the retrieved support evidence returned by the sidecar search
- the published candidate resolutions and their approval / suppression state
- the single ready resolution at `AsOf(e20)`: `apply-migration-credit`
- the single current selected resolution at `Current`: `apply-migration-credit` owned by `lead-ana`, epoch `2`
- the prior selected resolution at `AsOf(e23)`: `apply-migration-credit` owned by `triage-agent`, epoch `1`
- the fenced stale assignment attempts that no longer match live ownership
- a proof trace for why the current selected path holds

## Why It Matters

This demo is deliberately disciplined.

It does not pretend that AETHER is already a general ML orchestration platform.
It shows something narrower and more believable:

- one governed support desk
- one replayable operating history
- one evidence-to-action path where retrieval stays subordinate to semantic control
- one explanation surface for skeptical operators and buyers

That is enough to make AETHER relevant to ML operations without over-claiming.

## Source

The runnable example lives at:

- `crates/aether_api/examples/demo_05_ai_support_resolution_desk.rs`

The commercialization source-of-truth document for this exemplar lives at:

- `docs/COMMERCIALIZATION/AI_SUPPORT_RESOLUTION_DESK.md`
