# AI Support Resolution Desk

This document is the source-of-truth product narrative for the **AETHER AI
support resolution desk** exemplar.

It exists to answer the next question focus groups are now asking:

**What does AETHER look like in an end-user ML workflow people already care
about?**

The answer should land before anyone has to care about kernel vocabulary.

## Plain-Language Opener

The easiest way to understand AETHER for ML-oriented buyers is as the fabric
underneath a governed case desk.

Support agents, triage models, retrieval sidecars, and human leads share one
workspace for customer issues, retrieved evidence, candidate resolutions,
approvals, assignments, escalations, and fenced stale recommendations. The desk
does not just list activity. It can show what cases are active now, which
resolution is actually ready, which recommendation is current versus stale, who
owns the case now, and why the chosen path is true.

## The Ordinary Failure Mode

Without a shared semantic center, AI-assisted support work fragments quickly:

- tickets hold one version of the issue
- model suggestions live in another system
- retrieved knowledge sits beside the case instead of inside the decision path
- assignments and approvals drift through chat
- escalations happen on stale assumptions

That is where clarity breaks. Teams stop asking which resolution is right and
start asking what the system even believed.

## The AETHER Answer

AETHER turns that fragmented workflow into one governed operational surface.

In business terms, the support resolution desk provides:

- **governed shared memory**
  customer issues, retrieved evidence, candidate resolutions, and assignments
  live in one replayable operating history
- **derived next action**
  the system works out which resolution or escalation is truly ready from
  evidence, approvals, suppressions, confidence, and dependency state
- **controlled handoff**
  assignment and authority change through explicit semantic state instead of
  side-channel coordination
- **replay**
  operators can inspect what the desk believed at an exact prior cut of history
- **proof**
  the desk can explain why a resolution was selected, escalated, or fenced

## What Lives On The Desk

Client-facing desk objects:

- customer issue
- retrieved evidence
- candidate resolution
- approval
- escalation
- claim / assignment
- stale or fenced recommendation

In the current exemplar:

- inbound support cases are appended as journal facts
- sidecar artifact and vector search bring back prior-case or runbook evidence
- planner logic publishes candidate resolutions and escalation options
- AETHER derives which path is ready
- ownership moves through lease-backed assignment state
- stale recommendation attempts stay in history but are fenced in the live view

## The Five Support-Team Questions

This exemplar is meant to answer five questions cleanly:

1. **What cases are active now?**
   Which customer issues and retrieved evidence are live on the desk?
2. **What action is actually ready?**
   Which resolution or escalation has the required evidence, approvals, and
   dependency state?
3. **Which recommendation is current versus stale?**
   Which suggestion still matches live ownership and which one is now fenced?
4. **Who owns the case now?**
   Which worker, lead, or agent currently holds valid authority?
5. **Why was this resolution chosen?**
   Which evidence, approvals, and dependency facts made the selected path true?

## Why This Exemplar Works

The AI support resolution desk is a strong ML-facing front door because it
keeps the story concrete:

- support buyers can picture the workflow immediately
- retrieval and model assistance stay relevant without becoming hand-wavy
- the current pilot proof already supports the key semantics underneath
- the agentic direction becomes believable without pretending it is already a
  finished platform

This is why the support desk is the recommended **working app pack** for
ML-oriented design-partner conversations.

## Truth Boundary

This exemplar is intentionally disciplined.

It does **not** mean:

- AETHER is already a finished ML operations platform
- vector search is the authority layer
- AETHER is now a turnkey autonomous support SaaS
- the support desk introduces a new stable product API separate from current
  kernel and HTTP surfaces

It **does** mean:

- the current kernel can already support a governed AI-assisted support story
- retrieval can re-enter operations as evidence while staying subordinate to
  semantic control
- the support desk is working app packaging over current proof, not a new
  semantic claim

## How To Present It

Use this order in live conversations:

1. show the active cases
2. show the retrieved evidence
3. show the candidate resolutions
4. show which resolution is actually ready
5. show who owns the case now
6. show the prior `AsOf` cut and the fenced stale recommendation
7. show the proof trace

That sequence keeps the support question first and the kernel language second.

## Run The Exemplar

Documentation walkthrough:

- `examples/demo-05-ai-support-resolution-desk.md`

Runnable example:

- `cargo run -p aether_api --example demo_05_ai_support_resolution_desk`

Windows operator path:

- `scripts/run-demo-05.cmd`

Notebook path:

- `python/notebooks/06_ai_support_resolution_desk.ipynb`

Related proof anchors:

- `docs/COMMERCIALIZATION/GOVERNED_INCIDENT_BLACKBOARD.md`
- `docs/COMMERCIALIZATION/CANONICAL_USE_CASES.md`
- `docs/PILOT_COORDINATION.md`

## Technical Appendix

Only after the story lands, map the desk language back to AETHER terms.

| Desk language | AETHER meaning |
| --- | --- |
| governed case desk | semantic coordination fabric over one append-only history |
| active case | extensional facts visible at `Current` or `AsOf` |
| retrieved evidence | sidecar artifact/vector search projected back into the live reasoning path |
| candidate resolution | a published resolution or escalation entity plus its semantic state |
| actually ready | a derived fact closed over evidence, approvals, suppressions, confidence, and dependencies |
| who owns the case now | live lease-backed assignment authority |
| stale recommendation | a recommendation or assignment attempt preserved in history but no longer authorized in live state |
| why was this chosen | provenance-bearing explanation trace |

Memory note:

- for technical audiences, the retrieval layer is implemented through
  journal-anchored artifact and vector sidecars
- for client-facing conversations, the preferred term is **retrieved evidence**
  feeding a governed case desk

That distinction matters because the sidecar is useful, but it is not the
authority layer.
