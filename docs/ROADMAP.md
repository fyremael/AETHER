# ROADMAP

This document is the forward-looking companion to `docs/STATUS.md`.

`STATUS` answers “what exists now.” `ROADMAP` answers “what should we build
next without disturbing the semantic center.”

## Milestone Spine

The original v1 milestone spine is now substantially closed and formally
recorded in `docs/V1_CLOSEOUT.md`.

| Milestone | Status | Meaning now |
| --- | --- | --- |
| `M0` | Complete | Rust substrate core exists |
| `M1` | Complete | Deterministic resolver core exists |
| `M2` | Complete | Rule compiler and planning exist |
| `M3` | Complete for v1 | Recursive runtime, stratified negation, provenance, and bounded aggregation are implemented for the v1 slice |
| `M4` | Complete for pilot scope | Stable boundary, authenticated service, reports, and pilot launch workflow exist |
| `M5` | Complete for first boundary layer | Go shell and Python SDK are real, but still early ecosystems |

That means the roadmap is no longer about proving the kernel can exist. It is
about widening the system carefully around a settled semantic core.

## Current Planning Rule

Every next-step decision should preserve three properties:

- exact local truth stays exact
- derived meaning stays replayable and explainable
- outer layers do not quietly redefine inner semantics

If a proposed feature weakens one of those, it is the wrong next step.

## Active Tracks

### 1. Post-v1 QA hardening and defect intake

Focus:

- run persona-based sweeps across admin, operator, user, and exec perspectives
- keep the first pass internal-first and diagnostic before making it blocking
- mature the bug/spec-gap/usability-gap/security intake posture
- promote only the most stable hardening checks into `CI` and release-readiness
- use GitHub workflow artifacts, tracker issues, and promotion PRs to make that promotion path explicit rather than memory-based

Non-goal:

- do not launch a paid public bug bounty before the private disclosure path, evidence discipline, and promotion rules are calm enough to support it

### 2. Post-pilot service hardening

Focus:

- deepen the new status/reload/backup surfaces into broader operational discipline
- keep deployment and upgrade discipline coherent as the operator surface grows
- extend cut-diff and proof surfaces without widening beyond exact pilot semantics
- add longer-duration soak and recovery evidence around the now-hardened bundle

### 3. Distributed-truth execution

Focus:

- take the current single-host replicated authority-partition prototype toward clearer failover and recovery evidence
- widen durable federated service boundaries only where provenance remains exact
- keep imported-fact widening constrained to provenance-preserving shapes
- delay generalized consensus machinery until the current exact-local-truth model is exhausted

Governing docs:

- `docs/ADR/0001-authority-partitions-and-federated-cuts.md`
- `docs/FEDERATED_TRUTH_IMPLEMENTATION_PLAN.md`

### 4. Post-v1 language and runtime ergonomics

Focus:

- modular authoring and cleaner document composition
- richer explain/query ergonomics
- runtime optimization beyond the now-closed v1 bounded-aggregation slice

Non-goal:

- do not reopen the v1 semantic closure claim unless a real semantic defect is found

### 5. Operational evidence and release discipline

Focus:

- historical benchmark trend storage
- stronger release promotion evidence
- eventually signed artifacts and provenance

### 6. Product legibility and design-partner packaging

Focus:

- keep the front door anchored in operator questions and practical utility before kernel jargon
- package the governed incident blackboard exemplar across docs, demo, and Pages surfaces
- make adjacent-next agentic stories concrete without widening beyond current proof

## What Is Deliberately Not The Immediate Roadmap

Not every desirable capability should be pulled forward.

Deliberately not the near-term center:

- a broad multi-tenant platform story
- replicated sidecar control planes before replicated authority partitions
- post-v1 DSL flourish for its own sake
- feature breadth that outruns explainability or replay discipline

## Near-Term Decision Order

If the team needs a practical ordering, use this one:

1. harden the current pilot boundary
2. promote the most stable hardening checks into the mainline gates
3. deepen operator-facing proof and reporting
4. execute replicated authority partitions
5. widen distributed truth only where provenance remains exact
6. improve ergonomics and optimization around the already-settled core

The first pass through that order is now in motion: pilot hardening, delta
reporting, replicated authority partitions, federated HTTP surfaces, and the
new hardening program now exist. The next roadmap pass is to make those
surfaces calmer, more operable, and better evidenced rather than immediately
broader.
