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

### 1. Post-pilot service hardening

Focus:

- deepen the new status/reload/backup surfaces into broader operational discipline
- keep deployment and upgrade discipline coherent as the operator surface grows
- extend cut-diff and proof surfaces without widening beyond exact pilot semantics
- add longer-duration soak and recovery evidence around the now-hardened bundle

### 2. Distributed-truth execution

Focus:

- take the current single-host replicated authority-partition prototype toward clearer failover and recovery evidence
- widen durable federated service boundaries only where provenance remains exact
- keep imported-fact widening constrained to provenance-preserving shapes
- delay generalized consensus machinery until the current exact-local-truth model is exhausted

Governing docs:

- `docs/ADR/0001-authority-partitions-and-federated-cuts.md`
- `docs/FEDERATED_TRUTH_IMPLEMENTATION_PLAN.md`

### 3. Post-v1 language and runtime ergonomics

Focus:

- modular authoring and cleaner document composition
- richer explain/query ergonomics
- runtime optimization beyond the now-closed v1 bounded-aggregation slice

Non-goal:

- do not reopen the v1 semantic closure claim unless a real semantic defect is found

### 4. Operational evidence and release discipline

Focus:

- historical benchmark trend storage
- stronger release promotion evidence
- eventually signed artifacts and provenance

### 5. Product legibility and design-partner packaging

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
2. deepen operator-facing proof and reporting
3. execute replicated authority partitions
4. widen distributed truth only where provenance remains exact
5. improve ergonomics and optimization around the already-settled core

The first pass through that order is now in motion: pilot hardening, delta
reporting, replicated authority partitions, and federated HTTP surfaces exist.
The next roadmap pass is to make those new surfaces calmer, more operable, and
better evidenced rather than immediately broader.
