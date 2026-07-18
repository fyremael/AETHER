# ROADMAP

This document is the forward-looking companion to `docs/STATUS.md`.

`STATUS` answers “what exists now.” `ROADMAP` answers “what should we build
next without disturbing the semantic center.”

## Milestone Spine

The original unrestricted v1 milestone spine is substantially implemented and
recorded in `docs/V1_CLOSEOUT.md`. Policy-aware closure is reopened under the
July 2026 remediation programme.

The active external claim during remediation is:

> Controlled single-node alpha with a real Rust semantic kernel, limited to one
> visibility domain, trusted appenders, and explicitly supported deployment
> boundaries.

| Milestone | Status | Meaning now |
| --- | --- | --- |
| `M0` | Complete | Rust substrate core exists |
| `M1` | Complete | Deterministic resolver core exists |
| `M2` | Complete | Rule compiler and planning exist |
| `M3` | Implemented locally; qualification pending | Recursive runtime, stratified negation, provenance, and bounded aggregation execute inside policy-scoped replay and compilation, including the compatibility evaluator |
| `M4` | Controlled-alpha boundary only | Authenticated service, reports, execution-scoped proof identity, schema-admitted writes, and a verified-TLS transport contract exist; hosted transport and immutable release evidence remain blocked |
| `M5` | Complete for first boundary layer | Go shell and Python SDK are real, but still early ecosystems |

R1-R3 are locally implemented and green across the workspace: policy scope is now
semantic input to replay, compilation, runtime, service documents, federation,
reports, and sidecar cuts. R1 remains evidence-pending rather than externally
closed until its scheduled Postgres parity and performance matrix are captured
by the immutable R4 evidence pipeline. Execution-scoped trace handles and
transactional namespace-schema append admission are now implemented locally;
the R4 schemas, runner, deterministic bundler, cryptographic/API-bound verifier,
policy-only ledger, candidate-bound subject envelopes, canonical-package
qualification flow, promotion-record validator, negative suite, and two-stage
reusable workflow are implemented locally. Independent P0/P1 review of the
focused qualification implementation is complete and merged. Protected
candidate `11380eed81d0690717637a6926ae0087547205c2` passed CI, Supply Chain,
Pages, and Capacity, then failed Release Readiness on first-restart service
latency. The active dependency is the bounded diagnostic and remediation path
in `docs/RESTART_LATENCY_INVESTIGATION.md`. The atomic batch-persistence fix and
ten-process local comparison are green. The execution catalog now batches
manifest/traces atomically, uses WAL/`synchronous=NORMAL`, and avoids a
last-connection checkpoint while preserving database/WAL/SHM backup semantics.
The latest ten-process diagnostic and five local baseline/current comparisons
pass the unchanged gate. Hosted PR validation is pending, then a new protected
candidate must pass operational readiness, bundle verification, the dependent
verdict, and a fresh independent verdict.

The roadmap is no longer about proving the kernel can exist. Its immediate job
is to repair the correctness and claim boundaries before widening resumes.

## Current Planning Rule

Every next-step decision should preserve three properties:

- exact local truth stays exact
- derived meaning stays replayable and explainable
- outer layers do not quietly redefine inner semantics

If a proposed feature weakens one of those, it is the wrong next step.

The July 2026 review record is
`docs/COMPREHENSIVE_AUDIT_2026-07-09.md`, with supporting detail in
`docs/V2_EXTERNAL_REVIEW.md`. `docs/REMEDIATION_PROGRAMME.md` is the binding
execution sequence. Feature broadening that touches policy, service execution,
append, proof identity, or release claims stays frozen until its prerequisite
programme gate is green.

## Active Tracks

### 1. Post-v1 QA hardening and defect intake

Focus:

- run persona-based sweeps across admin, operator, user, and exec perspectives
- keep the first pass internal-first and diagnostic before making it blocking
- mature the bug/spec-gap/usability-gap/security intake posture
- run measured perturbation and capacity sweeps so scaling discussions stay tied to host evidence rather than intuition
- promote only the most stable hardening checks into `CI` and release-readiness
- use GitHub workflow artifacts, tracker issues, and promotion PRs to make that promotion path explicit rather than memory-based

Non-goal:

- do not launch a paid public bug bounty before the private disclosure path, evidence discipline, and promotion rules are calm enough to support it

### 2. Post-pilot service hardening

Focus:

- deepen the new status/reload/backup surfaces into broader operational discipline
- complete the Service v2 design-partner path: namespace-aware service isolation, tagged storage config, optional Postgres authoritative journal deployments, and container smoke evidence without changing kernel semantics
- keep deployment and upgrade discipline coherent as the operator surface grows
- extend cut-diff and proof surfaces without widening beyond exact pilot semantics
- add longer-duration soak and recovery evidence around the now-hardened bundle

### 3. Distributed-truth execution

Focus:

- keep the single-host replicated authority-partition prototype on an evidence path: restart-safe metadata reload, manual promotion, follower replay, stale-epoch fencing, lag/degraded status, and divergent-prefix rejection before broader consensus machinery
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

- do not treat the reopened policy-aware closure as ordinary ergonomics work;
  finish the R1 correctness gate first

### 5. Operational evidence and release discipline

Focus:

- historical benchmark trend storage
- repeated perturbation/capacity evidence across hosts and time
- mature the new capacity tracker until node-class, ceiling, and envelope drift are visible without manual artifact archaeology
- stronger release promotion evidence
- keep controlled design-partner alpha as the active target until R7
- replace authored/path-based readiness with immutable exact-candidate evidence
- obtain exact-SHA hosted evidence from the implemented CycloneDX,
  vulnerability/license/code/secret gates and verify package/SBOM attestations
  plus repository protection settings
- keep the successful Supply Chain artifact as the canonical package; never
  rebuild candidate bytes inside Release Readiness
- promote commercial beta only from a generated immutable record whose official
  and independent verdict bytes are identical and passed
- keep GA at `0/4` until support/incident posture, multi-platform distribution,
  signed promotion, and distributed-truth qualification pass separately

### 6. Product legibility and design-partner packaging

Focus:

- keep the front door anchored in operator questions and practical utility before kernel jargon
- package the AI support resolution desk as the flagship ML-facing working app pack
- keep the governed incident blackboard as the broader governed-workspace reference pattern
- make adjacent-next agentic stories concrete without widening beyond current proof

## What Is Deliberately Not The Immediate Roadmap

Not every desirable capability should be pulled forward.

Deliberately not the near-term center:

- a broad multi-tenant platform story
- replicated sidecar control planes before replicated authority partitions
- Postgres as a SQL rule engine, derived-state authority, or sidecar catalog backend in Service v2
- post-v1 DSL flourish for its own sake
- feature breadth that outruns explainability or replay discipline

## Near-Term Decision Order

If the team needs a practical ordering, use this one:

1. `R0`: contain claims and freeze scope
2. `R1`: make policy semantic input and prove noninterference
3. `R2`: execution-scoped handles implemented locally; immutable candidate evidence remains pending
4. `R3`: transactional namespace-schema append admission implemented locally; immutable candidate evidence remains pending
5. `R4`: immutable exact-candidate evidence implemented locally with signed
   provenance and live run/job/artifact verification; first green official run pending
6. `R5`: supply-chain, transport, concurrency isolation, capability-negotiated
   client migration, operational automation, and service resource controls are
   implemented locally; complete hosted exact-candidate evidence remains pending
7. `R6`: responsibility crates and executable-plan ownership implemented
   locally; preserve the compatibility facade until migration evidence permits removal
8. `R7`: localize and remediate first-restart latency without changing the gate,
   then requalify a new selected commercial-beta candidate only after independent bundle verification

Distributed-truth widening, DSL ergonomics, and broad product expansion remain
behind this sequence unless a separate accepted ADR proves they do not touch a
frozen contract.
