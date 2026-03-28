# ADR 0007: Sidecars Remain Subordinate To Semantic Control

- Status: Accepted
- Date: 2026-03-28
- Deciders: AETHER maintainers
- Related:
  - `SPEC.md`
  - `docs/ARCHITECTURE.md`
  - `docs/KNOWN_LIMITATIONS.md`
  - `docs/SEMANTIC_COMPLIANCE_MATRIX.md`

## Context

AETHER needs artifact and vector sidecars because important operational systems
use documents, reports, logs, and embeddings in addition to semantic facts.

But sidecars are a structural risk if they are allowed to become independent
truth sources for:

- identity
- provenance
- policy
- orchestration state
- replay scope

If that happens, the kernel stops being the authority and turns into a partial
index over behavior defined somewhere else.

## Decision

Sidecars remain subordinate to semantic control.

That means:

- the journal stores references and metadata, not raw blob or dense vector
  payloads inline
- semantic identity, provenance, policy, and orchestration stay anchored in the
  kernel
- sidecar visibility and effect are governed by committed journal state and
  temporal cuts
- sidecar projections back into the rule layer must carry provenance and remain
  replay-compatible

Sidecars may widen operationally, but they do not become a second authority
plane.

## Consequences

Positive:

- artifact and vector memory can be integrated without dissolving semantic
  control
- `Current` and `AsOf` remain meaningful across sidecar-backed workflows
- policy and provenance stay attached to the same governing journal truth

Tradeoffs:

- sidecar APIs cannot act as unconstrained workflow backdoors
- some storage-system conveniences are intentionally rejected if they would
  weaken replay or provenance
- broader distributed sidecar work must stay accountable to the kernel's
  authority boundaries

## Rejected Alternatives

### Sidecars as independent orchestration truth

Rejected because it creates two competing sources of authority.

### Inline blob or embedding storage as the journal default

Rejected because it muddies the datom journal's role and weakens its semantic
discipline.

### Sidecar-first product semantics

Rejected because artifacts and vectors are useful memory surfaces, not the
primary semantic control plane.
