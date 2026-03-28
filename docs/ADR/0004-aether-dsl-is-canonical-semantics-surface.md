# ADR 0004: The AETHER DSL Is The Canonical Semantics Surface

- Status: Accepted
- Date: 2026-03-28
- Deciders: AETHER maintainers
- Related:
  - `SPEC.md`
  - `RULES.md`
  - `IMPLEMENTATION_DECISION.md`
  - `docs/ARCHITECTURE.md`

## Context

AETHER needs one canonical surface for expressing:

- schema
- facts
- queries
- rules
- temporal views
- policy and visibility annotations

Without that center, the semantic model drifts toward whichever host-language
API happens to be most convenient at the moment. In practice that would let:

- Rust builder helpers
- Go wrappers
- Python notebooks
- HTTP request shapes

become competing semantic authorities.

That would make the system harder to reason about, harder to document, and
easier to accidentally split into incompatible variants.

## Decision

The AETHER DSL is the canonical semantics surface.

That means:

- the governing model for rule/query/schema meaning is expressed in DSL terms
- host-language APIs may construct or submit equivalent structures, but they do
  not redefine semantics
- service and SDK interfaces remain transport and ergonomics layers around the
  DSL-governed model

The goal is not to forbid host-language helpers.
The goal is to prevent semantic collapse into host-language-only rule
authoring.

## Consequences

Positive:

- one semantic center can govern documentation, tests, examples, and
  implementation
- the same rule/query meaning can be defended across Rust, Go, Python, and HTTP
- future ergonomics work can widen without creating multiple dialects of truth

Tradeoffs:

- parser/compiler quality becomes a first-class concern instead of an optional
  convenience
- some host-language ergonomics must wait until they can be expressed without
  changing meaning
- boundary teams cannot treat transport payloads as a shadow language

## Rejected Alternatives

### Host-language callbacks as the primary rule model

Rejected because they bury semantics inside arbitrary code and prevent a clean,
portable, replayable rule surface.

### HTTP payloads as the product-semantic center

Rejected because transport contracts should not dictate the internal semantic
model.

### Independent DSLs per boundary language

Rejected because it would produce multiple centers of truth instead of one
canonical semantics surface.
