# ADR 0016: Capability-Negotiated API Migration

## Status

Accepted

## Context

AETHER must replace process-local tuple explanation references and implicit
schema admission without letting independently deployed clients silently guess
which contract a server implements. Endpoint success alone is insufficient:
older and newer payloads can look superficially compatible while carrying
different identity and validity semantics.

## Decision

The service publishes explicit capability flags in its status response. Rust
exposes typed negotiation helpers and demo service startup prints the set. Go,
Python, TUI, CLI report, and notebook surfaces preflight the flags needed by
their semantic operations and fail closed if any are absent.

HTTP failures retain a human-readable `error` string for one transition and add
stable `code`, per-request `request_id`, and object-valued `details`. The same
request ID is returned in `X-Aether-Request-Id` on success and failure.

No first-party client may fall back from an execution-scoped trace handle to a
tuple ID. The legacy tuple endpoint and omitted append schema references remain
temporarily callable only with explicit audit telemetry. Their removal is
evidence-gated as defined in `docs/API_CLIENT_MIGRATION.md`.

## Consequences

- Client/server skew becomes an explicit typed failure.
- Human diagnostics remain readable while automation uses stable codes.
- Request correlation cannot be confused with proof or authority identity.
- Legacy use is measured before removal rather than inferred from declarations.
- First-party integrations must update capability preflights in the same change
  as any future boundary contract.

## Rejected alternatives

- Content negotiation by endpoint or HTTP status alone: it cannot express the
  semantic identity guarantees behind a superficially similar response.
- Automatic tuple-ID fallback: it reintroduces explanation aliasing.
- Immediate removal without telemetry: it makes remaining consumers invisible
  and turns migration into an outage-driven process.
