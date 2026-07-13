# Python Boundary

This directory now contains the first real AETHER Python SDK surface.

Client preflight requires `resource_limits_v1` and `pagination_v1`. Use
`history_page`, `run_document_page`, and `resolve_trace_handle_page` for bounded
results; do not fall back to an unbounded or tuple-ID endpoint after a typed
limit failure.

Scope for Python in v1:

- fixture builders
- notebook helpers
- benchmark runners
- high-level API clients

Implemented today:

- `aether_sdk.AetherClient`, a broader HTTP client for the stable Rust service boundary

Call `client.require_capabilities(...)` before semantic operations. The notebook
helper does this automatically for the current trace-handle, schema-ref,
append-receipt, and structured-error contracts. Explanation never falls back to
a tuple ID; use the execution receipt's trace handle.
- typed request and data models in `aether_sdk.models`
- fixture builders for datoms, policy contexts, artifacts, and vectors in `aether_sdk.fixtures`
- Colab-friendly onboarding notebooks in `python/notebooks/` over the authenticated pilot boundary
- a flagship ML-facing support-resolution notebook over the live HTTP boundary
- an M6 operating-proof notebook for status, reports, cut diffs, audit context, and trend artifacts
- live integration coverage against `crates/aether_api/examples/http_kernel_service.rs`
- policy-aware document execution plus sidecar artifact/vector calls, with authenticated services treating request policy as a narrowing control over token-granted visibility rather than an escalation path
- explain calls now participate in the same effective-policy contract as document execution on authenticated services
- sidecar flows that anchor artifact/vector registrations to real journal cuts before semantic search

Start the interactive learning path at `python/notebooks/README.md`.

Current test command:

```bash
python -m unittest discover python/tests -v
```

Out of scope:

- shadow implementations of kernel semantics
- authoritative rule or resolver logic
- a mature async or notebook-first SDK surface
