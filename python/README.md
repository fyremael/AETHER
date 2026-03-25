# Python Boundary

This directory now contains the first real AETHER Python SDK surface.

Scope for Python in v1:

- fixture builders
- notebook helpers
- benchmark runners
- high-level API clients

Implemented today:

- `aether_sdk.AetherClient`, a broader HTTP client for the stable Rust service boundary
- typed request and data models in `aether_sdk.models`
- fixture builders for datoms, policy contexts, artifacts, and vectors in `aether_sdk.fixtures`
- live integration coverage against `crates/aether_api/examples/http_kernel_service.rs`
- policy-aware document execution plus sidecar artifact/vector calls, with authenticated services treating request policy as a narrowing control over token-granted visibility rather than an escalation path
- sidecar flows that anchor artifact/vector registrations to real journal cuts before semantic search

Current test command:

```bash
python -m unittest discover python/tests -v
```

Out of scope:

- shadow implementations of kernel semantics
- authoritative rule or resolver logic
- a mature async or notebook-first SDK surface
