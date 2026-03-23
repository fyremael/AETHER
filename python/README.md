# Python Boundary

This directory now contains the first real AETHER Python boundary client.

Scope for Python in v1:

- fixture builders
- notebook helpers
- benchmark runners
- high-level API clients

Implemented today:

- `aether_sdk.AetherClient`, a minimal HTTP client for the stable Rust service boundary
- live integration coverage against `crates/aether_api/examples/http_kernel_service.rs`
- core document execution calls plus sidecar artifact/vector calls
- sidecar flows that anchor artifact/vector registrations to real journal cuts before semantic search

Current test command:

```bash
python -m unittest discover python/tests -v
```

Out of scope:

- shadow implementations of kernel semantics
- authoritative rule or resolver logic
