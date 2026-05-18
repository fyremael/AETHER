# AETHER Tuple Facade

This package adds a small Pythonic tuple-space facade for research agents,
notebooks, and demos.

It is intentionally a facade over AETHER semantics, not a second semantic
kernel. The key rule is:

> Linda-style `in(pattern)` becomes a leased claim, not destructive deletion.

That preserves append-only replay, provenance, stale-result fencing, ownership,
and explanation.

## Quick start

```python
from aether_tuple_facade import InMemoryBackend, TupleSpace

space = TupleSpace(InMemoryBackend())
space.out("task", "case-501", "draft-response", payload={"priority": 7})
claim = space.in_(("task", "case-501", None), owner="triage-agent")
if claim:
    space.complete(claim, result={"artifact": "draft://resolution-901"})
```

## Backends

- `InMemoryBackend` is a single-shard harness for tests, notebooks, and demos.
- `AetherHttpBackend` is deliberately incomplete for read/claim/complete until
  those operations are backed by AETHER kernel rules or service endpoints.

## Test

```bash
PYTHONPATH=python python -m unittest python/tests/test_tuple_facade.py -v
```

## Demo

```bash
PYTHONPATH=python python python/examples/tuple_facade_support_resolution_demo.py
```
