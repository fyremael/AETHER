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

## CoordinationDesk utility

For operator-facing demos and early agent workflows, use `CoordinationDesk`.
It turns the raw tuple primitives into an evidence-backed work queue.

```python
from aether_tuple_facade import CoordinationDesk, InMemoryBackend, TupleSpace

space = TupleSpace(InMemoryBackend())
desk = CoordinationDesk(space)

runbook = desk.add_evidence(
    "case-501",
    "migration-credit-runbook",
    uri="sidecar://runbook/migration-credit",
)

desk.submit_task(
    "case-501",
    "apply-migration-credit",
    priority=9,
    evidence=[runbook],
)

claim = desk.claim_next(owner="lead-ana", case_id="case-501")
if claim:
    desk.complete_claim(claim, result={"artifact": "draft://resolution-901"})
    print(desk.explain(claim.tuple_id))
```

## Backends

- `InMemoryBackend` is a single-shard harness for tests, notebooks, and demos.
- `AetherHttpBackend` is deliberately incomplete for read/claim/complete until
  those operations are backed by AETHER kernel rules or service endpoints.

## Test

```bash
PYTHONPATH=python python -m unittest python/tests/test_tuple_facade.py python/tests/test_coordination_desk.py -v
```

## Demo

```bash
PYTHONPATH=python python python/examples/tuple_facade_support_resolution_demo.py
PYTHONPATH=python python python/examples/coordination_desk_demo.py
```
