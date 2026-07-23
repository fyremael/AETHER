# Coordination Desk Utility

`CoordinationDesk` is a small operator-facing utility layered on the Python
AETHER tuple facade.

It demonstrates concrete utility without weakening the architecture:

- evidence is posted as typed tuples
- work is posted as prioritized task tuples
- operators or agents claim the highest-priority visible task
- completions are recorded as outcome tuples
- explanation follows the tuple event chain

## Why it exists

The low-level tuple facade is intentionally primitive. It proves a semantic
mapping: `out`, `read`, `claim`, `complete`, and `explain`.

The desk shows how that primitive becomes useful in a real workflow: support
resolution, incident response, model-evaluation triage, experiment review, or
any queue where evidence must be attached to action.

## Example

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

## Boundary

`CoordinationDesk` is not intended to become the semantic authority. It is a
convenience layer. The in-memory backend is useful for notebooks and demos; the
same high-level idioms should eventually compile to kernel-backed AETHER rules
for `tuple_visible`, `tuple_owned_by`, `tuple_completed`, and `tuple_fenced`.

## Run

```bash
PYTHONPATH=python python python/examples/coordination_desk_demo.py
PYTHONPATH=python python -m unittest python/tests/test_coordination_desk.py -v
```
