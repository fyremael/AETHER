# Tuple Facade Architecture

The tuple facade gives Python users a friendly coordination API while keeping
AETHER's Rust semantic kernel authoritative.

## Intent

Researchers and agent authors often want a Linda-like surface:

- `out(...)` publishes work, evidence, artifacts, or requests.
- `read(pattern)` inspects visible open tuples.
- `in_(pattern, owner=...)` claims work.
- `complete(claim, result=...)` records an outcome.

AETHER must not model `in` as deletion. Deletion destroys the very properties
that make AETHER valuable: replay, provenance, ownership history, stale-output
fencing, and explanation.

## Semantic mapping

| Facade call | AETHER meaning |
| --- | --- |
| `out(fields, payload, metadata)` | Append a source fact describing an open tuple envelope. |
| `read(pattern)` | Query kernel-derived visible/open tuples under policy. |
| `in_(pattern, owner)` | Append a claim/lease event for one visible tuple. |
| `release(claim)` | Append a release event for the active claim. |
| `complete(claim, result)` | Append a completion event tied to the active claim epoch. |
| `explain(tuple_id)` | Ask the kernel for provenance and rule trace. |

## Kernel rule pack still needed

The HTTP backend intentionally refuses to resolve reads and claims until the
kernel owns these derived predicates:

- `tuple_open(id, fields, payload, metadata)`
- `tuple_claim(id, owner, lease_epoch, expires_at)`
- `tuple_released(id, claim_id, reason)`
- `tuple_completed(id, claim_id, result)`
- `tuple_visible(id)`
- `tuple_owned_by(id, owner, lease_epoch)`
- `tuple_fenced(id, reason)`

This should be implemented as an AETHER document/rule pack or as native Rust API
endpoints. Python should remain a client boundary.

## Why this matters

The facade lets notebooks and agents use a simple coordination idiom while the
system retains the stronger AETHER guarantees: durable history, deterministic
replay, policy-aware visibility, and explainable outcomes.
