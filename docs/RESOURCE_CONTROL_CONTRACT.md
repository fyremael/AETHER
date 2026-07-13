# Service Resource-Control Contract

Status: implemented locally for R5.6; exact-candidate hosted evidence pending

AETHER rejects resource exhaustion at the service boundary with typed,
request-correlated, audited failures. A rejected operation must not partially
append authority, publish an execution receipt, or retain a trace handle.

## Enforced defaults

The effective values are published by `GET /v1/status` under
`resource_controls`.

| Resource | Default | Failure behavior |
| --- | ---: | --- |
| HTTP request body | 1,048,576 bytes | `413 request_body_too_large` |
| DSL document | 262,144 bytes | `413 resource_limit_exceeded` |
| Rules per document | 512 | `413 resource_limit_exceeded` |
| Runtime iterations | 4,096 | `422 resource_limit_exceeded` |
| Derived tuples | 1,000,000 | `422 resource_limit_exceeded` |
| Queue wait | 30,000 ms | `504 operation_timed_out` |
| Page size | 500 | `413 resource_limit_exceeded` |
| Requests per principal and namespace | 600 per fixed minute | `429 rate_limit_exceeded` with `Retry-After` |
| Global blocking workers | 8 | bounded admission |
| Per-namespace active semantic operations | 1 | deterministic same-namespace order |
| Per-namespace queued operations | 64 | `503 namespace_busy` with `Retry-After` |
| Audit writer queue | 1,024 | visible `audit_write_failed` entry on backpressure |
| Retained executions | 1,024 | evicted handles remain tombstoned and fail typed |

`HttpKernelOptions` can replace the HTTP/runtime/page/rate values and the
worker, namespace-queue, and audit-queue bounds for an embedded deployment.
The packaged pilot currently uses the documented fixed defaults. Auth reload
cannot change these process-lifetime controls.

## Cancellation and atomicity

The cancellation contract is
`cancel_before_start_complete_after_start`.

- Work waiting for a blocking worker may time out. Its closure is never run.
- Once synchronous semantic work starts, the service does not abandon it or
  return a timeout while it may still mutate authority in the background.
  Started work completes atomically and its actual response is returned.
- Document, rule, page, iteration, and tuple limits are checked before the
  affected append/evaluation receipt or trace metadata is persisted.
- Runtime limit failures discard the in-progress derived set. Source authority
  is unchanged.

This is intentionally not asynchronous cancellation of arbitrary Rust code.
Adding cooperative cancellation inside evaluation requires a separate semantic
checkpoint contract and ADR.

## Pagination

Bounded first-party endpoints are:

- `GET /v1/history/page?offset=<n>&limit=<n>`
- `POST /v1/documents/run/page?offset=<n>&limit=<n>`
- `POST /v1/explanations/resolve/page?offset=<n>&limit=<n>`

Responses carry `page` with `offset`, `limit`, `total`, and `next_offset`.
History is policy-projected before `total` is calculated. Trace resolution
still verifies the immutable execution and replay contract before returning a
tuple page. Paged document execution retains the full execution identity but
returns only the requested query rows.

## Supported ingress boundary

The backend limiter is keyed by effective namespace and authenticated
principal identity. It is defense in depth, not a substitute for a trusted
ingress limiter. A supported non-loopback deployment must also rate-limit at
the declared HTTPS ingress and prevent direct backend access. Loopback-only
plaintext remains the supported local path.

## Evidence

Local tests cover body, document, runtime, page, rate, namespace admission,
queue timeout/cancellation, audit backpressure, execution retention, and
authority-unchanged failures. R5 is not qualified until these outcomes are
captured for an immutable candidate and independently verified with the other
R5 subjects.
