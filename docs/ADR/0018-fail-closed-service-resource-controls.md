# ADR 0018: Service Resource Controls Fail Closed Before Semantic Mutation

## Status

Accepted

## Context

The service bounded its global blocking executor and execution stores, but it
did not consistently bound request size, DSL/runtime work, namespace queues,
result delivery, or request rate. A conventional timeout that abandons a
started blocking task would be unsafe: the caller could receive failure while
the task continued mutating authoritative state.

## Decision

- Enforce request-body, document, rule, runtime-iteration, derived-tuple, page,
  rate, global-worker, per-namespace queue, audit-queue, and execution-retention
  limits.
- Publish effective controls through service status and negotiate
  `resource_limits_v1` plus `pagination_v1` with first-party clients.
- Serialize semantic operations within a namespace, bound its queue, and keep
  independent namespace admission so one saturated namespace cannot consume
  another namespace's queue.
- Time out only work waiting to start. Never abandon a started synchronous
  operation; allow it to complete and return its real outcome.
- Validate bounds before authority or execution metadata can be partially
  committed. Return structured, request-correlated errors and write an audit
  outcome for every authenticated rejection.
- Require rate limiting at both the backend and any supported trusted HTTPS
  ingress; direct non-loopback backend access remains unsupported.

## Consequences

- Overload and oversized work fail predictably without partial semantic
  mutation.
- Same-namespace execution remains deterministic while independent namespaces
  retain separate queue capacity.
- Operation timeout means queue/admission timeout, not arbitrary cooperative
  cancellation of a running kernel evaluation.
- Execution and trace stores remain deliberately finite; evicted handles are
  tombstoned rather than allowed to alias future executions.
- The packaged pilot exposes fixed defaults. Dynamic quota administration and
  adaptive tenant budgets require a later contract.
