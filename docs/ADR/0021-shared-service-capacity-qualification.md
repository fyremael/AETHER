# ADR 0021: Shared-service capacity qualification

- Status: Accepted
- Date: 2026-07-19

## Context

The first protected capacity reports created a complete in-process HTTP service
inside every concurrency worker. They therefore measured independent service
instances rather than contention against the supported single-node service.
The planner also treated the first adjacent throughput gain below 15 percent on
the 4-vCPU calibration runner as a hard concurrency cap for projected 8-64 vCPU
node classes. That plateau moved from 8 to 16 between otherwise successful
runs, while both reports showed low raw 32-worker p95 latency.

Release Readiness correctly rejected candidate
`64797af68261bc72618487e47f8f44fae3a11d28` because its derived M-class
envelope remained below the 32-worker policy. The historical 32-worker bytes
could not be reinterpreted as proof because they described the wrong service
boundary.

## Decision

Each concurrency point constructs exactly one HTTP service and shares its
router and state across all worker runtimes. Service and worker setup complete
before the measured interval. Every rung records total, successful and failed
operations, 503 saturation responses, distinct setup and measurement durations,
throughput, p95 and p99 latency, and bounded failure diagnostics.

The node-class recommendation is the largest measured rung whose projected p95
is within that class target and whose raw run has complete success accounting,
zero failures and zero 503s. The first marginal-throughput plateau remains a
diagnostic efficiency and scale-out signal; it is not projected as an unscaled
hard cap.

Commercial-beta policy remains at a minimum raw 32-worker rung, maximum 2,000
ms p95, zero errors and zero 503s. The rung must retain at least 12 operations
per worker across the health, status, history, coordination report,
coordination-delta and explanation endpoints. Release qualification requires
exactly one such raw point from one shared service and checks its operation and
duration accounting. The candidate-bound capacity subject carries the raw
concurrency pack, and the subject verifier recomputes acceptance instead of
trusting the derived envelope or an authored pass flag. Official verification
redownloads the capacity artifact, requires the embedded raw pack to equal
those source bytes, requires the subject's policy copy to equal the validated
bundle policy, and uses canonical JSON byte comparison so booleans cannot
masquerade as numeric zeroes.

## Consequences

- Existing capacity reports do not satisfy the new subject contract and cannot
  be promoted or rewritten into compliance.
- A failed, missing, duplicated, inconsistently accounted, multi-service,
  over-latency, erroring or saturated policy rung blocks bundle assembly.
- Calibration-host throughput flattening can still trigger efficiency and
  partition/federation investigation without falsely constraining larger node
  classes.
- Passing this model locally does not qualify beta. A new protected candidate
  still needs exact-SHA CI, Supply Chain, Pages, Capacity Planning, Release
  Readiness, clean-room byte verification and independent review.
