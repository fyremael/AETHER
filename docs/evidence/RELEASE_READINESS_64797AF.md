# Release Readiness failure: `64797af68261bc72618487e47f8f44fae3a11d28`

Candidate `64797af68261bc72618487e47f8f44fae3a11d28` is permanently
disqualified for commercial beta. It must not be rerun, reinterpreted, or used
to author a successful release-evidence bundle.

## Immutable coordinates

- Candidate tree: `9826b2918a772c5a97336aaa416c77ce77576a36`
- Candidate ref: `refs/heads/main`
- CI: run `29687615280`, attempt 1, passed
- Supply Chain: run `29687615310`, attempt 1, passed
- Pages: run `29687615298`, attempt 1, passed
- Capacity Planning: run `29689359042`, attempt 1, passed
- Capacity report artifact: `8443622921`
- Capacity artifact archive SHA-256:
  `c56aaa30a0047a84c876ea9ca1f24b648875217c327989739e18a842821b7279`
- Immutable capacity JSON SHA-256:
  `3f33d60f27f12979f1ad77f110d63088022e96c20fcaed52e91451b0ae037411`
- Release Readiness: run `29691266971`, attempt 1, failed
- Release Readiness failure artifact: `8443896067`
- Failure artifact API size: `50717523` bytes
- Failure artifact archive SHA-256:
  `27d0dd4af7b85dac1875ffb544b0f6121272f4c5d10a838c4a7a05ae2d009a8b`
- Failure artifact expiry: `2026-10-17T14:39:29Z`

## Passed operational bytes

The Release Readiness suite itself passed before candidate-subject construction
failed. These outputs therefore diagnose the failed candidate but do not form
an official bundle or promotion verdict:

- Canonical package SHA-256:
  `37d1693b06871f7219ee8ba8a4c735c32eea7204014dfd57bc5a0ec1a1eea942`
- `release-readiness-evidence-64797af68261bc72618487e47f8f44fae3a11d28-29691266971-1.json`:
  2,714 bytes, SHA-256
  `5ab117b60fece516ba57b532b1453e53a9ad598d1cb93f8a9db97bbbdcbe1999`
- `service-v2-operability-20260719-144538.json`: 4,061 bytes, SHA-256
  `41635aba8f084d8761f082f4cf5d77e3f91f5e29e8695cca214f92cece638383`
- `performance-beta-20260719-144538.json`: 5,598 bytes, SHA-256
  `dc8f36a7c9dc9dc2609aeada41ca7692ff3b49b95b9956344d64d1ec940a13b7`
- `release-readiness-20260719-144538.txt`: 124,671 bytes, SHA-256
  `b9de8e68f96d53b94e0ba653fe88bd7eb1c84d1947968f32af0401416674d7c5`

The readiness manifest reports `status: passed`, the Service v2 proof reports
`beta_ready: true`, and the performance proof reports `beta_ready: true`. The
exact-candidate evidence producer was skipped, so no official evidence bundle
or dependent verdict exists.

## Failure and disposition

Candidate-subject construction failed closed with:

```text
capacity acceptance failed: ['operator_concurrency']
```

The release policy requires the M-class envelope to support at least 32 mixed
operator workers. The candidate's capacity report recommended 16. Its old
32-worker point reported about 525 operations per second with 206.068 ms p95,
but each worker constructed a separate in-process service. Those bytes measure
32 isolated service instances, not 32 workers contending on the one supported
single-node service, so they cannot prove the policy threshold. The old
recommendation was also capped because the 4-vCPU calibration host's first
marginal-throughput plateau occurred at 16.

The planner projected measured p95 latency from that 4-vCPU host to the 16-vCPU
M class, but applied the calibration host's raw one-sample plateau as an
unscaled hard cap. The preceding candidate's independent report produced a cap
of 8 on another 4-vCPU runner even though its 32-worker p95 was 242.978 ms.
That cross-run movement exposes a model-contract defect, not evidence that the
policy threshold passed.

The focused repair keeps the non-waivable 32-worker and 2,000 ms policies
unchanged. It measures one shared service, excludes setup from the measured
interval, records successes, failures, 503 responses, p95/p99 and duration,
and derives the recommendation from the largest raw rung that satisfies
latency and zero-error policy. Throughput saturation remains a diagnostic
efficiency/scale-out signal. The subject verifier recomputes acceptance from
that raw rung rather than trusting the derived envelope. Controlled alpha
remains unchanged. The repair requires independent review, a merge, and an
entirely new protected candidate with new CI, Supply Chain, Pages, Capacity
Planning, Release Readiness, clean-room byte verification, and independent
review.
