# V1 Closeout

- Status: Reopened in part; immutable qualification remains unsatisfied
- Date: 2026-03-28
- Reopened: 2026-07-09 following the comprehensive audit
- Scope: historical v1 acceptance record, now limited to the unrestricted
  single-node kernel slice

The active external claim is:

> Controlled single-node alpha with a real Rust semantic kernel, limited to one
> visibility domain, trusted appenders, and explicitly supported deployment
> boundaries.

The reproduced policy, explanation-identity, append-admission, and evidence
defects are recorded in `docs/COMPREHENSIVE_AUDIT_2026-07-09.md`. Their binding
repair order and requalification gates are in `docs/REMEDIATION_PROGRAMME.md`.

## What This Document Closes

This document records the repository's original v1 closeout. The following
items remain supported for the unrestricted library slice:

- append-only journal truth
- deterministic `History`, `Current`, and `AsOf`
- schema-aware resolution
- recursive derivation through SCC planning and semi-naive execution
- stratified negation
- bounded aggregation in the current v1 slice
- provenance-bearing derived tuples and explanation
- policy-scoped replay, compilation, runtime, reporting, and explanation
- execution-scoped service proof handles with durable replay metadata
- sidecar subordination on the current single-node path
- pilot-grade coordination semantics and authenticated service boundaries

The local R1 and R2 implementations repair policy-aware semantics and proof
identity, but this historical closeout still does not promote those claims:
R3 append admission is implemented locally. R4's immutable evidence contracts,
runner, deterministic bundle, verifier, negative tests, and reusable workflow
are also implemented locally, but the required official run and independent
downloaded verification remain open. The repaired contracts are therefore not
yet a release claim.

## Acceptance Closure Against The Original Kernel Bar

The original unrestricted library-level acceptance target is satisfied:

- schema with attribute merge classes exists
- datoms append to in-memory and durable journals
- current state materializes deterministically
- `AsOf` replay is exact
- the DSL parses the current canonical v1 program surface
- rules are safety-checked and stratified
- recursive SCCs are compiled explicitly
- semi-naive closure executes to a fixed point
- derivation traces are returned and explainable

The semantic compliance record for that claim is:

- `docs/SEMANTIC_COMPLIANCE_MATRIX.md`

## Governing Evidence

The closeout case rests on five evidence layers:

1. Governing scope documents
   - `README.md`
   - `docs/STATUS.md`
   - `docs/KNOWN_LIMITATIONS.md`
2. Spec-to-implementation mapping
   - `docs/SEMANTIC_COMPLIANCE_MATRIX.md`
3. Release and QA contract
   - `docs/QA.md`
   - `docs/PILOT_LAUNCH.md`
4. Implemented architecture decisions
   - `docs/ADR/0001-authority-partitions-and-federated-cuts.md`
   - `docs/ADR/0002-governed-incident-blackboard-is-demo-packaging.md`
   - `docs/ADR/0003-rust-is-mainline-kernel-language.md`
   - `docs/ADR/0004-aether-dsl-is-canonical-semantics-surface.md`
   - `docs/ADR/0005-recursion-compiles-through-scc-and-semi-naive-execution.md`
   - `docs/ADR/0006-go-is-a-shell-not-the-core-runtime.md`
   - `docs/ADR/0007-sidecars-remain-subordinate-to-semantic-control.md`
5. Repeatable validation paths
   - `.github/workflows/ci.yml`
   - `.github/workflows/pilot-validation.yml`
   - `.github/workflows/release-readiness.yml`

## Release And Validation Contract

The historical closeout used the structured release-readiness suite below.
During remediation it is diagnostic only; it cannot qualify a release until R4
binds every required result to one exact candidate:

```text
powershell -ExecutionPolicy Bypass -File scripts/run-release-readiness.ps1
```

That suite is expected to produce:

- `artifacts/qa/release-readiness/latest.txt`
- `artifacts/qa/release-readiness/latest.md`
- `artifacts/pages-preview-release/`
- `artifacts/pilot/reports/latest.md`
- `artifacts/performance/latest.md`
- `artifacts/pilot/launch/latest.txt`
- `artifacts/pilot/packages/aether-pilot-service-windows-x86_64.zip`

The routine repository gate remains:

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test
python -m unittest discover python/tests -v
(cd go && go test ./...)
```

## Non-Claims

This closeout does not claim:

- distributed consensus closure
- multi-host replication closure
- multi-tenant service closure
- a general multi-agent control plane
- a stable TupleSpace or blackboard API as the product contract
- signed-artifact release infrastructure
- mature post-v1 DSL ergonomics

Those are intentionally outside the v1 bar.

## Operational Release Language

The only current external wording authorized by R0 is:

> Controlled single-node alpha with a real Rust semantic kernel, limited to one
> visibility domain, trusted appenders, and explicitly supported deployment
> boundaries.

The wrong wording is broader than the evidence, for example:

- production-complete platform
- general multi-agent operating system
- finished distributed control plane
- full policy-aware v1 closure
- selected commercial beta

## What Moves To Post-v1

The remediation programme now precedes the former post-v1 roadmap tracks:

- post-pilot service hardening
- distributed-truth execution beyond the current prototype
- post-v1 language and runtime ergonomics
- stronger operational evidence and release discipline
- product legibility and design-partner packaging

Those tracks are already governed in `docs/ROADMAP.md`.

## Release Recommendation

Do not use this historical record to qualify a new release or restore the
policy-aware v1 claim. Requalification requires the ordered R1-R5 gates and one
independently verified exact-candidate evidence bundle at R7. Until then, the
controlled-alpha statement above is the complete release recommendation.
