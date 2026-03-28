# V1 Closeout

- Status: Closed for the v1 single-node semantic thesis
- Date: 2026-03-28
- Scope: full v1 single-node semantic closure plus the launch-ready
  design-partner pilot

## What This Document Closes

This document closes the repository's v1 claim in its explicitly narrow form:

- append-only journal truth
- deterministic `History`, `Current`, and `AsOf`
- schema-aware resolution
- recursive derivation through SCC planning and semi-naive execution
- stratified negation
- bounded aggregation in the current v1 slice
- provenance-bearing derived tuples and explanation
- policy-aware derivation and reporting
- sidecar subordination on the current single-node path
- pilot-grade coordination semantics and authenticated service boundaries

It does not close the broader platform horizon.
That later work remains real, but it is post-v1 work rather than unfinished
kernel truth.

## Acceptance Closure Against The Original Kernel Bar

The original library-level acceptance target is now satisfied:

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

The minimum release-candidate gate for this closeout is the structured
release-readiness suite:

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

The correct external wording for this closeout is:

- **full v1 single-node semantic kernel**
- **launch-ready design-partner pilot**

The wrong wording is broader than the evidence, for example:

- production-complete platform
- general multi-agent operating system
- finished distributed control plane

## What Moves To Post-v1

After this closeout, the next roadmap center is:

- post-pilot service hardening
- distributed-truth execution beyond the current prototype
- post-v1 language and runtime ergonomics
- stronger operational evidence and release discipline
- product legibility and design-partner packaging

Those tracks are already governed in `docs/ROADMAP.md`.

## Release Recommendation

The repository is now in a state that can be honestly tagged and discussed as
v1 for the single-node semantic thesis, provided the exact candidate tree
passes the documented release-readiness and CI gates.

This document closes the semantic and governance question.
Future work should widen from this point without pretending the scope was
larger than it was.
