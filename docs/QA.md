# QA And Regression Suite

This document defines the AETHER quality bar beyond everyday development checks.

It exists for one reason: a structured release needs a repeatable evidence pack, not just confidence that "CI is green."

For the post-v1, internal-first defect-hunting program that runs across admin,
operator, user, and exec perspectives, see `docs/QA_HARDENING_PROGRAM.md`.
That program is complementary to this release gate, not a replacement for it.

## QA Layers

AETHER now verifies itself in eight layers.

1. **Core semantic unit tests**
   Rust crate tests cover the substrate, resolver, rules, runtime, explainability, storage, and API seams.
2. **Semantic acceptance tests**
   The semantic-closure pack proves replay, recursion, stratified negation, bounded aggregation, policy-aware derivation, coordination fencing, sidecar projection, and explanation in one path.
3. **Boundary-client tests**
   Python and Go exercise the stable HTTP seam so the non-Rust boundaries do not silently drift.
4. **Persona hardening sweeps**
   The hardening runner exercises admin, operator, user, and exec workflows and writes triage artifacts without yet blocking release in phase one.
5. **Perturbation and capacity sweeps**
   A combined runner now reuses the persona sweep, full-stack benchmarks, drift checks, and release-mode stress workloads to leave behind one measured capacity snapshot instead of scattered raw outputs.
6. **Pilot launch validation**
   The Windows launch pack generates the operator report, performance report, drift comparison, release-mode API tests, soak suite, and stress suite.
7. **Packaging and documentation checks**
   The release-readiness suite builds the packaged pilot bundle and a GitHub Pages preview bundle from the same tree.
8. **Release-readiness orchestration**
   A single runner now executes the full structured-release contract and writes a saved transcript and summary.

## Standing Development Gate

The day-to-day baseline remains:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test
python -m unittest discover python/tests -v
(cd go && go test ./...)
```

That gate is necessary, but it is not the release gate.

Use it when the question is, “Did this change break the repository?”
Use the full release suite when the question is, “Can we defend this exact tree
as a release candidate?”

Use the hardening suite when the question is, “Where are the next admin,
operator, boundary, and clarity failures hiding?”

Use the perturbation sweep when the question is, “How does the current host
behave under a broader usability-plus-stress pass, and what single-node runway
does that imply?”

## Hardening Sweep

For post-v1 hardening and multi-perspective defect discovery, run:

```text
double-click scripts/run-hardening-sweep.cmd
```

or:

```bash
powershell -ExecutionPolicy Bypass -File scripts/run-hardening-sweep.ps1
```

That suite writes:

- `artifacts/qa/hardening/latest.md`
- `artifacts/qa/hardening/latest.json`
- `artifacts/qa/hardening/promotion/promotion-run.json`
- `artifacts/qa/hardening/promotion/promotion-tracker.json`
- timestamped siblings under `artifacts/qa/hardening/`

Phase-one policy:

- the hardening workflow is non-blocking
- findings are triage artifacts first, blockers second
- promotion into `CI` or release-readiness requires the criteria in `docs/QA_HARDENING_PROGRAM.md`
- the scheduled workflow now updates a standing tracker issue and opens a promotion PR automatically once the next pack satisfies the documented threshold

## Perturbation Sweep

For a broader usability, stress, and scaling pass, run:

```text
double-click scripts/run-perturbation-sweep.cmd
```

or:

```bash
powershell -ExecutionPolicy Bypass -File scripts/run-perturbation-sweep.ps1
```

That suite:

- runs the full persona hardening sweep by default before stressing the runtime
- captures a fresh `full_stack` performance bundle on the chosen host
- checks `core_kernel` and `service_in_process` against their accepted host baselines
- runs the ignored release-mode stress tests, including deeper closure, proof, service, and durable replay scales
- writes one markdown and one JSON summary under `artifacts/performance/perturbation/`

Primary artifacts:

- `artifacts/performance/perturbation/latest.md`
- `artifacts/performance/perturbation/latest.json`
- timestamped siblings under `artifacts/performance/perturbation/runs/`

The capacity section is intentionally a planning aid, not a product claim. It
is a straight-line projection from the measured host and current stress shapes,
useful for prioritization and risk review rather than SLA language.

## Structured Release Gate

For release preparation, run:

```text
double-click scripts/run-release-readiness.cmd
```

or:

```bash
powershell -ExecutionPolicy Bypass -File scripts/run-release-readiness.ps1
```

That suite executes, in order:

1. Rust format check
2. Rust clippy
3. Full Rust workspace tests
4. Python SDK tests
5. Go boundary tests
6. Rust API docs build
7. GitHub Pages preview bundle build
8. Criterion benchmark compile
9. Pilot launch validation pack
10. Packaged pilot bundle build

The runner resolves the accepted performance baseline in this order:

1. `-BaselinePath` for the `core_kernel` suite
2. `artifacts/performance/baselines/<suite>/<host>.json`
3. `fixtures/performance/baselines/<suite>/<host>.json`

## Artifact Pack

The release-readiness runner writes:

- `artifacts/qa/release-readiness/latest.txt`
- `artifacts/qa/release-readiness/latest.md`
- `artifacts/pages-preview-release/`
- `artifacts/pilot/reports/latest.md`
- `artifacts/performance/latest.md`
- `artifacts/performance/latest-drift.md`
- `artifacts/pilot/launch/latest.txt`
- `artifacts/pilot/packages/aether-pilot-service-windows-x86_64.zip`

Those files answer four different release questions:

- did the code pass the cross-language regression gate?
- did the pilot pass the operator and stress gate?
- did the docs and Pages bundle build from the candidate tree?
- did the packaged Windows pilot bundle build from the same candidate?

## CI Automation

The repository now has five quality-automation paths:

- `CI`
  The mainline gate for Rust, Go, Python, pilot launch validation, and pilot package build.
- `Performance Matrix`
  The scheduled/manual host-aware benchmark workflow that captures `full_stack` bundles on GitHub Windows and Ubuntu and then builds a matrix summary.
- `QA Hardening`
  The scheduled/manual multi-perspective hardening workflow with admin, operator, user, and exec artifact packs. It stays diagnostic-only until the tracker proves a pack is ready for promotion.
- `Pilot Validation`
  The scheduled/manual operator validation pack with uploaded pilot artifacts.
- `Release Readiness`
  The manual and tag-triggered structured release suite with QA transcripts, Pages preview, pilot artifacts, and packaged bundle artifacts.

## Failure Policy

Treat any of these as release blockers:

- format, lint, or test failures
- Python or Go boundary-client regressions
- GitHub Pages preview build failure
- benchmark compile failure
- pilot launch validation failure
- packaged pilot bundle build failure

Warning-level drift remains a review point, not an automatic block, but it must be consciously accepted in the release record.

## Current Frontier

The suite is comprehensive for the current single-node pilot release shape, but it is not yet a full product-release system.

Still open:

- promotion of stable hardening subchecks into blocking CI
- public bug-bounty launch posture
- signed artifacts
- multi-platform packaged bundles beyond the current Windows pilot package
- historical benchmark trend storage beyond workflow artifacts
- release promotion workflows tied to versioned changelog and signed provenance
