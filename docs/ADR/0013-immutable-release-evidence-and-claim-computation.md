# ADR 0013: Immutable Release Evidence and Claim Computation

- Status: Accepted
- Date: 2026-07-12
- Programme gate: `release.evidence_integrity`

## Context

AETHER's previous readiness renderer accepted authored gate statuses and source
path existence. A declaration such as `ready`, a checked-in workflow file, or a
mutable `latest` artifact could therefore look like observed success without
proving that a command ran for the candidate being promoted. This is unsuitable
for semantic correctness, package identity, security, or commercial claims.

## Decision

Release policy and observed evidence are separate document classes.

The commercial ledger contains only requirements, owners, gate classes, claim
boundaries, required evidence gate IDs, and required bundle subjects. It may
not contain observed statuses, evidence paths, or blockers. Rendering that
ledger never computes readiness.

Observed commands emit versioned evidence envelopes. Each envelope binds one
stable gate ID to:

- repository, full commit and tree SHA, ref, and a clean-worktree assertion;
- workflow file at the candidate, run, attempt, job, runner, host, and tools;
- exact ordered commands, working directory, timestamps, exit code, and every
  attempt;
- named input digests, the observed status and metrics; and
- an output log with path, media type, byte size, SHA-256 digest, and expiry.

Machine evidence has only `passed`, `failed`, `error`, or `skipped`. Only
`passed` satisfies a gate. `ready`, `accepted_risk`, and `ci_blocking` are not
evidence statuses. Semantic correctness never retries into green. A permitted
infrastructure retry must expose every attempt and classification; any hidden
or semantic fail-then-pass is rejected.

Official evidence comes only from the protected reusable exact-candidate
workflow named by gate policy. Local capture uses the same schema but is marked
diagnostic and cannot promote. The workflow checks out the explicit full SHA
detached, disables persisted credentials, checks HEAD/tree/ref/clean state,
runs exact gate commands, builds the package once, and uploads SHA/run/attempt-
named fragments and bundle.

The immutable ZIP name is:

```text
aether-release-evidence-<full-sha>-<run-id>-<attempt>.zip
```

The bundle contains the exact gate policy, canonical evidence envelopes,
output logs, package, optional signed waivers and SBOMs, and a file-integrity
manifest. Its canonical manifest records candidate and policy identity,
fragment digests, package and attested-subject digest, computed verdict and
blockers, and verifier version. ZIP entry order, timestamps, and canonical JSON
serialization are deterministic. A `latest` pointer may exist for navigation
but is rejected as authoritative verifier input.

The standard-library verifier operates offline except for later external
attestation identity lookup. It independently re-hashes every included byte,
recomputes envelope and bundle identities, enforces exact candidate/workflow/
command/host/metric policy, exposes attempts, validates waivers, compares the
package with its attested subject, recomputes the sorted blockers and verdict,
and emits byte-stable canonical verdict JSON.

## Waivers

A waiver is a separate candidate-commit/tree-bound, owner-approved, expiring
fact with compensating controls and an external-attestation reference. It
never changes an observed status. Policy decides whether the failed gate may be
waived. Policy correctness, trace identity, append admission, full semantic
acceptance, Rust/Go/Python quality, package identity, critical/high
vulnerabilities, and secret exposure are non-waivable.

## Package manifests, SBOMs, beta, and GA

A package file-integrity manifest proves bytes and paths; it is not a dependency
SBOM. Standard component/dependency SBOMs, vulnerability/license/code scan
results, and package provenance are separate required beta subjects delivered
under R5. Until they exist, a structurally valid R4 bundle verifies but its
commercial-beta verdict remains blocked.

Beta provenance binds one selected source tree, package, supported transport,
schema boundary, and evidence run. GA adds separately governed support,
incident, signed-promotion, and platform-distribution evidence. A valid beta
bundle does not imply GA or generalized distributed-truth claims.

## Consequences

- A checked-in workflow or authored ledger can no longer manufacture green.
- Failure, missing execution, expiry, tampering, candidate drift, concealed
  retries, and invalid waiver scope fail closed.
- The old readiness runner and policy renderer may run diagnostically during
  migration, but only the immutable verifier may promote.
- R4 can prove evidence integrity while correctly returning a blocked beta
  verdict for R5/R7 subjects that do not yet exist.

## Verification

`python/tests/test_release_evidence.py` covers clean deterministic verification
and stale/dirty identity, missing/skipped/unknown status, byte tampering,
future/expired evidence, hidden retries, authored outcomes, declaration-only
workflows, wrong suite/baseline/threshold, Capacity nesting, Pages SHA drift,
invalid/non-waivable/cross-candidate waivers, incomplete SBOMs, and package
attestation mismatch.
