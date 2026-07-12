# Commercial Release Readiness

AETHER's active target is **controlled design-partner alpha**. Commercial beta
is blocked, and general availability remains separately blocked.

The only authorized external claim during remediation is:

> Controlled single-node alpha with a real Rust semantic kernel, limited to one
> visibility domain, trusted appenders, and explicitly supported deployment
> boundaries.

The reproduced failures are recorded in
`docs/COMPREHENSIVE_AUDIT_2026-07-09.md`. The binding repair and
requalification sequence is `docs/REMEDIATION_PROGRAMME.md`.

## Stage Boundary

| Stage | Current posture | Meaning |
| --- | --- | --- |
| Controlled design-partner alpha | Active target | Closely supported single-node evaluation inside the exact visibility, append-authority, and deployment limits above |
| Commercial beta | Blocked | Cannot be restored until all six non-waivable remediation gates pass and one exact-candidate evidence bundle verifies independently |
| General availability | Blocked | Still needs its separate signed-promotion, support/incident, distribution, and distributed-truth gates |

## Commercial-Beta Blockers

All six gates are non-waivable:

1. `semantic.policy_noninterference` — policy-scoped semantic correctness
2. `semantic.trace_handle_identity` — execution-scoped trace identity
3. `storage.transactional_schema_append` — transactional schema-valid append
4. `release.evidence_integrity` — immutable release evidence
5. `security.dependency_supply_chain` — dependency SBOM, vulnerability, and license evidence
6. `service.transport_security` — supported transport-security boundary

The tracked policy source is
`fixtures/release/commercial-readiness-ledger.json`. During R0 that ledger
was used to record contained posture and blockers. Under R4 it is now strictly
requirements/owner/claim policy and rejects authored outcome fields. R4
computes blockers and verdict only from immutable outcomes for an exact commit,
tree, package, inputs, and workflow run; see `docs/RELEASE_EVIDENCE.md`.

## Existing Release Artifacts

The current runner remains useful for development diagnostics:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-release-readiness.ps1
```

It can produce package, performance, recovery, customer-workflow, and
security/key-lifecycle artifacts under `artifacts/`. Those artifacts are not a
commercial-beta qualification because they are mutable/ignored and are not all
bound to one exact candidate. The policy renderer no longer trusts or accepts
authored statuses; only the immutable verifier may promote.

The operational package inventory is now correctly named a file manifest.
Strict CycloneDX dependency/package SBOMs and the scanner/attestation workflow
are implemented, but their source files are not evidence that the hosted jobs
succeeded for the candidate. Likewise, a `ci_blocking` marker or workflow
source path is not an observed outcome.

## Promotion Rule

Do not sell or publish past the active controlled-alpha boundary. Commercial
beta may be reconsidered only after:

- R1-R5 exit gates are green in order;
- every non-waivable gate has a passed exact-candidate result;
- the final package, dependency SBOM, provenance, documentation, and site name
  the same SHA;
- the evidence bundle verifies from a fresh environment;
- protected release approval completes at R7.

No previous local artifact, `latest` file, ledger status, or workflow
declaration can substitute for that qualification.
