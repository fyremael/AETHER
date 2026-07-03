# Commercial Release Readiness

AETHER is currently positioned for **selected commercial beta**, not broad
general availability.

The tracked source of truth is
`fixtures/release/commercial-readiness-ledger.json`. The release-readiness
runner renders that ledger into saved JSON and markdown artifacts on every run.

## Stage Boundary

| Stage | Current posture | Meaning |
| --- | --- | --- |
| Design-partner alpha | Ready | Controlled, closely supported pilots over the single-node semantic kernel and packaged pilot service |
| Commercial beta | Ready target | Paid, selected beta deployments with explicit support boundaries, Service v2 proof, rollback evidence, performance thresholds, security/key lifecycle evidence, and a tested customer workflow |
| General availability | Blocked | Needs signed artifact promotion, support/security posture, multi-platform distribution, and separately gated distributed-truth claims |

## Operating Rule

Do not sell past the green stage.

For the current tree, that means:

- we may claim a defensible selected commercial beta for exact single-node semantic coordination deployments
- we must not claim GA or broad managed-platform readiness
- we must not claim GA until the release, security, support, and distribution posture is product-grade

## Release Evidence

Run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-release-readiness.ps1
```

For a stricter commercial-beta candidate run, use:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-release-readiness.ps1 -CommercialBetaCandidate
```

The runner now also enforces beta-specific Service v2 and rollback checks when
the ledger target is `commercial_beta`, so the flag is retained mainly as an
explicit operator override.

The generated release summary includes:

- `artifacts/qa/release-readiness/service-v2-operability-latest.md`
- `artifacts/qa/release-readiness/service-v2-operability-latest.json`
- `artifacts/qa/release-readiness/performance-beta-latest.md`
- `artifacts/qa/release-readiness/performance-beta-latest.json`
- `artifacts/qa/release-readiness/security-key-lifecycle-latest.md`
- `artifacts/qa/release-readiness/security-key-lifecycle-latest.json`
- `artifacts/qa/release-readiness/rollback-record-latest.md`
- `artifacts/qa/release-readiness/rollback-record-latest.json`
- `artifacts/qa/release-readiness/customer-workflow-latest.md`
- `artifacts/qa/release-readiness/customer-workflow-latest.json`
- `artifacts/qa/release-readiness/commercial-readiness-latest.md`
- `artifacts/qa/release-readiness/commercial-readiness-latest.json`
- a commercial readiness section inside `artifacts/qa/release-readiness/latest.md`

## Service V2 Hardening Focus

Commercial beta remains green only while the ledger evidence proves:

- Service v2 namespace, Postgres, and container behavior remain named blocking gates
- SQLite restart/replay is captured directly in the Service v2 proof artifact
- Postgres restart/replay is accepted as blocking CI evidence by default and captured live when `AETHER_POSTGRES_TEST_URL` is present
- package backup/restore through restart is captured after the pilot package build
- rollback is captured in a versioned release record per candidate
- the AI support resolution desk customer workflow executes as a release acceptance artifact
- token lifecycle and package integrity evidence are release-visible
- package SBOM/checksum evidence exists for beta; signed artifact provenance remains a GA gate
- performance trends have explicit beta thresholds rather than one-off benchmark snapshots
