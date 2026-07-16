# Immutable Release Evidence

AETHER release claims are computed from candidate-bound evidence, never from
the commercial claim-policy ledger or mutable `latest` artifacts.

The governing decision is
[`ADR 0013`](ADR/0013-immutable-release-evidence-and-claim-computation.md).
The machine contracts are under `schemas/release/`, and the exact gate policy
is `fixtures/release/gate-policy.json`.

## Local diagnostic capture

Run only from a clean committed tree:

```powershell
python scripts/release_evidence.py capture --enforce
```

Local envelopes use the same content and identity contract as CI, but carry
`official: false`. They can test the trust layer and expose blockers; they
cannot promote a release.

After building the package once, assemble and verify a bundle:

```powershell
python scripts/release_evidence.py assemble `
  --evidence-dir <capture-directory> `
  --package artifacts/pilot/packages/aether-pilot-service-windows-x86_64.zip `
  --output-dir artifacts/release/bundles

python scripts/verify_release_evidence.py `
  artifacts/release/bundles/aether-release-evidence-<sha>-<run>-<attempt>.zip `
  --expected-commit-sha <full-sha> `
  --expected-tree-sha <full-tree-sha>
```

The verifier returning success means the bundle is internally authentic and
its verdict was recomputed. Inspect `computed_verdict` separately. A valid
diagnostic or incomplete bundle is expected to say `blocked` while official
workflow identity or any required R5/R6 gate and bundle subject is absent.
`gate-policy.json` enumerates supply chain, transport, beta service operations,
recovered architecture, and evidence-integrity source gates in addition to
R1-R4.

## Official capture

`.github/workflows/reusable-exact-candidate-evidence.yml` is invoked by Release
Readiness with an explicit full SHA and ref. It checks out detached with
persisted credentials disabled, checks HEAD/tree/clean state, runs every gate,
builds the package once, attests it, assembles the SHA/run/attempt-named bundle,
and uploads it. A separate dependent job downloads that immutable artifact and
verifies the included provenance signature plus the live GitHub run, completed
producer job, exact artifact digest and bundle bytes, candidate, ref, and
GitHub-hosted runner before it uploads the canonical verdict. A declared run ID,
host, or artifact name is not evidence.

Release Readiness places both the reusable evidence job and the operational
readiness job behind a dedicated `release` environment approval. The hosted
environment permits only `main`; a branch or unapproved run cannot become an
official candidate merely by invoking the reusable workflow.

Download the immutable artifact by its full name. Do not feed a file or path
containing `latest` to the verifier. For promotion, also pass
`--require-official`; once R5 subjects exist, promotion additionally passes
`--require-passed`.

## Failure and waiver rules

- `failed`, `error`, and `skipped` never become `passed`.
- Semantic correctness never retries into green.
- Infrastructure retry exposes all attempts and is allowed only where policy
  explicitly says `infrastructure_only`.
- A waiver is a separate externally attested, candidate-bound, expiring fact.
- Core semantic, quality, package-identity, high/critical vulnerability, and
  secret-exposure gates are non-waivable.
- Blocking promotion is a safe rollback. Restoring authored readiness is not.
