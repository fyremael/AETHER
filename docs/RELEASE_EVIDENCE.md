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
diagnostic or incomplete R4 bundle is expected to say `blocked` while official
workflow identity and R5 bundle subjects are absent.

## Official capture

`.github/workflows/reusable-exact-candidate-evidence.yml` is invoked by Release
Readiness with an explicit full SHA and ref. It checks out detached with
persisted credentials disabled, checks HEAD/tree/clean state, runs every gate,
builds the package once, assembles the SHA/run/attempt-named bundle, verifies it
independently, and uploads both bundle and canonical verdict.

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
