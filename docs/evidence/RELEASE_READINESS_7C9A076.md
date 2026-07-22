# Release Readiness failure: `7c9a0763627e9c11f88ad72520116ddd8e15ac71`

Candidate `7c9a0763627e9c11f88ad72520116ddd8e15ac71` is permanently
disqualified for commercial beta. It must not be rerun, reinterpreted, or used
to author a successful promotion record.

## Immutable coordinates

- Candidate tree: `c70fe2f4c39d4edaf375d2ee3412ffa7554263f7`
- Candidate ref: `refs/heads/main`
- CI: run `29875839172`, attempt 1, passed
- Supply Chain: run `29875839196`, attempt 1, passed
- Canonical package artifact: `8513021594`, 10,100,188 bytes, archive
  SHA-256 `eebfbae852cfdac09829b3f34ccf57d1c97c7c2ab73e194256d3cfc5e911baae`
- Canonical package SHA-256:
  `6d16efd4096eea6b0ea1778d7c34cbcd5421fc9c88269e0580ec6d761a331924`
- Supply Chain evidence artifact: `8513132181`, 83,408 bytes, archive
  SHA-256 `76859da2c745fdd24bc22f61a7cfa4a59d4da495ffafabb5ef10de82ce7b068d`
- Pages: run `29875839182`, attempt 1, passed
- Pages deployment-verification artifact: `8512946809`, 423 bytes, archive
  SHA-256 `c457f948db067de4256ad69e1942c7b27701a575ddda827c0a6488e76fb0cc02`
- Capacity Planning: run `29878742179`, attempt 1, passed
- Capacity report artifact: `8514954015`, 14,629 bytes, archive SHA-256
  `18b48ca6702fe18b58ba75edfe1ebd1796373091e28ef0cdb5264a731d0ad416`
- Immutable capacity JSON SHA-256:
  `28704d522beaee58c13f88afc10bde714be4d65abbaa4760e32b21185c72dc68`
- Release Readiness: run `29881660086`, attempt 1, failed
- Protected approval job: `88803669094`, passed
- Windows operational-readiness job: `88803757575`, passed
- Exact-candidate evidence producer job: `88807351923`, passed
- Dependent evidence verifier job: `88809874295`, failed
- Qualification-subject artifact: `8515429667`, 10,380,454 bytes, archive
  SHA-256 `7964ad3e04f981826272d321d69ad2ba8e55cc75ff2869e68f4037c10836f3a3`
- Release-readiness artifact: `8515431422`, 50,723,770 bytes, archive
  SHA-256 `98e59ef7bb1c6d9acef3a6214fa9599d9d842de975f46ef652c09c1bc7fcae9f`
- Exact-gate diagnostic artifact: `8515725696`, 37,344 bytes, archive
  SHA-256 `513c629124ec52e2c9e5171b18493ffac96af73b65ab360eca999756c93bcfd3`
- Evidence artifact: `8515732193`, 10,307,433 bytes, archive SHA-256
  `46c19dc5920e335a1df8c6a81c8926cee5717cc446053aa08a316b4ddd774af5`
- Inner evidence bundle SHA-256:
  `f2166f5070aff4ec0d87c6c55d3f3396fb02eaa555db74bc11a218c5c79716d3`
- Bundle ID:
  `3a003582a98b16eedb9b6b327ffc3b521a6c1c5d495043deadaf17815937c535`
- Readiness manifest SHA-256:
  `08361758ae402f27917f49550fb6419104cba0af6c360c29d7d8739092796d9f`
- Release artifacts expire at `2026-10-20T00:55:50Z`.

## Passed evidence that remains diagnostic

Operational readiness passed, exact gate capture passed, all 18 subjects were
assembled exactly once, and the producer's canonical bundle computation had no
blockers. The raw Capacity policy rung proves one shared service, the exact six
operation endpoints, 32 workers, 384/384 successes, zero failures, zero 503
responses, 221.3981 ms p95 and 250.7303 ms p99 against the unchanged 2,000 ms
p95 limit.

The bundle manifest records `computed_verdict: passed`, but that internal
computation is not a promotion verdict. The dependent verifier failed and did
not upload `verified-verdict.json`; the aggregate workflow therefore failed.

## Failure and disposition

The dependent verifier failed closed with:

```text
release evidence verification failed: qualification readiness workflow binding differs
```

The readiness manifest binds the caller workflow as exact run/attempt
`29881660086/1`. The verifier instead compared that two-field caller binding to
the reusable producer's full workflow envelope, which additionally contains
the producer job, workflow file, artifact name, runner, host and tool versions.
Those are different legitimate identities within the same run, so exact object
equality can never pass the real two-stage workflow.

The focused repair projects the already-validated producer envelope to exact
`run_id` and `attempt` before comparing it with the readiness caller binding.
It retains the full producer-envelope checks and adds negative cross-run and
cross-attempt tests. Controlled alpha remains unchanged. After review and merge,
an entirely new protected candidate must restart from exact-SHA CI, Supply
Chain and Pages.
