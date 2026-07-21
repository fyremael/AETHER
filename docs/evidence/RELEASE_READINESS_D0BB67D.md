# Release Readiness failure: `d0bb67d0388db6305865f83648e766e3786e0f69`

Candidate `d0bb67d0388db6305865f83648e766e3786e0f69` is permanently
disqualified for commercial beta. It must not be rerun, reinterpreted, or used
to author a successful release-evidence bundle.

## Immutable coordinates

- Candidate tree: `294204e9c0a62fbf945564623ec750fa9cd7e1ef`
- Candidate ref: `refs/heads/main`
- CI: run `29696344625`, attempt 1, passed
- Supply Chain: run `29696344593`, attempt 1, passed
- Supply Chain canonical package artifact: `8445147172`
- Supply Chain evidence artifact: `8445185012`
- Pages: run `29696344611`, attempt 1, passed
- Pages verification artifact: `8445114749`
- Capacity Planning: run `29698117362`, attempt 1, passed
- Capacity report artifact: `8446110896`
- Capacity artifact archive SHA-256:
  `77ec014d242271f483ea795cc1d2d25b1d21bf124b7fae7531ea81069f795d2c`
- Immutable capacity JSON SHA-256:
  `9fc9e905ebb76548a31f0a0130db4a0b38bda3dc241ce1dddd1c636f5cf888a2`
- Release Readiness: run `29863838006`, attempt 1, failed
- Release-readiness artifact: `8509044720`, 50,722,603 bytes, archive
  SHA-256 `dc7a179a41137d489ae6f652e144856f862a41490b1fab9a609f42f67be0e126`
- Qualification-subject artifact: `8509041995`, 10,380,921 bytes, archive
  SHA-256 `c492bae51212729e4789f5622c5268e49ac7777d9493c42131e3df8f1d829a0a`
- Both failure artifacts expire at `2026-10-19T19:59:27Z`.

## Passed operational bytes

The protected approval and complete Windows operational-readiness job passed
before the exact-candidate evidence producer failed. These bytes diagnose the
failed candidate but cannot form an official bundle or promotion verdict:

- Readiness manifest: 2,714 bytes, SHA-256
  `21f0c771390f027791ed52b4a8f5157cef513e6d70a15b9306929c628e815bbd`
- Service v2 proof: 4,062 bytes, SHA-256
  `1a974216e84c249fc46187d30d5a3268f919e69d0a6f55042ab0455464d3ac6e`
- Performance-beta proof: 5,596 bytes, SHA-256
  `792e572e70f2f977b1040d2da5f3d60bba980c597a59901dde12faecdffaea7c`
- Customer-workflow proof: 6,760 bytes, SHA-256
  `fb0b36161d293bf1b294a024e9680b479a0597d3aef64a479ce4919db3f711f7`
- Readiness transcript: 125,749 bytes, SHA-256
  `500548a4c647928978c141c4fa462316b7a37688b1b29f2e19747d92f6d97844`

The readiness manifest reports `status: passed`; the service and performance
proofs report `beta_ready: true`; and customer workflow reports
`workflow_ready: true`. The repaired shared-service Capacity report proves 32
workers, 384/384 successful operations, zero failures, zero 503 responses and
228.5721 ms p95 against the unchanged 2,000 ms limit.

## Failure and disposition

The exact-candidate evidence producer failed closed at the non-waivable gate:

```text
quality.python_boundary: failed
```

The gate policy executes `python -m pytest python/tests -q`, but the reusable
workflow installed only `requirements-release.txt`, which did not declare
pytest. The ordinary CI Python job uses `unittest`, so its successful outcome
did not prove that the separate pytest runner dependency existed. The evidence
workflow therefore trusted mutable hosted-image contents for a required test
runner. A local Python 3.12.10 reproduction with pytest 8.3.5 ran all 100 tests
successfully; that diagnostic narrows the dependency defect but cannot
rehabilitate the failed candidate.

The focused repair pins pytest in the release dependency set and adds an
always-uploaded SHA/run/attempt-named gate-diagnostic artifact plus an explicit
failure barrier before qualification subjects can be consumed. Failed gate
outputs will therefore remain inspectable without allowing bundle assembly to
continue. Controlled alpha remains unchanged. After review and merge, an
entirely new protected candidate must restart from exact-SHA CI, Supply Chain
and Pages.
