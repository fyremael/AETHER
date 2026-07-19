# Release Readiness blocker — `5e4f95a`

Protected candidate `5e4f95a50792a7a301598abc34f6fd23e32bb91d`
(tree `0d57234b81296a398a7a0a6481c03a55af21d284`) did not qualify commercial
beta. Controlled design-partner alpha remains the active claim.

## Immutable workflow evidence

- Release Readiness run: `29678877127`, attempt `1`
- Workflow: `.github/workflows/release-readiness.yml`
- Ref: `refs/heads/main`
- Conclusion: `failure`
- Failure artifact: `8440118839`, `release-readiness-artifacts`
- Failure artifact byte size: `10269862`
- Failure artifact archive SHA-256:
  `240e727c9c4ca2c89c9cf7f1f58f530859fb12ea13922aed5ceb0224f43125af`
- Failure artifact expiry: `2026-10-17T07:53:55Z`
- Evidence manifest SHA-256:
  `492d4f032a977e61a13a1a49b18277a5cf2614395c648b7ee9d37ab72374b567`
- Readiness transcript SHA-256:
  `fa3771caf5587b156540ed641ceca3858c22e504c4281ca0a28bafe220238678`
- Passing performance-beta output SHA-256:
  `b2b083ec01d5a9c3077063d95ffdfa7c2b3b4539915d18cf85ab6ba7b0eb0a02`
- Run URL:
  <https://github.com/fyremael/AETHER/actions/runs/29678877127>

The exact prerequisite collector passed for CI `29667418277`, Supply Chain
`29667418295`, Pages `29667418282`, and Capacity Planning `29677246612`.
The performance-beta gate also passed every threshold. The workflow then
failed before candidate-bound subjects were built.

## Root cause

`run-release-readiness.ps1` accepted and verified the canonical Supply Chain
package from the qualification-input directory. When it tried to stage those
same bytes at the package-local path, it removed any previous destination but
did not create the parent `artifacts/pilot/packages` directory. The subsequent
`Copy-Item` failed with:

> Could not find a part of the path
> `D:\a\AETHER\AETHER\artifacts\pilot\packages\aether-pilot-service-windows-x86_64.zip`.

This is a deterministic workflow plumbing defect. It is not evidence of a
semantic, performance, Capacity, or canonical-package failure.

## Remediation and boundary

The package staging path now creates its parent directory before copying the
canonical package. A contract test fixes the ordering so the missing-directory
case cannot silently regress.

Run `29678877127` remains failed and must never be rerun or reinterpreted as
green. The repair requires a new protected-main candidate and a complete new
CI, Supply Chain, Pages, Capacity Planning, Release Readiness, and independent
verification sequence.
