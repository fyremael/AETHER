# AETHER Gap Analysis — 2026-07-18

## Executive verdict

AETHER is ready to attempt exact-candidate commercial-beta qualification, but
it is not ready to claim commercial beta.

The original remediation targets are no longer the principal risk. Policy is
applied inside replay, compilation, and evaluation; explanation identity is
execution-scoped and durably recoverable; append admission is validated before
commit; and release qualification is designed around immutable candidate-bound
subjects. Protected `main` is green for CI, Supply Chain, and Pages.

The immediate gap is evidence completion. The selected protected candidate has
no Capacity Planning run, no Release Readiness run, no official passed verdict,
and no independent exact-byte verification. The commercial ledger correctly
remains at controlled design-partner alpha.

The next phase should therefore be a qualification exercise, not feature
development. Freeze the candidate, run the remaining exact-SHA sequence, and
promote only if both official and independent verdicts pass.

## Current protected candidate

| Field | Observed state |
| --- | --- |
| Commit | `5e4f95a50792a7a301598abc34f6fd23e32bb91d` |
| Tree | `0d57234b81296a398a7a0a6481c03a55af21d284` |
| Branch | Protected `main` |
| Local checkout | Clean and aligned with `origin/main`; Git reports permission warnings while scanning two `.pytest_cache` directories |
| CI | Passed — [run 29667418277](https://github.com/fyremael/AETHER/actions/runs/29667418277) |
| Supply Chain | Passed — [run 29667418295](https://github.com/fyremael/AETHER/actions/runs/29667418295) |
| Pages | Passed — [run 29667418282](https://github.com/fyremael/AETHER/actions/runs/29667418282) |
| Capacity Planning | No run for this SHA |
| Release Readiness | No run for this SHA |
| Independent bundle verification | Not available |
| Promotion record | Not available |
| Current commercial target | `design_partner_alpha` |

The three passing workflows started from the same protected-main push and name
the exact candidate SHA. They are necessary prerequisites, not a beta verdict.

## What is materially closed

These items should not be reopened without new contradictory evidence:

1. **Policy is semantic input, not response filtering.** Service evaluation
   uses policy-scoped replay, compilation, and bounded evaluation. Regression
   tests exercise hidden-fact noninterference and policy-sensitive result
   identity.
2. **Explanation identity crosses process boundaries.** Trace handles identify
   execution-scoped durable records rather than process-local object aliases.
   Restart and non-aliasing paths are covered in the service core.
3. **Invalid appends fail at admission.** Schema and transaction invariants are
   checked before journal commit rather than being deferred to later replay.
4. **Readiness trusts outcomes and bytes.** The workflow accepts exact successful
   prerequisite runs, uses the Supply Chain package as canonical input, produces
   candidate-bound evidence, and has a dependent verifier rather than accepting
   authored status declarations.
5. **The kernel architecture remains coherent.** The Rust workspace still owns
   canonical DSL semantics, SCC/semi-naive execution, deterministic temporal
   replay, and provenance. Go and Python remain boundary clients rather than
   competing semantic engines.

These are implementation and test conclusions. They do not substitute for the
missing official exact-candidate evidence.

## Prioritized gaps

### P0 — blocks commercial beta now

| Gap | Evidence | Required closure |
| --- | --- | --- |
| Exact candidate is only partially qualified | CI, Supply Chain, and Pages pass; Capacity Planning and Release Readiness are absent for the same SHA | Dispatch Capacity Planning for the frozen candidate, require success, then dispatch Release Readiness with the four exact successful prerequisite run IDs |
| No admissible official beta verdict | No immutable Release Readiness bundle or dependent verifier verdict exists for the candidate | Require operational readiness, all 18 candidate-bound subjects, the reusable evidence producer, dependent verifier, and aggregate workflow conclusion to pass |
| No independent proof of artifact identity | No fresh detached-checkout download or exact-byte comparison exists | Download by immutable artifact ID, verify API size and SHA-256, verify the inner bundle bytes, recompute the verdict, and compare it byte-for-byte with the dependent verdict |
| No authorized promotion record | Ledger target remains alpha and no `aether.commercial-beta-promotion.v1` fixture exists | Create the promotion PR only after both official and independent verdicts are `passed`; otherwise preserve alpha unchanged |

The gate policy names 18 required future bundle subjects: the package file
manifest; three SBOMs; vulnerability, license, code, and secret scans; TLS;
namespace contention; resource controls; recovery; performance; soak; capacity;
Pages deployment; package provenance; and customer workflow. Presence alone is
not enough: each subject must pass semantic validation and bind to the same
candidate, workflow outcome, artifact, and canonical package bytes.

### P1 — claim and governance defects to close before promotion merge

1. **The GA ledger does not yet model the promised four independent blockers.**
   `fixtures/release/commercial-readiness-ledger.json` currently has three GA
   entries. `ga_support_and_incident` folds signed promotion into the support
   requirement, and `ga_distributed_truth` is classified as `future` rather than
   `blocking`. This conflicts with the promotion validator and maintained claim
   surfaces, which require GA to remain `0/4` on four separate blockers:
   support/incident posture, multi-platform distribution, signed promotion, and
   distributed-truth qualification. Split and block all four in the conditional
   promotion PR, with negative tests that prevent recombination or authored
   success.
2. **Maintained status documents lag the repository.** `docs/STATUS.md` still
   lists exact-SHA CI, Supply Chain, and Pages confirmation as open and names the
   already-remediated restart-latency work as the immediate focus.
   `docs/ROADMAP.md` still describes the pre-merge host-policy repair state.
   Refresh these only after the candidate outcome is known so they record either
   the immutable pass or the immutable blocker.
3. **Temporary repository governance is deliberately weak.** Protected `main`
   requires strict CI and Supply Chain gates and conversation resolution, but
   currently requires zero approving reviews. The `release` environment has one
   reviewer, allows self-review, and permits administrator bypass. This matches
   the temporary small-team policy, but it does not establish organizational
   independence. Keep an external review receipt for beta and restore distinct
   account approval, self-review prevention, and tighter bypass controls as the
   team grows.

### P2 — limits the strength and breadth of the beta

1. **Performance evidence is usable for qualification but not yet a broad
   market claim.** The Colab diagnostic for predecessor `c39ab41` is correctly
   marked `diagnostic_only`; explanation throughput varied by 24.18% across its
   three retained runs. Colab is useful containment and defect discovery, but it
   cannot qualify the Windows package. Diagnose allocation, ordering, and host
   noise before publishing strong explanation-throughput claims. Keep only the
   accepted native-Windows regression surfaces blocking.
2. **Deployment support is intentionally narrow.** The selected beta boundary is
   Windows x86_64, single node, SQLite by default, optional `verify_full`
   Postgres with local SQLite sidecars, and loopback HTTP or trusted TLS ingress
   with direct backend access blocked. Remote Postgres, non-loopback ingress,
   recovery, and resource-control results still need to be present in the exact
   candidate bundle.
3. **Distributed truth is a prototype, not a product claim.** The current
   single-host leader/follower authority-partition slice has durable metadata,
   manual promotion, fencing, lag/degraded status, and divergence rejection. It
   does not have automatic election, quorum consensus, multi-host replication,
   follower-read contracts, or managed failover.
4. **Schema evolution is not production-qualified.** Mixed historical schemas
   and in-place attribute type or merge-class changes need explicit migration,
   compatibility, and rollback qualification.
5. **SDK and authoring ergonomics remain early.** Go and Python are real boundary
   clients, but their async, administration, packaging, and compatibility
   surfaces are not mature. DSL ergonomics and document modularity remain
   post-v1 work.

### P3 — important maintenance, not a reason to interrupt this candidate

1. **Automation runtime debt is visible.** GitHub annotates actions that still
   declare the Node 20 runtime and are being forced onto Node 24. Issue #2 tracks
   the migration.
2. **Dependency backlog is large.** Fifteen PRs are open: thirteen Dependabot
   updates plus AETHER-POL (#10) and CoordinationDesk (#5). Several dependency
   updates are major versions. Do not merge them into the frozen qualification
   candidate; triage them in guarded batches after the candidate passes or is
   abandoned.
3. **Operational trend storage is still local/artifact-oriented.** Persistent
   benchmark dashboards and durable trend retention remain open beyond saved run
   bundles, summaries, and uploaded workflow artifacts.
4. **Planning work is not fully retired.** Open issues still track QA hardening,
   capacity planning, the Node runtime migration, and the GCL regret contract.
   Convert each into a bounded milestone or close it; avoid a permanent parallel
   backlog with ambiguous claim authority.

## Recommended execution order

1. **Freeze protected `main` at the candidate above.** Any mainline advance
   invalidates the sequence and requires a new candidate SHA, tree, and runs.
2. **Dispatch Capacity Planning.** Require the candidate-bound capacity artifact
   and workflow conclusion to pass. A diagnostic Colab result cannot replace it.
3. **Dispatch Release Readiness.** Supply the exact successful CI, Supply Chain,
   Pages, and Capacity run IDs; approve the `release` environment; permit no
   concealed semantic retry from red to green.
4. **Retain immutable evidence coordinates.** Record the candidate commit/tree,
   run and attempt IDs, artifact IDs, artifact digests, canonical package digest,
   bundle name/digest, and dependent verdict name/digest.
5. **Independently verify exact bytes.** Use a fresh detached checkout of the
   candidate and download by artifact ID. Recompute the official verifier result
   with the exact commit, tree, `refs/heads/main`, `--require-official`, and
   `--require-passed`. Require byte equality between the redownloaded inner
   bundle, supplied bundle, recomputed verdict, and dependent verdict.
6. **Obtain independent review.** Review the 18 subject mappings, GitHub outcomes,
   canonical package binding, and recomputed verdict. Any disagreement blocks
   promotion.
7. **Conditionally create the beta-promotion PR.** In that PR, generate the
   immutable promotion record, split GA into four blocking gates, refresh claim
   surfaces to name the exact candidate, and keep the supported boundary narrow.
8. **If anything fails, stop.** Preserve controlled alpha, save the immutable
   blocker evidence, repair the defect in a focused PR, merge it, and start with
   a new protected candidate. Do not patch or reinterpret a failed bundle.

## Readiness dashboard

| Dimension | Current assessment | Rationale |
| --- | --- | --- |
| Kernel thesis | Green | Canonical Rust/DSL/replay/SCC/provenance architecture remains intact |
| Semantic remediation | Green in code and exact-SHA CI | Policy noninterference, durable trace identity, and append admission are implemented and tested |
| Qualification plumbing | Green enough to exercise | Canonical package, candidate-bound subject model, exact-run collection, and dependent verification exist |
| Candidate prerequisites | 3/4 passed | CI, Supply Chain, and Pages pass; Capacity is absent |
| Official readiness | 0/1 | No Release Readiness run for the candidate |
| Independent verification | 0/1 | No immutable artifact download and byte-for-byte recomputation |
| Commercial beta | Blocked | No official or independent passed verdict and no promotion record |
| GA | 0/4 and blocked | Support/incident, platform distribution, signed promotion, and distributed truth remain separate unmet obligations |
| Repository governance | Temporary/accepted for now | Required aggregate checks are strict; distinct-account approval and self-review prevention are relaxed |
| Performance claim strength | Limited | Native candidate evidence remains authoritative; Colab variance is diagnostic and explanation throughput is noisy |

## Decision

Proceed with exact-candidate qualification now. Do not broaden features, merge
dependency upgrades, or change claims while the candidate is frozen. A passed
Capacity run, passed Release Readiness verdict, exact-byte independent
verification, and independent review are the only justified path to a beta
promotion PR. GA should remain blocked after any beta promotion.

## Audit basis

This report combines the protected local checkout, live GitHub workflow results,
live branch and release-environment protection settings, current release policy
fixtures, maintained status/roadmap/limitations documents, the Colab diagnostic
summary, and the open PR/issue backlog. Mutable `latest` artifacts were treated
as navigation or diagnostics only, never as qualification evidence.
