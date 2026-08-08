# AETHER GCL ecosystem-admission record

Status: `CONTROLLED_ALPHA_RETAINED`

Captured: `2026-08-08T09:28:50Z`

Work package: [grandchallenge/MATH-PROGRAMME#294](https://github.com/grandchallenge/MATH-PROGRAMME/issues/294)

Design-partner pilot: [grandchallenge/AETHER#51](https://github.com/grandchallenge/AETHER/issues/51)

## Disposition

AETHER is in GCL custody as incubating semantic infrastructure. The repository
transfer is complete, but governed profile admission remains a candidate until
the protected review and merge of
[grandchallenge/gcl-standards#33](https://github.com/grandchallenge/gcl-standards/pull/33).

This record does not activate AETHER as institutional authority. It does not
authorize a mathematical, constitutional, publication, commercial-beta, or GA
claim. The live INTELLECT/GCL bridge remains on hold and requires a later,
separately authorized work package.

No tag, GitHub prerelease, or promotion record was created. Fresh
post-transfer exact-main qualification is blocked, so `controlled_alpha`
remains the release label.

## Identity and custody

| Item | Exact value | Result |
| --- | --- | --- |
| Repository before transfer | `fyremael/AETHER` | Historical only |
| Repository after transfer | `grandchallenge/AETHER` | Live custody |
| Repository ID | `1184906615` | Unchanged |
| Repository node ID | `R_kgDORqA9dw` | Unchanged |
| Protected `main` | `62272646689b726bdb54bd94b86f42efc812f618` | Post-transfer qualification candidate |
| Orbital preservation branch | `codex/report-timeout` | Preserved |
| Orbital preservation head | `a4b513bdb63b65a29463656d56bfbec035882401` | Draft PR #53; not in `main` |
| Pre-transfer controls head | `e6b6e3be149a28537714f22570d49f0e20cba22e` | Preserved in draft PR #54 |
| Post-transfer controls/tooling head used for pilot | `9218ea08cdfb529da482203854dbb0d7175e79fa` | Product code is byte-identical to `main`; only controls, workflows, tests, and documentation differ |
| GCL profile candidate | `1bfb4dfa349394e4a4f5cddf159e79e317fb505d` | Draft gcl-standards PR #33; checks green |

The transfer preserved open issues, draft PR #53, draft PR #54, and both
remote branch heads. Evidence produced while the repository was named
`fyremael/AETHER` is not accepted as qualification of the new repository
identity.

## Repository controls

The post-transfer live readback at tooling head `9218ea0` returned
`{"blockers": [], "status": "passed"}` for:

- strict protected-main checks;
- zero additional-human PR approvals during the active bootstrap deferral;
- stale-review dismissal and resolved conversations;
- no CODEOWNER or last-push approval claim;
- administrator enforcement, locked `main`, and no force push or deletion;
- GCL organization Actions selection with full-SHA policy;
- wiki disabled, rebase merge disabled, merge and squash retained;
- Dependabot security updates, secret scanning, push protection, and private
  vulnerability reporting enabled;
- release and Pages environment administrator bypass disabled;
- the existing named release reviewer retained.

Transfer temporarily reset secret scanning and the repository Actions
selection to the organization boundary. Secret scanning was restored. The
workflow adaptation in draft PR #54 removes the two excluded external action
uses rather than widening the GCL organization policy.

## Post-transfer exact-candidate qualification

All four runs below were newly dispatched after transfer against exact
protected-main SHA `62272646689b726bdb54bd94b86f42efc812f618`:

| Workflow | Run | Observed disposition |
| --- | ---: | --- |
| CI | [31249900197](https://github.com/grandchallenge/AETHER/actions/runs/31249900197) | In progress when this record was prepared; many product jobs passed, but this cannot compensate for failed prerequisites |
| Supply Chain | [31249901392](https://github.com/grandchallenge/AETHER/actions/runs/31249901392) | `startup_failure`; protected `main` still names an action excluded by GCL policy |
| Pages | [31249902540](https://github.com/grandchallenge/AETHER/actions/runs/31249902540) | `startup_failure`; protected `main` still names an action excluded by GCL policy |
| Capacity Planning | [31249903736](https://github.com/grandchallenge/AETHER/actions/runs/31249903736) | In progress when this record was prepared |

Qualification is therefore blocked without invoking Release Readiness. Prior
successful runs retained through the repository transfer are historical
evidence and were not reused.

## AETHER #51 design-partner pilot

Selected workflow: **governed incident blackboard**.

Named operator: **fyremael**, acting only as the bounded pilot operator and
repository Human Steward. This naming does not grant AETHER institutional
authority.

Declared boundary:

- one controlled single-node SQLite deployment;
- trusted appenders and one declared visibility domain;
- AETHER as a read/derive/explain fabric, not an autonomous actor;
- Git/GitHub remains the GCL institutional source;
- no exclusive GCL fact may exist only in AETHER;
- Postgres, multi-host failover, quorum, global `AsOf`, and a live GCL bridge
  are outside this pilot.

### Drill results

| Boundary | Evidence | Result |
| --- | --- | --- |
| SQLite restart/replay | `service_v2_operability.py` current-run gate | Passed |
| Packaged backup/restore through restart | Snapshot manifest plus pre/post mutation history check | Passed in 8.829 seconds |
| Live Postgres restart/replay | No `AETHER_POSTGRES_TEST_URL` in the local run | Unavailable; beta remains false |
| Operator error | Backup and restore without `-ConfirmServiceStopped` | Both failed closed with exit 1 |
| Resource exhaustion | HTTP resource-limit atomicity plus namespace rate/pagination tests | 2 passed; typed rejection left authority unchanged |
| Policy isolation | `policy_noninterference` | 8 passed |
| Stale authority | `pilot_contract` plus incident-board demo | Passed; stale actors fenced at Current |
| Abstention | `pilot_contract` pre-heartbeat authorization | Passed; returned no authorized tuple before a live heartbeat |
| Storage equivalence | `pilot_contract` | Passed for in-memory and SQLite services |
| Provenance | incident-board explanation | Root derived tuple `t11`; trace linked to source elements `e19`, `e20`, `e18`, and `e5` |

The combined admin/operator hardening wrapper produced the package but then
hung with no Cargo or AETHER child process and no new artifact. It was
terminated and is not counted as passing evidence. The direct recovery
collector and focused tests above are the accepted local pilot evidence.

### Fact boundary

Source inputs are journal datoms admitted from trusted pilot appenders:
observations, candidate-action metadata, dependency completion, approvals,
suppression, claims, lease epochs, heartbeats, and execution outcomes. The
GitHub work package and repository settings remain external institutional
sources, not AETHER facts of authority.

AETHER-derived facts are readiness, live authorization, stale-attempt
rejection, prior-cut authorization, and their provenance traces. In the demo,
`action/201` was derived ready, `remediator-b` at epoch 2 was derived as the
current authorized actor, and stale alternatives were derived as fenced.

External decisions remain external: whether an observation is trustworthy,
whether an incident is severe, whether a remediation should execute, whether
a human approval is valid, whether a repository is admitted, whether a claim
may promote, and whether the bridge may activate. An empty derived result is
abstention, not permission.

## Remaining exact-revision actions

1. Human review and merge of gcl-standards PR #33 is required before the
   repository profile is admitted.
2. Human review and merge of AETHER PR #54 is required before protected `main`
   can run under the GCL Actions boundary.
3. Orbital PR #53 remains separate; review and merge it only if Orbital is to
   enter a later protected-main candidate.
4. After the chosen merges, qualification must restart from the new exact
   protected-main SHA under `grandchallenge/AETHER`.
5. A prerelease and promotion record may be created only from a fully passing,
   independently verified post-transfer qualification bundle.
6. Any live INTELLECT/GCL bridge requires a new work package.
