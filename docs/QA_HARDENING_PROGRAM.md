# QA Hardening Program

This document governs AETHER's post-v1 quality-hardening program.

Its purpose is narrower and more operational than `docs/QA.md`.
`docs/QA.md` defines the standing regression and release-readiness gates.
This document defines how we actively hunt defects, spec gaps, operational gaps,
and usability failures from multiple perspectives before they silently become
release blockers.

The posture for this first pass is deliberate:

- internal-first
- ops/admin-led
- phased promotion from diagnostics into blockers
- no paid public bug bounty yet

## Hardening Objective

The hardening program exists to answer one question:

Can we drive AETHER from the perspectives that will actually use, operate,
judge, and scale it, and turn the resulting defects into a disciplined queue
instead of anecdotal discomfort?

That means hunting:

- semantic defects
- replay and proof inconsistencies
- operational failure modes
- auth and policy missteps
- packaging and deployment regressions
- SDK and HTTP drift
- documentation and demo gaps that make the system harder to understand than it needs to be

## Persona Matrix

The first hardening pass runs the same repository from four perspectives.

| Persona | Primary question | Core surfaces |
| --- | --- | --- |
| `admin` | Can I deploy, rotate, restart, back up, restore, and recover the pilot bundle safely? | packaged pilot service, config, auth reload, backup/restore, restart/replay |
| `operator` | Can I tell what is active, what changed, what is allowed, and why? | reports, delta reports, explain, policy-aware visibility, TUI smoke |
| `user` | Does the stable boundary behave consistently for a boundary consumer? | HTTP API, Go client, Python SDK, notebooks, onboarding docs |
| `exec` | Does the flagship story still communicate the real utility clearly and honestly? | Demo 03, Demo 04, Pages/front door, commercialization and education links |

These personas are not abstractions for their own sake. They are the lenses we
use to classify incoming defects and to decide which checks deserve promotion
into blocking automation.

## Defect Classes

Every intake item should be classified as one primary type.

### Bug

Use `bug` when the implementation violates the documented behavior, breaks an
existing workflow, returns an inconsistent contract, or produces an incorrect
semantic answer.

Examples:

- replay returns the wrong cut
- auth reload leaves revoked tokens usable
- report redaction widens past the caller's policy
- packaged restore does not actually restore the prior state

### Spec gap

Use a semantics discussion or explicit spec-gap report when the current docs,
spec, ADRs, or behavior do not say enough to determine what the correct answer
is.

Examples:

- a partition or imported-fact edge case is under-specified
- the docs describe a proof surface, but not its behavior under policy denial
- the current text leaves operator intent or replay boundaries ambiguous

### Usability gap

Use this when the system may be technically correct but the user journey is
confusing, jargon-heavy, misleading, or brittle.

Examples:

- the notebook setup path hides a dependency assumption
- a demo proves the right thing but in a hard-to-follow order
- the docs require glossary-first reading for a first-time evaluator

### Operational gap

Use this when the semantics are intact but deployment, rotation, observability,
packaging, backup, restore, or recovery discipline is insufficient.

Examples:

- a config failure is hard to diagnose
- a packaged helper does not leave clear artifacts
- the admin flow works only by hand-waving through hidden state

### Security report

Use the responsible-disclosure path in `SECURITY.md` for vulnerabilities,
secret-handling problems, auth bypasses, data exposure, or anything that should
not be reported in public first.

## Severity And Priority Rubric

Hardening findings should record both severity and priority.

### Severity

| Severity | Meaning |
| --- | --- |
| `critical` | Breaks release trust, security posture, semantic correctness, or packaged recoverability in a way that demands immediate action |
| `high` | Materially weakens operator/admin reliability, policy correctness, or stable boundary behavior and should be fixed in the next cycle |
| `medium` | Real defect or gap with clear user/operator cost, but not immediately release-blocking |
| `low` | Worth fixing, but the system still behaves coherently enough to continue the current phase |
| `observational` | Not yet a bug or blocker; useful evidence, drift, or ambiguity that should be tracked for follow-up |

### Priority

Use the review-style priority ladder when a finding is actionable:

- `P0`: drop everything; universal blocker
- `P1`: urgent next-cycle fix
- `P2`: normal fix
- `P3`: low-priority improvement

Severity describes impact. Priority describes scheduling.

## Required Evidence

Every hardening report should carry enough evidence that another engineer can
reproduce or triage it without guesswork.

Required fields:

- persona
- surface
- title
- status
- severity
- exact command or workflow
- artifact path, cut, element, tuple, or page when relevant
- concise notes describing the observed behavior

When possible, include:

- transcript path
- screenshot or page path
- failing response payload
- expected behavior anchor
- commit SHA

## Hardening Artifact Contract

The hardening sweep writes:

- `artifacts/qa/hardening/latest.md`
- `artifacts/qa/hardening/latest.json`
- timestamped siblings under `artifacts/qa/hardening/`

The JSON contract is internal and triage-oriented.
Each result entry must include:

- `persona`
- `surface`
- `severity`
- `title`
- `status`
- `repro_command`
- `artifact_path`
- `notes`

Recommended optional fields:

- `started_at`
- `finished_at`
- `duration_seconds`
- `command_exit_code`

## Sweep Packs

The first hardening runner is organized into four packs.

### Admin pack

- packaged service build and unpack
- packaged service boot plus `/health` and `/v1/status`
- token rotation and auth reload
- backup, restore, restart, and replay verification
- config failure handling for missing token sources, stale token paths, and bad command-backed tokens

### Operator pack

- pilot coordination report generation
- coordination delta report generation
- policy-aware visibility and redaction checks
- explain, replay, handoff, and stale-fencing validation
- TUI launch smoke only

### User pack

- Go and Python boundary-client contract checks
- structured HTTP error and denial-message consistency
- notebook JSON and helper smoke
- SDK and HTTP onboarding path correctness

### Exec pack

- Demo 03 story assertions
- Demo 04 story assertions
- Pages preview build
- front-door and flagship-link checks

## Weekly Triage Cadence

The hardening program assumes a standing weekly rhythm.

1. Review the latest hardening artifacts.
2. Promote reproducible `critical` and `high` items into issues immediately.
3. Classify ambiguity as a spec gap instead of silently accepting drift.
4. Decide which `observational` checks should mature into standing assertions.
5. Record whether any hardening subcheck is ready for promotion into blocking CI.

The goal is not to create a bureaucracy.
The goal is to keep the evidence fresh enough that the repo never loses track
of which failures are semantics, which are operations, and which are clarity.

## Promotion Policy

Phase one is diagnostics-first.
The `qa-hardening` workflow is intentionally non-blocking while it proves that
its checks are stable and worth promoting.

A hardening subcheck is eligible for blocker status only after:

- 3 consecutive scheduled green runs
- 1 successful manual or local validation pass

Promotion order:

1. admin package boot, auth, backup/restore, restart/replay
2. operator report, delta, explain, and redaction checks
3. user SDK and HTTP contract checks
4. exec demo and Pages consistency checks

Once a subcheck is promoted, move it into the main `CI` or release-readiness
path instead of leaving it buried in one large catch-all workflow.

## Public Disclosure Posture

This hardening pass prepares the repo for future external scrutiny, but it does
not launch a paid public bug bounty.

Current posture:

- internal-first defect intake
- responsible disclosure for security issues
- public issues for non-sensitive bugs and usability gaps
- semantics discussions for ambiguity and under-specification

If and when a public bounty is launched, this document should be updated in the
same change that updates `SECURITY.md`, issue intake, and workflow posture.
