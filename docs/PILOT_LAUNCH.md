# Pilot Launch

This document defines what “launch-ready” means for the current AETHER pilot and how to prove it with saved artifacts.

The phrase matters. It does not mean “production platform complete.” It means the current single-node coordination pilot is ready for a design-partner launch with a repeatable technical validation path, durable replay, authenticated service access, operator-grade reports, and benchmark drift discipline.

## Launch Scope

The launch target is the current narrow pilot:

1. readiness and claimability across dependency graphs
2. lease authority, lease heartbeats, outcome acceptance, handoff, and stale-result fencing

The launch target is not:

- multi-node coordination
- production multitenancy
- cluster failover
- general workflow replacement
- broad SDK or ecosystem coverage

## Launch Gates

The pilot is launch-ready when all of these are true:

1. durable replay is exact before and after restart
2. authenticated HTTP answers are exact before and after restart
3. operator reports explain why a worker is authorized, why a result is accepted, or why it is fenced
4. semantic audit logs preserve the cut, goal, tuple, and count context operators need
5. benchmark baselines exist and drift is checked against them
6. longer-run soak and misuse drills pass on the launch candidate

The repository now contains all of those gates for the current pilot.

The repo now contains two GitHub Actions paths for this same pack:

- `ci.yml`, where `pilot-launch-gate` makes the launch pack and drift check a required mainline gate
- `pilot-validation.yml`, which keeps the same validation pack available on a schedule or by manual dispatch and uploads the resulting artifacts

## One-Command Validation

Windows operator path:

```text
double-click scripts/run-pilot-launch-validation.cmd
```

Technical path:

```bash
powershell -ExecutionPolicy Bypass -File scripts/run-pilot-launch-validation.ps1
```

CI paths:

- GitHub Actions workflow: `CI`
  - required gate: `pilot-launch-gate`
  - artifacts: pilot report, performance report, drift report, launch transcript
- GitHub Actions workflow: `Pilot Validation`
  - triggers: manual dispatch and scheduled weekly run
  - artifacts: pilot report, performance report, drift report, launch transcript

That validation pack performs these steps in order:

1. generate the current coordination pilot report
2. generate the current release-mode performance report
3. compare the current build to the selected accepted baseline
4. run release-mode `aether_api` tests
5. run the ignored pilot soak suite
6. run the ignored performance stress suite

The validation transcript is written to:

- `artifacts/pilot/launch/latest.txt`
- `artifacts/pilot/launch/pilot-launch-validation-<timestamp>.txt`

## Required Inputs

The launch validation resolves its baseline in this order:

1. an explicit `-BaselinePath`
2. `artifacts/performance/baseline.json`
3. `fixtures/performance/accepted-baseline.windows-x86_64.json`

If you want the current local checkpoint to be the reference point, capture it first:

```bash
cargo run -p aether_api --example capture_performance_baseline --release
```

The tracked fixture baseline makes the launch path reproducible on a fresh QA machine or CI worker. The transcript records both the resolved baseline path and whether it came from an explicit override, a local artifact, or the tracked fixture.

## Expected Output Pack

When launch validation succeeds, the operator artifact set should include:

- `artifacts/pilot/reports/latest.md`
- `artifacts/pilot/reports/latest.json`
- `artifacts/performance/latest.md`
- `artifacts/performance/latest-drift.md` if the drift runner was used separately
- `artifacts/pilot/launch/latest.txt`

Those files answer four different questions:

- What happened in the pilot workload?
- Why is the current coordination answer true?
- How is the kernel performing right now?
- Did the launch candidate survive the soak and stress drills?

## Failure Policy

Treat any of these as launch blockers:

- drift comparison returns a fail-level regression
- the pilot soak suite fails
- the performance stress suite fails
- the release API test pass fails
- the coordination report cannot be generated

Treat warning-level drift as a review point, not an automatic block. The launch owner should record the warning and decide whether it is expected variance or a real concern worth fixing before launch.

## Post-Launch Posture

After launch, the next hardening priorities are:

- deeper operator-intent and semantic-diff audit context
- longer-duration soak windows
- packaged deployment promotion beyond the current single-node Windows pilot bundle
- service hardening beyond the current single-node bearer-token boundary

That is the post-launch road, not a prerequisite for the current pilot launch.
