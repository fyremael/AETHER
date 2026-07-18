# Restart Latency Investigation

## Decision status

Commercial beta remains blocked. Protected candidate
`11380eed81d0690717637a6926ae0087547205c2` passed exact-SHA CI, Supply
Chain, Pages, and Capacity Planning, but Release Readiness run `29625522886`
failed the predeclared service performance gate. No official release-evidence
bundle or passed verdict exists for that candidate, so no promotion record or
beta claim may be created.

This investigation is diagnostic only. It does not change the five raw samples,
four restart passes per sample, arithmetic-mean statistic, regression threshold,
or fail-closed behavior of the release gate.

## Observed anomaly

The failed run retained these raw sample durations for durable coordination
restart/replay:

- same-run baseline: `6.9986421 s`, `48.8598 ms`, `48.3526 ms`,
  `48.8769 ms`, `48.4733 ms`
- protected candidate: `17.2430404 s`, `51.0047 ms`, `53.2219 ms`,
  `52.9339 ms`, `51.8038 ms`

The candidate arithmetic mean was `3.49040094 s`, yielding `187.9440 rows/s`
and a `-58.78%` throughput delta. The first observed restart dominated both
sets; subsequent samples remained in a narrow approximately `48-53 ms` band.
That pattern localizes the investigation to first-process/first-restart work,
but it does not yet identify a cause.

## Diagnostic contract

Restart benchmark measurements now retain every pass inside every raw sample.
Each pass has a total duration, a first-observed or subsequent classification,
and non-overlapping phase durations for:

- execution-store, journal, and sidecar open/configure/schema work
- document parsing and active-schema validation
- journal history read and policy replay
- evaluation identity, state resolution, and semi-naive execution
- query/explain materialization and execution-receipt persistence
- service close and any remaining harness-only time

The ordinary service API still runs through the same implementation without
collecting timings. Diagnostic collection is exposed as a hidden Rust surface
used by the performance crate, not as a new kernel, DSL, HTTP, Go, or Python SDK
contract.

`scripts/run-restart-latency-diagnostics.ps1` builds once, then starts a fresh
release-mode performance process for each repetition. It refuses a dirty
tracked worktree, binds every source bundle to one commit, tree, ref, host, and
suite, and never updates a mutable `latest` path. The aggregator re-hashes each
source bundle, rejects missing/duplicated/misclassified/over-attributed pass
data, retains all raw passes, and reports first-observed and subsequent
distributions separately.

Example:

```powershell
pwsh -File scripts/run-restart-latency-diagnostics.ps1 `
  -Suite service_in_process `
  -Runs 10 `
  -Samples 1
```

## Interpretation rules

- A journal-open spike includes SQLite connection configuration, schema checks,
  and any busy-timeout wait; it is not automatically evidence of replay cost.
- A replay, resolution, or semi-naive spike must be attributed to its named
  phase before optimization work begins.
- A harness-unattributed spike requires host/process investigation rather than
  a semantic-kernel change.
- First-observed and subsequent restarts must remain separate in diagnostic
  summaries. Neither may be discarded from the official raw samples.
- Diagnostic artifacts cannot satisfy the performance subject, author a
  release verdict, or support a promotion record.

## Exit criteria

The investigation can close only when repeated fresh-process native Windows
runs localize the stall, the responsible behavior is fixed or bounded without
weakening the gate, regression coverage is in place, and a new protected
`main` candidate passes the unchanged official and independent qualification
sequence. Candidate `11380eed81d0690717637a6926ae0087547205c2` remains a
failed qualification candidate permanently; it must not be retrospectively
promoted.

## First diagnostic result

Ten fresh native Windows processes from clean instrumentation commit
`008516a1187958446aa25d29ff58c6695db6f230` localized `99.67%` of
first-observed restart time to execution-receipt and trace persistence. SQLite
open, journal replay, policy replay, resolution, semi-naive execution, and
harness-only time remained millisecond-scale. The exact per-process hashes and
phase distributions are recorded in
`docs/evidence/RESTART_LATENCY_008516A.md`.

Code inspection matches the measurement: a new execution persists each derived
tuple trace with a separate SQLite insert outside a shared transaction, while
equivalent later restarts reuse the stored traces. The next bounded change is an
atomic batch-persistence contract with rollback/restart/identity regression
coverage, followed by the same fresh-process diagnostic and a new protected
qualification candidate.

That bounded change is now implemented on investigation commit
`ce69db7fdf40a8233920bfa160ae66d78fc87a56`. Ten fresh processes reduced
first-observed mean restart latency from `3,347.816 ms` to `30.579 ms` while
subsequent restarts remained stable. The before/after hashes, raw phase
summaries, and correctness boundary are recorded in
`docs/evidence/RESTART_LATENCY_CE69DB7.md`. Hosted CI and the official protected
candidate sequence remain required.

The next hosted attempt showed that batching removed the seconds-long defect
but left one filesystem-sync tail on the default SQLite rollback journal. The
derived execution catalog now uses the same WAL, `synchronous=NORMAL`, and
5-second busy-timeout posture as AETHER's authoritative SQLite journal. Ten
fresh processes bounded first persistence to `11.840 ms`; three separate local
baseline/current drift invocations all passed the unchanged durable-restart
gate. Exact results are in `docs/evidence/RESTART_LATENCY_0D9E63B.md`.
