# Restart Latency Connection-Posture Evidence: `0d9e63b`

## Scope

This diagnostic record covers the final local step after atomic trace batching:
aligning the derived execution catalog with AETHER's existing SQLite journal
connection posture.

- commit: `0d9e63b3db5f460a7d1ef7bc7f7c873b339eec5e`
- tree: `8e3b2fdf1401c4b309ea51b5d027a502ad75f172`
- ref: `refs/heads/codex/aether-restart-latency-investigation`
- host: `dev-chad-windows-native`
- execution-catalog posture: WAL, `synchronous=NORMAL`, 5-second busy timeout

This is not official release evidence and does not qualify commercial beta.

## Fresh-process result

Ten clean fresh processes retained all 40 restart passes:

| Classification | Passes | Mean | P95 | Max | Persistence mean | Persistence max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `first_observed_restart` | 10 | 42.140 ms | 48.530 ms | 48.530 ms | 9.670 ms | 11.840 ms |
| `subsequent_restart` | 30 | 15.880 ms | 17.670 ms | 21.850 ms | 5.610 ms | 9.710 ms |

WAL setup moves some work into the open phases, but the single persistence
commit no longer carries the host filesystem-sync tail seen under the default
rollback-journal posture. First persistence remained within an `11.840 ms`
maximum across the ten separate processes.

## Exact baseline/current sequence

The local runner then executed the same sequence used by the hosted pilot gate:
capture one five-sample `service_in_process` baseline and compare three new
five-sample current bundles against it with the tracked arithmetic-mean policy.
All three unchanged drift invocations exited `0`:

| Invocation | Durable restart throughput delta | Severity | Gated overall |
| ---: | ---: | --- | --- |
| 1 | -5.30% | `ok` | `warn` (unrelated coordination-run warning) |
| 2 | -4.42% | `ok` | `ok` |
| 3 | +8.57% | `ok` | `ok` |

No sample was discarded, no retry occurred inside a measurement or verdict,
and no threshold/statistic/policy file changed. The three separate invocations
are diagnostic repetition, not semantic retry logic.

## Hosted attempt history

PR #29 run `29630831553` tested the batched-but-pre-WAL head and still failed
the launch gate. Artifact `8425443118` proved the multi-second defect was gone,
but one first persistence transaction varied from the same-run baseline's
`55.434 ms` to the current run's `175.687 ms`, causing a `-31.15%` arithmetic-
mean throughput delta. That immutable failure motivated aligning the derived
catalog with the already-used journal connection posture. A new hosted run on a
head containing `0d9e63b` is still required.

## Durability boundary

This is not a relaxation invented for performance: the authoritative SQLite
journal already uses WAL plus `synchronous=NORMAL` and a 5-second busy timeout.
The execution catalog stores derived proof material that is digest-checked and
replay-verifiable from the journal. Atomic manifest-plus-trace transactions,
restart durability, handle reuse, corruption detection, backup/restore, and
bounded retention remain tested.
