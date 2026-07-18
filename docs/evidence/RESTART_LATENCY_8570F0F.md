# Restart Latency Close-Checkpoint Evidence: `8570f0f`

## Scope

This diagnostic record covers the derived execution catalog's final local
latency boundary: SQLite must not checkpoint its WAL merely because a benchmark
or short-lived service process closes its last connection.

- commit: `8570f0f4c28aacedad75155e60da32264a132077`
- tree: `c0c05783f85a334c5fdd244e28f36d3bea2d5895`
- ref: `refs/heads/codex/aether-restart-latency-investigation`
- host: `dev-chad-windows-native`
- suite: `service_in_process`
- aggregate diagnostic SHA-256:
  `a2f94427d53c6d6f3d76d991399d958be4bca8242816daf56562ba1de6fe59c4`

This is diagnostic evidence only. It is not an official release subject and
does not qualify commercial beta.

## Hosted localization

PR #29 run `29631200293` exercised the WAL-aligned head and retained failed
launch-gate artifact `8425569262`. Batching and WAL reduced first persistence
to `14.367 ms`, but the first current process spent `536.132 ms` in
`service_close` versus `44.739 ms` for the same-run baseline. The tracked
arithmetic mean therefore reported a `-60.75%` durable-restart throughput
delta.

The shift from persistence to close identified SQLite's last-connection WAL
checkpoint as the remaining host-sensitive tail. The execution catalog now
sets `SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE`; ordinary automatic checkpoints remain
enabled while the connection is open.

## Fresh-process result

Ten clean native Windows processes retained all 40 restart passes:

| Classification | Passes | Mean | P95 | Max | Persistence mean | Persistence max | Close mean | Close max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `first_observed_restart` | 10 | 30.408 ms | 44.156 ms | 44.156 ms | 6.084 ms | 7.239 ms | 0.808 ms | 0.985 ms |
| `subsequent_restart` | 30 | 25.347 ms | 39.706 ms | 42.615 ms | 5.202 ms | 6.003 ms | 0.819 ms | 1.198 ms |

The result keeps first-observed and subsequent passes separate and retains
every raw pass. Replay, policy projection, resolution, and semi-naive execution
remain millisecond-scale; the close path no longer carries an implicit WAL
checkpoint.

## Exact baseline/current sequence

One five-sample local baseline was followed by five independent five-sample
current captures. Every unchanged drift invocation exited `0` and reported the
durable-restart workload as `ok`:

| Invocation | Durable restart throughput delta |
| ---: | ---: |
| 1 | +35.70% |
| 2 | +28.00% |
| 3 | +29.67% |
| 4 | +37.36% |
| 5 | +40.52% |

No raw sample, arithmetic-mean statistic, threshold, or retry rule changed.
These repeated local invocations are diagnostic confidence checks, not retries
inside a release measurement or verdict.

## Durability boundary

`SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE` does not discard committed WAL records.
The package backup contract already requires a stopped or quiesced service and
copies each SQLite database together with any `-wal` and `-shm` companions.
The execution-store restore regression now snapshots those exact files, removes
the live set, restores the set, reopens it, and resolves the persisted trace.
The authoritative journal cut and its derived proof metadata still have to be
captured at one operational cut.

Hosted CI on the final pull-request head and a completely new protected
candidate remain mandatory. This result cannot repair or promote failed
candidate `11380eed81d0690717637a6926ae0087547205c2`.
