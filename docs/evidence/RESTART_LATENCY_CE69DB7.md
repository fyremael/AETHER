# Restart Latency Remediation Evidence: `ce69db7`

## Scope

This diagnostic comparison validates the atomic execution-trace persistence
change. It does not replace the unchanged release performance gate or qualify a
commercial-beta candidate.

- commit: `ce69db7fdf40a8233920bfa160ae66d78fc87a56`
- tree: `230ac3f070f5f98d239208515ee032359a645f1e`
- ref: `refs/heads/codex/aether-restart-latency-investigation`
- host: `dev-chad-windows-native`
- suite: `service_in_process`
- fresh processes: `10`
- retained restart passes: `40`

## Before and after

Both runs used the same tracked runner, host manifest, suite, one sample per
fresh process, and four restart passes per sample.

| Measurement | Before `008516a` | After `ce69db7` | Change |
| --- | ---: | ---: | ---: |
| First-observed total mean | 3,347.816 ms | 30.579 ms | -99.09% |
| First-observed total max | 7,236.504 ms | 43.235 ms | -99.40% |
| First-observed persistence mean | 3,336.844 ms | 20.177 ms | -99.40% |
| First-observed persistence max | 7,225.305 ms | 28.652 ms | -99.60% |
| Subsequent total mean | 15.442 ms | 14.500 ms | -6.10% |
| Subsequent total max | 32.474 ms | 16.566 ms | -48.98% |

After batching, first-observed restart ranged from `23.144 ms` to `43.235 ms`.
The former multi-second first-write tail disappeared without discarding a pass,
changing the arithmetic mean, adding a retry, or moving a threshold.

## Per-process first restart

| Process | Source bundle SHA-256 | Total ns | Execution-persistence ns |
| ---: | --- | ---: | ---: |
| 1 | `497019cc1b9d42e98395ee3432fb5791ed7c5cf8ac3c95acf0888b40d1c376dc` | 33,783,700 | 24,259,900 |
| 2 | `ca472306ca4beacf3d0e01bae632752e3cd95e25b493b8e7a1be27562cc37694` | 34,212,000 | 24,718,300 |
| 3 | `44d5c0f3cbd67f5ffd666783b236028d7c8b0230c2552a5153573fe10c57eefa` | 43,235,000 | 28,652,500 |
| 4 | `47db788614871d6a49b83260011216005d758efe3d4a6f9768b0b324b7bcc230` | 28,883,800 | 18,800,100 |
| 5 | `35f716d450b10141a8fa8c0d5d9a7dd300d3245fa2188e0819876bdfc3bd9e2b` | 28,053,400 | 18,779,900 |
| 6 | `4cfdae7b0c53e968e38e68944bc22a946a88b821216402a4493e63add0fd7809` | 31,385,900 | 20,912,800 |
| 7 | `3dec9075e44147a57437e5978357f176f129e36f820411236ff20c699ffd8dbe` | 23,143,800 | 13,823,100 |
| 8 | `22f933a09287c8b29b46d8fa33e27eb62dfa9a21a8a33343797b7dcd45f1eea5` | 26,396,200 | 15,875,000 |
| 9 | `b6b8093d45d33540a31a3a3b21c282e4ab1f3e05af70a3d69feb654e43449f6f` | 28,857,600 | 18,069,700 |
| 10 | `1973d7de8f7ed41b3424a7462d91e252fb785411d83c1e0bd03acbb726f90bfa` | 27,843,300 | 17,882,100 |

## Correctness boundary

The remediation adds one store operation for an optional execution manifest and
its new traces. The SQLite implementation uses one transaction and one prepared
trace-insert statement. The in-memory implementation validates every handle and
execution reference before mutation. Tests require:

- a duplicate handle rolls back the manifest and every trace in both stores
- the committed SQLite manifest and all traces survive restart
- equivalent executions retain handle reuse and bounded retention behavior
- corrupted or incompatible stored execution material still fails closed

## Hosted status

PR #29 CI attempt 1, run `29629828135`, failed before this remediation at
`Pilot launch gate`. Uploaded artifact `8425153639` recorded service restart
throughput `156.96 rows/s` against baseline `243.47 rows/s` (`-35.53%`) and a
raw full-stack restart sample of `23.14 s`. A new hosted run on the remediated
head is required; this local diagnostic cannot turn that failed attempt green.
