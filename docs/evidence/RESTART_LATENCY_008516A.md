# Restart Latency Evidence: `008516a`

## Scope

This is a diagnostic record, not release evidence and not a performance-gate
verdict. It records ten fresh native Windows processes produced from one clean
instrumentation candidate:

- commit: `008516a1187958446aa25d29ff58c6695db6f230`
- tree: `b3b5bd45bb60d6e16179eb5e28dd22613f0e79d6`
- ref: `refs/heads/codex/aether-restart-latency-investigation`
- host: `dev-chad-windows-native`
- suite: `service_in_process`
- samples per process: `1`
- restart passes per sample: `4`

The tracked runner rejected dirty or mixed-candidate inputs, hashed every source
bundle, retained all 40 restart passes, and classified exactly one pass per
fresh process as first-observed. It did not filter, retry, warm up, or produce a
promotion verdict.

## Result

| Classification | Passes | Min | Mean | P50 | P95 | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `first_observed_restart` | 10 | 2,129.070 ms | 3,347.816 ms | 2,259.399 ms | 7,236.504 ms | 7,236.504 ms |
| `subsequent_restart` | 30 | 13.642 ms | 15.442 ms | 14.861 ms | 18.036 ms | 32.474 ms |

For first-observed restarts, `run.execution_persistence` averaged
`3,336.844 ms`, ranged from `2,118.864 ms` to `7,225.305 ms`, and accounted
for `99.67%` of total observed time. By contrast:

- all three open/configure/schema phases together averaged under `4 ms`
- policy replay averaged `0.028 ms`
- state resolution averaged `0.061 ms`
- semi-naive execution averaged `2.559 ms`
- query and explanation materialization averaged `2.143 ms`
- harness-unattributed time averaged `0.244 ms`

Subsequent restart persistence averaged `5.422 ms`. This rules out SQLite open,
journal replay, state resolution, recursive execution, and harness scheduling
as the source of the multi-second first-restart anomaly on this candidate.

## Per-process first restart

| Process | Source bundle SHA-256 | Total ns | Execution-persistence ns |
| ---: | --- | ---: | ---: |
| 1 | `d09c5f493b6ef39bec6e4a911a141461f05a11c3c25de1d290796b80590a7cf2` | 2,198,473,700 | 2,188,625,000 |
| 2 | `a4e1fbae147c618a2934dbad00ac89a268858322500ab10fefe9697ff5b53b4d` | 2,161,447,300 | 2,148,662,500 |
| 3 | `40d08227eb49cd94656522e86ad60771b4a7cf6315a47f3e0fa5d6c08d4ff138` | 2,836,610,900 | 2,826,533,500 |
| 4 | `f140a6d0a6589c0d0f53d859061d849570b21e7fde15107d24e3b0e7c55f6cee` | 2,505,624,900 | 2,493,771,500 |
| 5 | `900c52b010d56e21af37ce534c348e5328a835da2b9a009adb409ac7773b8199` | 7,236,503,700 | 7,225,305,400 |
| 6 | `911e63af69c24727f113fafad3e23f576e5582cdb065aa0b58bf68160ffe2cb2` | 5,091,545,500 | 5,079,004,000 |
| 7 | `067ade0cab2fcea4cc66c64c11910dff8aa2451608d5776fb234c24396a78515` | 2,177,230,400 | 2,167,352,400 |
| 8 | `8529226f4ac42837c5f27cb97dcd9ea2831bcaa4ccd55886cdb2f3a99582745b` | 2,259,398,900 | 2,248,910,100 |
| 9 | `f6accdff702034f8cc45580c1c483760f51c79ddaf3ee0eae47622d437875554` | 4,882,258,200 | 4,871,411,100 |
| 10 | `e5129ce7fee23bb4d95ccb9c9c8bb5744f6de8acdb36baf2f684467e81f9ef68` | 2,129,070,300 | 2,118,864,300 |

## Implementation-path finding

The measured behavior matches the persistence implementation:

1. a first execution creates one durable trace record for every derived tuple;
2. `SqliteExecutionStore::put_trace` issues each insert separately without an
   enclosing batch transaction;
3. SQLite therefore commits each new trace independently;
4. later equivalent restarts load and reuse existing traces, so they avoid that
   first-write sequence.

This is the narrow remediation target. A follow-up change should make creation
of one execution manifest and its trace set atomic and batched, preserve opaque
handle identity/reuse/corruption checks/retention behavior, add rollback and
restart tests, and then repeat this exact diagnostic before selecting a new
protected qualification candidate.

## Claim boundary

Candidate `11380eed81d0690717637a6926ae0087547205c2` remains a failed
qualification candidate. Candidate `008516a1187958446aa25d29ff58c6695db6f230`
is an investigation candidate only. Neither may produce a beta-promotion
record, and controlled alpha remains unchanged.
