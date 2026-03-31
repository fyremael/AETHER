# Operator Guide

This guide is for people running AETHER demonstrations, capturing reports, or presenting the project to others.

It assumes you are not trying to extend the kernel. It assumes you want the cleanest path to showing what the system already does.

## The Short Version

If you are operating a live pilot service, start with the operator cockpit.

Inside a packaged bundle, the shortest path is:

```text
double-click run-aether-ops.cmd
```

That launches `aetherctl tui` against the local authenticated pilot service
with the packaged operator token.

If you only need one showcase rather than a live service cockpit, run Demo 03.

If you need the clearest ML-facing product walkthrough for design partners, run
Demo 05.

On Windows, the simplest path is:

```text
double-click scripts/run-demo-05.cmd
```

That is the current ML-facing customer-facing exemplar.

If you need the broader governed shared workspace reference pattern rather than
the support-desk application wedge, run Demo 04.

If you need the strongest operator-proof showcase, run:

```text
double-click scripts/run-demo-03.cmd
```

That remains the strongest current kernel-proof demonstration.

If you need fresh performance numbers for operations, run the performance report:

```text
double-click scripts/run-performance-report.cmd
```

If you need a live visual readout while the suite is running, launch the dashboard:

```text
double-click scripts/run-performance-dashboard.cmd
```

If you need a saved coordination incident/explain artifact for the pilot workload, run:

```text
double-click scripts/run-pilot-report.cmd
```

If you need a saved “what changed between cuts?” artifact for the same pilot
workload, run:

```text
double-click scripts/run-pilot-delta-report.cmd
```

If you need to check whether the latest build regressed against the current accepted baseline, run:

```text
double-click scripts/run-performance-drift.cmd
```

That comparison is now host-aware and suite-aware. On the canonical Windows dev host it resolves against `artifacts/performance/baselines/<suite>/<host>.json` first and then `fixtures/performance/baselines/<suite>/<host>.json`, with `core_kernel` and `service_in_process` as the current accepted gates.

If you need the full launch candidate validation pack for the current design-partner pilot, run:

```text
double-click scripts/run-pilot-launch-validation.cmd
```

If you need a packaged pilot service bundle for deployment review or handoff, run:

```text
double-click scripts/build-pilot-package.cmd
```

If you need the broader release candidate evidence pack rather than only the
pilot launch pack, run:

```text
double-click scripts/run-release-readiness.cmd
```

If you need the deployment and upgrade runbook for that bundle, read:

- `docs/PILOT_OPERATIONS_PLAYBOOK.md`

If you need the current durable pilot service rather than a demo, run:

```bash
cargo run -p aether_api --bin aether_pilot_service --release -- --config <path-to-config>
```

That hardened pilot service now starts only from an explicit deployment config and uses a secret-backed auth token source instead of a baked-in default token. The packaged deployment path keeps the bearer token in a config-relative secret file by default and writes audit events beside the SQLite database as JSONL.

The same config model can also fetch tokens from an external secret-manager CLI or broker script with `token_command`, so operators can keep package-local files out of the trust path when needed.

Those audit entries now carry semantic context, not just endpoint metadata. For the current pilot path that includes the temporal cut, the query goal, tuple IDs for explain requests, and result-count summaries where they exist.

## Choose By Question

Use this guide by the question you need answered:

| If you need to know... | Use |
| --- | --- |
| “What is happening in the live pilot service right now?” | `run-aether-ops.cmd` inside the package |
| “What is the clearest ML-facing design-partner walkthrough?” | Demo 05 |
| “What is the broader governed-workspace reference pattern?” | Demo 04 |
| “What is the clearest single showcase?” | Demo 03 |
| “How does replay and closure work?” | Demo 01 |
| “How do heartbeats, handoff, and fencing work?” | Demo 02 |
| “Why was this worker authorized or fenced?” | `run-pilot-report.cmd` |
| “What changed between two important cuts?” | `run-pilot-delta-report.cmd` or the `Delta` tab in `run-aether-ops.cmd` |
| “Did performance drift?” | `run-performance-drift.cmd` |
| “Is this exact pilot candidate launch-ready?” | `run-pilot-launch-validation.cmd` |
| “Is this exact tree release-ready?” | `run-release-readiness.cmd` |
| “Can I hand someone a packaged service bundle?” | `build-pilot-package.cmd` |

## Demo Catalog

| Demo | Purpose | Best use |
| --- | --- | --- |
| Demo 01 | Temporal replay and recursive closure | Introduce the semantic substrate and `AsOf` model |
| Demo 02 | Multi-worker lease handoff | Show heartbeat-backed authority and outcome fencing through the service path |
| Demo 03 | Coordination situation room | Present the strongest current end-to-end story in one run |
| Demo 04 | Governed incident blackboard | Explain AETHER as a shared governed workspace for agents and operators |
| Demo 05 | AI support resolution desk | Explain AETHER through a relevant ML-facing support application with retrieved evidence and governed handoff |

## Which Demo To Use

### Use Demo 01 when

- the audience needs the foundation first
- you want to show that replay is semantic, not decorative
- you want the clearest minimal recursive example

### Use Demo 02 when

- the audience cares about handoff, heartbeats, and stale-result fencing
- you want a smaller coordination story than the flagship showcase
- you want a direct service-backed example without the larger narrative arc

### Use Demo 03 when

- the audience wants the best available statement of AETHER’s current maturity
- you need one run that shows replay, recursion, claim windows, leases, heartbeat-backed authority, fencing, and proof traces together
- you want the strongest current demo for review, strategy, or external presentation

### Use Demo 04 when

- the audience needs product utility before kernel vocabulary
- you want a design-partner-ready story about a shared incident board for agents and operators
- you want to show observations, candidate actions, governed authority, replay, and proof in a 5-10 minute screen share

## Running Demos

### Windows operator path

Double-click one of these:

- `scripts/run-demo-01.cmd`
- `scripts/run-demo-02.cmd`
- `scripts/run-demo-03.cmd`
- `scripts/run-demo-04.cmd`
- `scripts/run-demo-05.cmd`

Each launcher calls the shared PowerShell runner and pauses before closing so the output remains visible.

### Technical path

You can also run the examples directly:

```bash
cargo run -p aether_explain --example demo_01_temporal_dependency_horizon
cargo run -p aether_api --example demo_02_multi_worker_lease_handoff
cargo run -p aether_api --example demo_03_coordination_situation_room
cargo run -p aether_api --example demo_04_governed_incident_blackboard
cargo run -p aether_api --example demo_05_ai_support_resolution_desk
cargo run -p aether_api --example http_kernel_service
```

The HTTP example starts the current minimal networked kernel boundary on `127.0.0.1:3000`.

The durable pilot example starts the same boundary over a SQLite-backed journal at `artifacts/pilot/coordination.sqlite` unless you provide a custom path.

The authenticated pilot boundary currently adds:

- `GET /v1/audit`
- bearer-token auth for `/v1/*`
- persisted audit logging for pilot requests
- semantic audit context for pilot query and explain actions
- SQLite-backed sidecar replay for artifact and vector registrations on the durable pilot path
- journal-anchored sidecar registration: append the anchor datom first, then register the artifact or vector payload against that current tail element

Available endpoints today:

- `GET /health`
- `GET /v1/history`
- `GET /v1/audit`
- `GET /v1/status`
- `POST /v1/admin/auth/reload`
- `POST /v1/append`
- `POST /v1/state/current`
- `POST /v1/state/as-of`
- `POST /v1/documents/parse`
- `POST /v1/documents/run`
- `POST /v1/reports/pilot/coordination`
- `POST /v1/reports/pilot/coordination-delta`
- `POST /v1/explain/tuple`
- `POST /v1/sidecars/artifacts/register`
- `POST /v1/sidecars/artifacts/get`
- `POST /v1/sidecars/vectors/register`
- `POST /v1/sidecars/vectors/search`

## Reports

Demo reports are written to:

- `artifacts/demos/demo-01/`
- `artifacts/demos/demo-02/`
- `artifacts/demos/demo-03/`
- `artifacts/demos/demo-04/`
- `artifacts/demos/demo-05/`

Each run produces:

- a timestamped report file
- `latest.txt`, which points to the most recent run output for that demo

If you need to hand someone the last run without rerunning it, use the `latest.txt` file.

Performance reports are written to:

- `artifacts/performance/`

The release-mode report produces:

- a timestamped markdown report
- `latest.md`, which points to the most recent performance output

The baseline capture produces:

- a suite-specific local baseline under `artifacts/performance/baselines/<suite-id>/<host-id>.json`

The drift runner produces:

- a timestamped markdown drift report
- `latest-drift.md`, which points to the most recent drift capture
- `latest-drift-core_kernel.md` and `latest-drift-service_in_process.md` for the current gated suites

The matrix runner produces:

- `artifacts/performance/matrix/latest.md`
- `artifacts/performance/matrix/latest.json`

Pilot coordination reports are written to:

- `artifacts/pilot/reports/`

Each run produces:

- a timestamped markdown report
- a timestamped JSON report
- `latest.md`
- `latest.json`

Pilot coordination delta reports are written to the same directory:

- `artifacts/pilot/reports/`

Each run produces:

- a timestamped markdown delta report
- a timestamped JSON delta report
- `latest-delta.md`
- `latest-delta.json`

Pilot launch-validation transcripts are written to:

- `artifacts/pilot/launch/`

Each run produces:

- a timestamped text transcript
- `latest.txt`

The launch transcript also records which accepted baseline was used: explicit override, local artifact, or tracked fixture.

The same launch pack now runs in two GitHub Actions paths:

- the required `pilot-launch-gate` job in the main `CI` workflow
- the dedicated scheduled/manual `pilot-validation` workflow, which uploads the generated report, drift, and launch transcript artifacts for review

For the full engineering-facing performance suite, also run:

```bash
cargo bench -p aether_api
cargo test -p aether_api --test performance_stress --release -- --ignored --nocapture
```

Use the dashboard when people want to watch the measurements arrive in real time. Use the markdown report when you need to hand someone a saved artifact afterward.

Use the pilot report when someone asks, “Why is this worker authorized, or why was this reported result fenced right now?” Use the drift report when someone asks, “Did this change materially slow the pilot path down?”
Use the pilot delta report when someone asks, “What changed between the earlier cut and now?”
Use the launch validation pack when someone asks, “Is this exact pilot candidate ready to go?”
Use the operations playbook when someone asks, “How do we deploy, rotate, upgrade, or roll back this pilot safely?”

## Replicated Prototype

The replicated authority-partition prototype is now exposed as an example
service rather than a packaged operator bundle.

Run it with:

```bash
cargo run -p aether_api --example replicated_partition_http_service --release
```

That prototype adds:

- `GET /v1/partitions/status`
- `POST /v1/partitions/append`
- `POST /v1/partitions/promote`
- `POST /v1/federated/history`
- `POST /v1/federated/run`
- `POST /v1/federated/report`

Use it when you need to show:

- exact local truth per authority partition
- manual leader/follower failover with epoch fencing
- federated imported-fact reasoning without a fake global clock

Do not present it as a generalized distributed platform. It is a deliberate,
single-host prototype for the next architectural step.

## How To Present The Output

The operator framing that tends to work best is:

1. Start with the journal events.
2. Show the semantic cut you care about.
3. Show the derived answer at that cut.
4. Show how the answer changes at a later cut.
5. End with the proof trace for why the current answer is true.

That sequence mirrors how AETHER itself works: history, cut, derivation, change, explanation.

## Troubleshooting

### If the runner says `cargo` is missing

The Rust toolchain is not visible on `PATH`.

Use the installed development environment or ask the platform team to restore the AETHER Rust toolchain before running demos.

### If the demo output does not match the narrative

Treat that as a documentation bug or example bug, not as normal drift.

The example, narrative, and runner output are expected to agree. If they do not, open an issue and capture the generated report.

### If you need the strongest single showcase

Run Demo 03 and keep the report from that run. It is the best current summary of AETHER’s progress.

## Related Documents

- `examples/README.md` for the example catalog
- `scripts/README.md` for launcher details
- `docs/PILOT_LAUNCH.md` for the launch-readiness contract and full validation pack
- `docs/PERFORMANCE.md` for the benchmark harness and interpretation guidance
- `examples/demo-03-coordination-situation-room.md` for the flagship narrative
