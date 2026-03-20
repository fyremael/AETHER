# Operator Guide

This guide is for people running AETHER demonstrations, capturing reports, or presenting the project to others.

It assumes you are not trying to extend the kernel. It assumes you want the cleanest path to showing what the system already does.

## The Short Version

If you only need one showcase, run Demo 03.

On Windows, the simplest path is:

```text
double-click scripts/run-demo-03.cmd
```

That is the current flagship demonstration.

If you need fresh performance numbers for operations, run the performance report:

```text
double-click scripts/run-performance-report.cmd
```

If you need a live visual readout while the suite is running, launch the dashboard:

```text
double-click scripts/run-performance-dashboard.cmd
```

If you need the current durable pilot service rather than a demo, run:

```bash
cargo run -p aether_api --example pilot_http_kernel_service --release
```

That pilot service now starts with bearer-token auth enabled. By default it prints the local pilot token at startup and writes audit events beside the SQLite database as JSONL.

## Demo Catalog

| Demo | Purpose | Best use |
| --- | --- | --- |
| Demo 01 | Temporal replay and recursive closure | Introduce the semantic substrate and `AsOf` model |
| Demo 02 | Multi-worker lease handoff | Show coordination semantics through the service path |
| Demo 03 | Coordination situation room | Present the strongest current end-to-end story in one run |

## Which Demo To Use

### Use Demo 01 when

- the audience needs the foundation first
- you want to show that replay is semantic, not decorative
- you want the clearest minimal recursive example

### Use Demo 02 when

- the audience cares about handoff and stale fencing
- you want a smaller coordination story than the flagship showcase
- you want a direct service-backed example without the larger narrative arc

### Use Demo 03 when

- the audience wants the best available statement of AETHER’s current maturity
- you need one run that shows replay, recursion, claim windows, leases, fencing, and proof traces together
- you want the strongest current demo for review, strategy, or external presentation

## Running Demos

### Windows operator path

Double-click one of these:

- `scripts/run-demo-01.cmd`
- `scripts/run-demo-02.cmd`
- `scripts/run-demo-03.cmd`

Each launcher calls the shared PowerShell runner and pauses before closing so the output remains visible.

### Technical path

You can also run the examples directly:

```bash
cargo run -p aether_explain --example demo_01_temporal_dependency_horizon
cargo run -p aether_api --example demo_02_multi_worker_lease_handoff
cargo run -p aether_api --example demo_03_coordination_situation_room
cargo run -p aether_api --example http_kernel_service
```

The HTTP example starts the current minimal networked kernel boundary on `127.0.0.1:3000`.

The durable pilot example starts the same boundary over a SQLite-backed journal at `artifacts/pilot/coordination.sqlite` unless you provide a custom path.

The authenticated pilot boundary currently adds:

- `GET /v1/audit`
- bearer-token auth for `/v1/*`
- persisted audit logging for pilot requests

Available endpoints today:

- `GET /health`
- `GET /v1/history`
- `POST /v1/append`
- `POST /v1/state/current`
- `POST /v1/state/as-of`
- `POST /v1/documents/parse`
- `POST /v1/documents/run`
- `POST /v1/explain/tuple`

## Reports

Demo reports are written to:

- `artifacts/demos/demo-01/`
- `artifacts/demos/demo-02/`
- `artifacts/demos/demo-03/`

Each run produces:

- a timestamped report file
- `latest.txt`, which points to the most recent run output for that demo

If you need to hand someone the last run without rerunning it, use the `latest.txt` file.

Performance reports are written to:

- `artifacts/performance/`

Each run produces:

- a timestamped markdown report
- `latest.md`, which points to the most recent performance output

For the full engineering-facing performance suite, also run:

```bash
cargo bench -p aether_api
cargo test -p aether_api --test performance_stress --release -- --ignored --nocapture
```

Use the dashboard when people want to watch the measurements arrive in real time. Use the markdown report when you need to hand someone a saved artifact afterward.

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
- `docs/PERFORMANCE.md` for the benchmark harness and interpretation guidance
- `examples/demo-03-coordination-situation-room.md` for the flagship narrative
