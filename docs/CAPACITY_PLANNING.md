# Capacity Planning

This document is the internal sizing guide for AETHER after the v1 closeout.

It answers four planning questions with measured evidence rather than narrative
alone:

- how large a single pilot node can safely get
- what hardware class we should recommend
- what the current single-node ceiling is
- when we should stop scaling up and partition or federate instead

These numbers are internal planning guidance, not public SLAs.

## Current Measured Readout

The current live artifacts are:

- `artifacts/performance/capacity/latest.json`
- `artifacts/performance/capacity/latest.md`
- `artifacts/performance/perturbation/latest.json`
- `artifacts/performance/perturbation/latest.md`

The current default recommendation is:

- node class: `M`
- hardware: `16 vCPU`, `64 GB RAM`, `500 GB NVMe`
- reference workload: governed incident board / pilot coordination surface

Current measured single-node envelope from the latest canonical Windows host
run:

- `M`: `1,024` pilot-board tasks
- `M`: `32` mixed operator-service concurrent workers
- `M`: `10,000` durable replay entities

The current limiting factor for the default `M` recommendation is
`report_latency`, not restart/replay or local storage.

## Internal Node Classes

The sizing system uses four internal node classes:

| Class | vCPU | RAM | NVMe | p95 target | Replay target |
| --- | ---: | ---: | ---: | ---: | ---: |
| `S` | 8 | 32 GB | 250 GB | 2.5 s | 90 s |
| `M` | 16 | 64 GB | 500 GB | 2.0 s | 60 s |
| `L` | 32 | 128 GB | 1 TB | 1.5 s | 45 s |
| `XL` | 64 | 256 GB | 2 TB | 1.0 s | 30 s |

Current conservative envelopes from the latest planner run:

| Class | Pilot board | Mixed concurrency | Durable replay | Limiting factor |
| --- | ---: | ---: | ---: | --- |
| `S` | `1,024` tasks | `32` | `10,000` entities | `memory` |
| `M` | `1,024` tasks | `32` | `10,000` entities | `report_latency` |
| `L` | `4,096` tasks | `32` | `10,000` entities | `report_latency` |
| `XL` | `4,096` tasks | `32` | `10,000` entities | `report_latency` |

Those envelopes are intentionally capped at the largest measured ladder points
that still satisfy the planner thresholds. We do not extrapolate aggressive
upper bounds just because the node class is larger.

## What Limits A Single Node Today

There are two different ceilings to keep separate.

### 1. Customer-shaped board ceiling

The current measured board ladder shows:

- `1,024` tasks: about `93 ms` mean coordination latency
- `4,096` tasks: about `1.28 s` mean coordination latency
- `8,192` tasks: about `4.76 s` mean coordination latency

That is why the default `M` recommendation stops at `1,024` tasks today. The
planner treats the board ceiling as **near**, not because the kernel breaks,
but because coordination/report latency moves too far once the board widens
beyond the current comfortable operator cell.

### 2. Recursive closure memory ceiling

The current measured closure ladder shows:

- `chain 512`: about `389 MiB`
- `chain 1,024`: about `2.85 GiB`

The current perturbation projections then put the next larger closure shapes at
roughly:

- `chain 2,048`: about `11.4 GiB`
- `chain 4,096`: about `45.7 GiB`

That means planetary scale will not come from one ever-larger monolithic
closure. The right posture remains exact local operator cells plus explicit
federation across bounded cuts.

## Storage And Replay Planning

The storage planner now measures:

- SQLite file growth versus datom count
- WAL and SHM size when present
- backup snapshot size
- restore/replay time
- sidecar catalog size
- peak process RSS during major durable ladders

The current default `M` envelope is still light on local storage:

- steady-state storage: about `346.8 KiB`
- 30-day retained journal budget: about `10.16 MiB`
- backup/restore scratch budget: about `693.6 KiB`

That means storage is not the current blocker for the pilot-shaped workload.
The practical bottleneck remains board/report latency first, closure growth
second.

## Scale-Out Doctrine

When the planner moves from “scale up” to “partition / federate,” use this
order:

1. add snapshot or checkpoint acceleration if replay becomes the dominant limit
2. partition by workspace, incident domain, or tenant if board size or closure
   size becomes the dominant limit
3. federate across explicit cuts once those one-node operator cells are well
   bounded
4. do not chase one giant node beyond `XL`

This is the core doctrine:

**scale one exact operator cell responsibly, then widen by partition and
federation rather than by semantic collapse.**

## Commands

Local operator path:

```text
double-click scripts/run-capacity-planner.cmd
```

Technical paths:

```bash
powershell -ExecutionPolicy Bypass -File scripts/run-capacity-planner.ps1 -SkipHardening
```

```bash
cargo run -p aether_api --example performance_capacity_curves --release -- \
  --host-manifest fixtures/performance/hosts/dev-chad-windows-native.json \
  --output-json artifacts/performance/perturbation/runs/<timestamp>/capacity-curves.json \
  --output-report artifacts/performance/perturbation/runs/<timestamp>/capacity-curves.md
```

```bash
cargo run -p aether_api --example performance_capacity_report --release -- \
  --perturbation-json artifacts/performance/perturbation/latest.json \
  --matrix-json artifacts/performance/matrix/latest.json \
  --capacity-inputs-json artifacts/performance/perturbation/runs/<timestamp>/capacity-curves.json \
  --output-json artifacts/performance/capacity/latest.json \
  --output-report artifacts/performance/capacity/latest.md
```

## GitHub Tracking

The live GitHub workflow is:

- `.github/workflows/capacity-planning.yml`

That workflow:

- runs a scheduled/manual perturbation sweep
- builds a fresh matrix summary
- publishes the capacity report artifact
- updates the standing capacity tracker issue
- opens a follow-up issue automatically when the recommendation, limiting
  factor, or ceiling status changes materially

This keeps capacity guidance as a live planning surface instead of a stale
brief.
