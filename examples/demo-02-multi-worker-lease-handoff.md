# Demo 02: Multi-Worker Lease Handoff

This is the second public demonstration in the AETHER series.

It shows a coordination story closer to the project’s real destination:

- one task blocked by a dependency and later controlled by a lease
- two workers competing across lease epochs
- a clean handoff from one worker to another
- authorized execution separated from fenced stale execution
- unclaimed ready work exposed to eligible workers
- proof traces reconstructed through the service boundary

## Why This Demo Comes Next

Demo 01 proved that AETHER can replay time and derive recursive closure.

Demo 02 proves something more operational: the same kernel can answer coordination questions that an operator actually cares about.

Who is allowed to run this task right now? Who has gone stale? Which task is still open for claim? What changed between an earlier cut of the journal and the present?

## Story

We model three tasks and two workers:

- `task/1` depends on `task/2`
- `task/2` is completed
- `task/3` is independent and unclaimed
- `worker-a` holds lease epoch `1` for `task/1` at `AsOf(e5)`
- later, the claim moves to `worker-b` and the lease epoch advances to `2`

We also record four execution attempts as DSL facts:

- `worker-a` attempts `task/1` at epoch `1`
- `worker-b` attempts `task/1` at epoch `1`
- `worker-a` attempts `task/1` at epoch `2`
- `worker-b` attempts `task/1` at epoch `2`

The rules derive:

- the single authorized execution attempt at a given cut of history
- all fenced stale attempts
- tasks that are ready and eligible for claim by an available executor

## Run It

Simplest Windows operator path:

```text
double-click scripts/run-demo-02.cmd
```

Technical path:

```bash
cargo run -p aether_api --example demo_02_multi_worker_lease_handoff
```

The Windows launcher writes a timestamped report to `artifacts/demos/demo-02/`.

## What You Should See

The demo prints:

- the journal handoff sequence
- the single authorized execution at `AsOf(e5)` as `task/1, worker-a, 1`
- the single authorized execution at `Current` as `task/1, worker-b, 2`
- the fenced stale attempts that are no longer valid
- the currently claimable work, which should be `task/3` for both workers
- a proof trace for the current authorized execution tuple

The contrast is the point. The kernel is not only telling us that a lease exists; it is drawing the boundary between valid and invalid work as the journal moves forward.

## Why It Matters

This demo is the first credible glimpse of AETHER as a coordination kernel rather than a recursive substrate alone.

The same append-only history now supports:

- temporal replay
- recursive and stratified derivation
- lease fencing
- operator-facing answers through the service boundary

That is much closer to the system AETHER is meant to become.

## Source

The runnable example lives at:

- `crates/aether_api/examples/demo_02_multi_worker_lease_handoff.rs`

The service-level integration coverage lives at:

- `crates/aether_api/src/lib.rs`
