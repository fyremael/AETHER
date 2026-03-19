# Demo 03: Coordination Situation Room

This is the first flagship showcase for the AETHER kernel.

It is meant to answer a simple question: what can the system do now, not in theory, but in one coherent run?

The answer is now substantial.

## What This Demo Proves

In one operator-facing path, AETHER can now:

- replay one append-only journal at multiple semantic cut points
- derive recursive dependency closure
- derive blocked and ready work through stratified negation
- expose claim windows for eligible workers
- model a lease handoff across epochs
- fence stale execution attempts
- explain a current authorized execution with a proof trace

That is a meaningful slice of the kernel’s intended role.

## Story

We model four tasks and two workers.

- `task/1` depends on `task/2`
- `task/2` depends on `task/3`
- `task/3` completes first
- `task/2` completes next
- `task/1` then becomes claimable
- `worker-a` acquires lease epoch `1` for `task/1`
- later the claim moves to `worker-b` and the lease epoch advances to `2`
- `task/4` remains independent and unclaimed throughout

The demo then walks through four semantic moments:

1. `AsOf(e2)`: the recursive dependency chain is already visible, and blocked work is still blocked
2. `AsOf(e4)`: the dependency chain has closed, so claimable work opens
3. `AsOf(e7)`: `worker-a` is the only authorized executor
4. `Current(e9)`: `worker-b` is authorized, stale attempts are fenced, and only the truly open work remains claimable

## Run It

Simplest Windows operator path:

```text
double-click scripts/run-demo-03.cmd
```

Technical path:

```bash
cargo run -p aether_api --example demo_03_coordination_situation_room
```

The Windows launcher writes a timestamped report to `artifacts/demos/demo-03/`.

## Why It Is Compelling

This is not a thin wrapper around a static report.

It is one DSL-authored semantic program, evaluated against one journal, producing multiple principled answers at different points in time through the service boundary. The operator sees not only the answer that is true now, but also how that answer changed, why it changed, and which work is no longer valid.

That is the strongest available statement of AETHER’s progress so far.

## Source

The runnable example lives at:

- `crates/aether_api/examples/demo_03_coordination_situation_room.rs`
