# Demo 01: Temporal Dependency Horizon

This is the first public demonstration in the AETHER series.

It is designed to showcase the strongest slice of the kernel that is already real:

- append-only datom history
- deterministic `AsOf` replay
- rule compilation
- recursive fixed-point evaluation
- derivation metadata
- proof traces
- source datom provenance

## Why This Demo Comes First

The earliest meaningful thing AETHER can prove is not a dashboard, a CLI, or a service shell. It is semantic continuity through time.

We compile one recursive program once. Then we replay the journal at an earlier prefix and at the present. The same rules produce two different closures, for principled reasons, with visible iteration behavior. That is the project in miniature.

## Story

We model a simple chain of task dependencies:

- `task/1 -> task/2`
- `task/2 -> task/3`
- `task/3 -> task/4`
- `task/4 -> task/5`
- `task/5 -> task/6`

Then we ask two questions:

1. What was the dependency horizon at `AsOf(e3)`?
2. What is the dependency horizon at the current journal head `e5`?

The extensional relation comes from resolved state via the `task.depends_on` attribute. The intensional relation `depends_transitive(x, y)` is derived recursively.

## Run It

Simplest Windows operator path:

```text
double-click scripts/run-demo-01.cmd
```

That launcher captures a timestamped report in `artifacts/demos/demo-01/` and pauses before closing so the output stays visible.

Technical path:

```bash
cargo run -p aether_explain --example demo_01_temporal_dependency_horizon
```

## What You Should See

The demo prints:

- the journal events
- a small compiler summary
- the closure at `AsOf(e3)`
- the closure at `Current(e5)`
- iteration delta sizes for each snapshot
- one highlighted longest-path derivation with supporting source datoms
- a proof trace that walks the tuple graph behind that derivation

At `AsOf(e3)`, the closure stops at `task/4`.

At `Current(e5)`, the closure reaches all the way to `task/6`.

That difference is the point. The kernel is not merely storing facts; it is replaying semantics.

## Why It Matters

This demo shows that AETHER has crossed an important line:

- the journal is not inert
- the resolver is not decorative
- the rule compiler is not a placeholder
- the runtime is not rhetorical

The system can already state a recursive semantic claim, replay history, and derive different truths at different cut points of the same append-only history.

It can now also show which journal elements support that claim.

## Source

The runnable example lives at:

- `crates/aether_explain/examples/demo_01_temporal_dependency_horizon.rs`

The underlying runtime tests live in:

- `crates/aether_runtime/src/lib.rs`
