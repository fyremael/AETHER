# Examples

This directory holds worked examples and end-to-end walkthroughs for the AETHER kernel.

For the operator path and demo-selection guidance, start with `docs/OPERATIONS.md`.

Start here:

- `demo-03-coordination-situation-room.md` is the current flagship showcase: recursive closure, temporal replay, claim windows, lease handoff, stale fencing, and proof traces in one operator-facing run.
- `demo-01-temporal-dependency-horizon.md` is the first public showcase: temporal replay, recursive closure, and explainable proof traces over the same journal.
- `demo-02-multi-worker-lease-handoff.md` is the first coordination showcase: multi-worker lease handoff, heartbeat-backed authority, accepted versus fenced outcomes, claimable work, and service-backed explanation.
- `transitive-closure.md` shows the first real recursive vertical slice through the textual DSL path, from parsing through resolution, compilation, and fixed-point evaluation.
- `crates/aether_api/examples/http_kernel_service.rs` starts the current minimal HTTP JSON kernel boundary for technical demonstrations and integration work.
- the `aether_api` integration tests now exercise a coordination-focused DSL document with facts, `AsOf` queries, policy annotations, heartbeat-backed authority, accepted outcomes, and stale-result rejection.

For non-technical Windows operators:

- double-click `scripts/run-demo-01.cmd`
- double-click `scripts/run-demo-02.cmd`
- double-click `scripts/run-demo-03.cmd`

The DSL parser is now real for the core authoring path. Some examples still use the Rust AST surface where that is the clearest way to isolate a semantic behavior under test.
