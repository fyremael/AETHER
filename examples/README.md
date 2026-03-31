# Examples

This directory holds worked examples and end-to-end walkthroughs for the AETHER kernel.

For the operator path and demo-selection guidance, start with `docs/OPERATIONS.md`.

Start here:

| Example | Best use | What it teaches |
| --- | --- | --- |
| `demo-05-ai-support-resolution-desk.md` | ML-facing design-partner and focus-group walkthrough | A governed support case desk with retrieved evidence, candidate resolutions, selected ownership, replay, and proof |
| `demo-04-governed-incident-blackboard.md` | Design-partner and sponsor walkthrough | A governed shared workspace for agents and operators, ready-action derivation, authority handoff, replay, and proof in plain language |
| `demo-03-coordination-situation-room.md` | Executive, operator, or partner showcase | Recursive closure, temporal replay, claim windows, lease handoff, stale fencing, and proof traces in one run |
| `demo-01-temporal-dependency-horizon.md` | First semantic introduction | Temporal replay, recursive closure, and explainable proof traces |
| `demo-02-multi-worker-lease-handoff.md` | Coordination-focused walkthrough | Heartbeat-backed authority, accepted versus fenced outcomes, claimable work, and service-backed explanation |
| `transitive-closure.md` | Technical DSL slice | Parsing through resolution, compilation, and fixed-point evaluation |

Also useful:

- `crates/aether_api/examples/http_kernel_service.rs` starts the current minimal HTTP JSON kernel boundary for technical demonstrations and integration work.
- the `aether_api` integration tests exercise a coordination-focused DSL document with facts, `AsOf` queries, policy annotations, heartbeat-backed authority, accepted outcomes, and stale-result rejection.

For non-technical Windows operators:

- double-click `scripts/run-demo-01.cmd`
- double-click `scripts/run-demo-02.cmd`
- double-click `scripts/run-demo-03.cmd`
- double-click `scripts/run-demo-04.cmd`
- double-click `scripts/run-demo-05.cmd`

The DSL parser is now real for the core authoring path. Some examples still use the Rust AST surface where that is the clearest way to isolate a semantic behavior under test.
