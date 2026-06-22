# Demo 06: AETHER-Learn Routing Proof

This demo is the first AETHER-Learn proof. It shows a learning control plane before distributed gradient training.

## Dependency-free proof

Run from the repository root: `python python/examples/aether_learn_no_regret_routing.py --json`.

The script writes `summary.json`, `summary.csv`, and `ledger.jsonl` under `target/aether-learn/no-regret-routing/`.

The ledger contains TaskTuple, ProposalTuple, RoutingDecisionTuple, RoutingOutcomeTuple, RouterUpdateTuple, and PromotionTuple records. The environment shifts across three phases. The router keeps contextual UCB state by task family and records every learning update as an explicit tuple.

## Service-backed report

Run the AETHER service-backed report:

```bash
cargo run -p aether_api --example demo_06_aether_learn_service_report
```

This example maps the six proof tuple families into service datoms, runs a DSL program through `InMemoryKernelService`, and prints:

- service-backed joined routing records;
- accepted router updates;
- retained router updates;
- a proof trace for the first accepted update.

The important step is that accepted versus retained updates are no longer explained by an offline script alone. They are derived by the AETHER rule engine from journal-backed learning artifacts.
