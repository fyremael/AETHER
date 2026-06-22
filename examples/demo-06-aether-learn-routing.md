# Demo 06: AETHER-Learn Routing Proof

This demo is the first AETHER-Learn proof. It shows a learning control plane before distributed gradient training.

Run from the repository root: `python python/examples/aether_learn_no_regret_routing.py --json`.

The script writes `summary.json`, `summary.csv`, and `ledger.jsonl` under `target/aether-learn/no-regret-routing/`.

The ledger contains TaskTuple, ProposalTuple, RoutingDecisionTuple, RoutingOutcomeTuple, RouterUpdateTuple, and PromotionTuple records. The environment shifts across three phases. The router keeps contextual UCB state by task family and records every learning update as an explicit tuple. Negative outcomes remain in the ledger as evidence.
