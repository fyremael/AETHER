# No-Regret Routing Exemplar

## Purpose

This is the first AETHER-Learn proof. It demonstrates a learning control plane before any distributed gradient training work begins.

The exemplar uses a contextual UCB router. It receives tasks from shifting task families and chooses among several workers. Outcomes update the router. Every stage is represented as a typed learning tuple.

## Why routing first

Routing is measurable, useful without model-weight training, robust to non-stationary workloads, commercially legible as utility-per-cost improvement, and naturally represented as tasks, proposals, decisions, outcomes, and promotions.

## Scenario

Workers:

```text
fast_cheap_worker
accurate_expensive_worker
math_specialist_worker
code_specialist_worker
```

Task families:

```text
simple
math
code
ambiguous
adversarial
```

Distribution shift:

```text
phase A: simple tasks dominate
phase B: math/code tasks dominate
phase C: adversarial and ambiguous load increases
```

## Learning loop

```text
TaskTuple
  -> ProposalTuple*
  -> RoutingDecisionTuple
  -> RoutingOutcomeTuple
  -> RouterUpdateTuple
  -> PromotionTuple
```

The router keeps a bandit state per task family. This is not a full contextual learner, but it is enough to prove the AETHER-Learn control-plane shape: context matters, outcomes update routing, and the update is recorded as a learning artifact.

## Run

```bash
python python/examples/aether_learn_no_regret_routing.py --json
```

Optional:

```bash
python python/examples/aether_learn_no_regret_routing.py \
  --horizon 480 \
  --exploration 0.85 \
  --seed 11 \
  --output-dir target/aether-learn/no-regret-routing
```

## Expected artifacts

```text
target/aether-learn/no-regret-routing/summary.json
target/aether-learn/no-regret-routing/summary.csv
target/aether-learn/no-regret-routing/ledger.jsonl
```

`ledger.jsonl` is the proof object. It contains replay-style typed learning tuples for every simulated task.

## Acceptance criteria

The exemplar is accepted when it demonstrates:

1. all six tuple families appear in the ledger;
2. routing decisions are replayable from task/proposal/router context;
3. cumulative regret is reported;
4. selection counts shift toward useful workers for task families;
5. negative outcomes remain in the ledger as learning evidence;
6. no model weights are trained.
