# AETHER-Learn Evaluation Protocol

## Evaluation posture

AETHER-Learn must earn its claims with small, replayable demonstrations before platform language expands.

The first target metric is:

```text
utility per cost under distribution shift
```

## Kernel-facing metrics

- append latency;
- read/query latency;
- report generation latency;
- visibility-filter overhead;
- sidecar projection overhead;
- replay latency from explicit cuts.

## Learning metrics

- cumulative reward;
- cumulative regret;
- regret by task phase;
- average reward by task phase;
- selection counts by worker and task family;
- negative-transfer rate;
- promotion precision;
- rollback completeness.

## Governance metrics

- percentage of promoted artifacts with sufficient evidence;
- percentage of negative outcomes retained for learning;
- rejected artifacts later found useful;
- accepted artifacts later causing regression;
- explanation trace completeness;
- audit reconstruction time.

## No-regret routing gate

For the first proof, a run must produce:

```text
summary.json
summary.csv
ledger.jsonl
```

The summary must include:

```text
horizon
seed
exploration
cumulative_reward
oracle_reward
cumulative_regret
learned_values
selection_counts
phase_avg_reward
phase_regret
```

The ledger must include:

```text
TaskTuple
ProposalTuple
RoutingDecisionTuple
RoutingOutcomeTuple
RouterUpdateTuple
PromotionTuple
```

## Interpretation rule

AETHER-Learn does not need to show perfect routing. It needs to show disciplined learning coordination:

- every update is evidence-bearing;
- every decision is replayable;
- bad outcomes are retained rather than erased;
- promotion and scope are explicit;
- the system can improve without moving model weights.

## Next gate after routing

The next proof should be memory promotion. Candidate memories must be evaluated before promotion, and vector-sidecar material must remain subordinate to journal-anchored provenance.
