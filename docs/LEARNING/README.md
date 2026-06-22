# AETHER-Learn

AETHER-Learn is the Grand Challenge Labs project for **Coordinated Distributed Learning over Typed Tuple Spaces**.

The project begins with a conservative claim:

> AETHER should prove learning coordination before it attempts distributed gradient training.

AETHER-Learn treats observations, experiences, memories, model deltas, evaluations, routing decisions, and promotion decisions as typed, provenance-bearing learning tuples. The numerical work may happen in model workers, simulators, benchmark harnesses, review stations, or benchmark runners. AETHER supplies the semantic control plane: exact local state, deterministic replay, visibility boundaries, explanation, and explicit federation.

## Project position

AETHER-Learn is not a replacement for PyTorch, JAX, Ray, Kubernetes, MLflow, or federated-learning libraries. It is the governed learning layer around them.

The first proof target is no-regret model routing, not distributed backpropagation. That choice is deliberate: routing is measurable, useful, commercially legible, and already aligned with AETHER's coordination strengths.

## Documents

- [`CDL_TTS_SPEC.md`](./CDL_TTS_SPEC.md) defines the formal project.
- [`LEARNING_TUPLE_SCHEMA.md`](./LEARNING_TUPLE_SCHEMA.md) defines the first schema vocabulary.
- [`NO_REGRET_ROUTING_EXEMPLAR.md`](./NO_REGRET_ROUTING_EXEMPLAR.md) defines the first proof slice.
- [`EVALUATION_PROTOCOL.md`](./EVALUATION_PROTOCOL.md) defines acceptance metrics.
- [`CODEX_AGENT_BRIEF.md`](./CODEX_AGENT_BRIEF.md) recruits the implementation team.

## Runnable proof

Run the dependency-free proof:

```bash
python python/examples/aether_learn_no_regret_routing.py --json
```

The script writes a replay-style ledger under:

```text
target/aether-learn/no-regret-routing/
```

It emits task tuples, proposal tuples, routing decisions, outcomes, router policy deltas, and promotion tuples for a contextual UCB router operating under distribution shift.

## Build sequence

1. Codify AETHER-Learn as a formal GCL project.
2. Prove the learning-control-plane claim with no-regret routing.
3. Add a report surface that reconstructs why each routing policy delta was retained or promoted.
4. Add memory promotion with journal-anchored sidecar references.
5. Add federated learning claims across authority domains.
6. Only then widen toward distributed gradient training.
