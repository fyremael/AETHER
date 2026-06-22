# Codex Agent Brief: AETHER-Learn

This document recruits the initial Codex team for AETHER-Learn.

## Lead architectural decision

Build the learning control plane first. Do not start with distributed gradient training.

The first proof is no-regret routing over typed learning tuples.

## Agent roster

### Agent 1: Schema Keeper

Mission: maintain `docs/LEARNING/LEARNING_TUPLE_SCHEMA.md`.

Responsibilities:

- keep tuple names stable;
- prevent bulk tensors from being embedded in semantic tuples;
- ensure every learning artifact carries provenance and visibility context;
- prepare future Rust/DSL schema translation.

Acceptance evidence: schema diffs are small, typed, and backwards-aware.

### Agent 2: Routing Proof Builder

Mission: own `python/examples/aether_learn_no_regret_routing.py`.

Responsibilities:

- keep the proof dependency-free;
- preserve deterministic seeded runs;
- emit `summary.json`, `summary.csv`, and `ledger.jsonl`;
- add tests only after the shape stabilizes.

Acceptance evidence: the script runs from the repository root with standard Python.

### Agent 3: Evaluation Auditor

Mission: own `docs/LEARNING/EVALUATION_PROTOCOL.md`.

Responsibilities:

- prevent vague success claims;
- add measurable gates before broader project claims;
- define failure cases and negative-transfer tests;
- ensure every future exemplar has a replayable evidence bundle.

Acceptance evidence: every promoted exemplar has metrics and artifacts.

### Agent 4: AETHER Integration Engineer

Mission: bridge the Python proof to the real AETHER service after the standalone proof is accepted.

Responsibilities:

- map proof tuples to AETHER facts;
- add a report path that explains accepted and retained router updates;
- respect partition-local truth and imported-fact boundaries;
- avoid pretending the current prototype is a production multi-host control plane.

Acceptance evidence: a service-backed demo produces the same learning ledger shape.

### Agent 5: MODULUS Liaison

Mission: prepare model-delta metadata after routing is proven.

Responsibilities:

- define norm, spectral, boundary, and rollback metadata for `ModelDeltaTuple`;
- keep optimizer/control claims separated from routing proof claims;
- coordinate with MODULUS boundary-contract language.

Acceptance evidence: model-delta schemas are ready before any distributed gradient training begins.

## First work order

1. Review this PR as the founding AETHER-Learn patch.
2. Run:

```bash
python python/examples/aether_learn_no_regret_routing.py --json
```

3. Confirm that the ledger contains the six tuple families.
4. File follow-up issues for service-backed tuple ingestion, report rendering, memory promotion, federated learning claims, and MODULUS metadata.
