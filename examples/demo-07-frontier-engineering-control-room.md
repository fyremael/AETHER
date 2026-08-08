# Demo 07: Frontier Engineering Control Room

This is AETHER's frontier-engineering showcase: a governed software-change
campaign from dependent work graph to one qualified candidate.

The demo is intentionally not an "agent swarm" animation. It shows how agents,
CI runners, reviewers, package builders, and release tooling can contribute
evidence while AETHER remains the operational truth layer that decides what is
current, admissible, stale, or promotable.

## Scenario

Six engineering agents and specialist workers collaborate on a runner and
release-control repair:

- 24 recursively dependent work packages describe diagnosis, implementation,
  regression, evidence collection, and qualification
- three competing candidates publish product SHA, tooling SHA, package digest,
  review state, change scope, and proposer identity
- CI, Supply Chain, Pages, and Capacity each publish an exact prerequisite
  receipt for every candidate
- runner authority moves from epoch 1 to epoch 2
- the product SHA stays frozen while the admissible candidate changes only
  release-control tooling
- the actual demo executable is hashed and registered as the canonical local
  package artifact

## The Three Candidates

`candidate-a-stale-runner`

- preserves the selected product SHA
- uses superseded tooling and package bytes
- carries successful receipts from runner epoch 1
- remains visible but is fenced from current qualification

`candidate-b-product-drift`

- changes the product SHA and product scope
- cannot reuse the selected product candidate's prerequisite evidence
- is rejected rather than being relabeled as a tooling-only repair

`candidate-c-release-control-repair`

- preserves the selected product SHA
- uses the protected current tooling SHA
- stays inside the release-control-only boundary
- matches the canonical package digest
- carries four exact successful receipts from runner epoch 2
- becomes the only promotable candidate after the repair closes the work graph

## Run It

Windows operator path:

```text
double-click scripts/run-demo-07.cmd
```

Technical path:

```bash
cargo run -p aether_api --example demo_07_frontier_engineering_control_room
```

The shared runner saves timestamped transcripts under
`artifacts/demos/demo-07/`.

## Screen-Share Flow

1. Open on the control-room header: product SHA, tooling SHA, package digest,
   replay cut, and current cut.
2. Show the blocked recursive work graph at the prior `AsOf` cut.
3. Advance to `Current` and show that the runner repair closes the graph.
4. Show all three candidates before discussing which one wins.
5. Show the four successful but stale epoch-1 receipts being fenced.
6. Show independent identity, scope, package, and evidence rejection reasons.
7. Contrast the empty prior promotion view with the one-row current promotion.
8. Show accepted and retained routing updates.
9. Finish on the provenance trace for the promoted candidate.

## Expected Semantic Outcomes

The runnable demo fails if these outcomes drift:

- six downstream work packages are blocked before the repair
- no work remains blocked at `Current`
- exactly three candidate rows are published
- exactly four stale runner receipts are fenced
- no candidate is promotable at the prior cut
- exactly one candidate is promotable at `Current`
- one routing update is accepted and one is retained as evidence
- the named queries share exactly two temporal evaluations

## Why This Reflects Frontier Engineering

The frontier is not merely generating more patches. It is coordinating many
specialized workers under changing evidence, expensive validation, strict
identity, and incomplete information.

This demo makes that control problem visible:

- dependencies are recursive rather than a flat checklist
- successful evidence can still be stale
- product and qualification tooling identities remain independent
- immutable bytes matter independently of source labels
- learning updates are explicit governance decisions
- every promotion remains replayable and explainable

## Truth Boundary

This is a functional control-room proof over deterministic campaign data and
one real local package artifact.

It does not claim:

- AETHER writes or reviews code
- AETHER replaces GitHub or CI
- the fixture represents a live production repository
- routing updates train model weights
- one local run qualifies autonomous software delivery or commercial GA

AETHER's demonstrated role is narrower and stronger: it supplies operational
truth for frontier engineering work contributed by external agents, tools,
runners, and reviewers.
