# Frontier Engineering Control Room

This document is the product source of truth for the AETHER frontier
engineering exemplar.

## Product Thesis

Frontier engineering teams increasingly combine coding agents, specialist
models, human reviewers, hosted runners, package systems, and release
qualification. The hard problem is no longer producing activity. It is
maintaining operational truth across activity:

- what work is actually unblocked
- which candidate preserves the intended product identity
- which evidence belongs to that exact candidate
- which runner currently holds authority
- which successful results are stale
- whether immutable package bytes changed
- why a candidate may be promoted
- which routing lessons are accepted versus merely retained

AETHER is positioned beneath those workers as the semantic control layer.

## Demonstrated Contract

The runnable Demo 07 uses:

- a 24-node recursive engineering work graph
- six agents and specialist workers
- three competing change candidates
- four prerequisite gate lanes per candidate
- independent product and tooling SHA identities
- exact package-digest equality
- runner epochs and stale-result fencing
- `Current` and `AsOf` replay
- provenance-bearing promotion proof
- accepted and retained routing updates

The promoted candidate is derived only when work readiness, product identity,
tooling admissibility, review approval, current exact evidence, and immutable
package bytes all align.

## Why This Is A Better Frontier Story

The support resolution desk remains an accessible application pack. The
frontier engineering control room is the stronger technical and executive lead
because it demonstrates the broader category:

> AETHER is the operational truth layer for agentic work.

It brings the named product pillars into one screen:

- **Core** evaluates the semantic programme.
- **Coordinate** governs work dependencies, runner authority, and promotion.
- **Memory** preserves exact journal history and anchored package evidence.
- **Learn** records accepted and retained routing updates.
- **Explain** reconstructs the promoted candidate's proof.

## Presentation Order

Lead with the candidate decision, not kernel vocabulary:

1. three candidates exist
2. successful evidence from the old runner is stale
3. one candidate changes the product and cannot reuse qualification
4. one release-control-only candidate preserves exact identity and bytes
5. the prior cut blocks promotion
6. Current allows exactly one promotion
7. the trace explains why

## Run Path

```bash
cargo run -p aether_api --example demo_07_frontier_engineering_control_room
```

Windows:

```text
scripts/run-demo-07.cmd
```

Walkthrough:

- `examples/demo-07-frontier-engineering-control-room.md`

## AETHER Orbital Web Console

The high-level operations surface is available as a private production site:

- https://aether-orbital-control.baltigor.chatgpt.site

The console turns the same semantic contract into a continuously advancing
control-room view:

- animated semantic topology and authority flow
- a causal-journal event stream
- exact `AsOf` scrubbing and return-to-Current playback
- recursive work readiness and blocked-path posture
- evidence freshness across four prerequisite lanes
- candidate identity, fencing, rejection, and promotion
- expandable proof provenance
- an executive shift brief for product, tooling, package, and review posture

Its source lives under `web/frontier-control-room/`.

## Truth Boundary

The demo uses deterministic campaign fixtures plus the SHA-256 and byte length
of the actual running demo executable as its canonical local package artifact.

The web console is a deterministic live simulation of those fixtures, not a
live GitHub telemetry connection. Neither surface is an autonomous coding
product, production capacity test, software-delivery qualification, or
model-training claim. Agents and CI remain external workers. AETHER
demonstrates the governed operating record and derived promotion decision
beneath them.
