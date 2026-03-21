# Concept Translation Guide

This guide maps technical AETHER concepts into language that clients and users
can understand quickly without losing the real meaning.

## Translation Table

| Technical term | Client-facing translation | What to say |
| --- | --- | --- |
| Recursive closure | complete dependency resolution | "AETHER keeps following the chain until nothing relevant is left out." |
| Provenance | proof trail | "It shows which facts caused the answer." |
| `Current` | live truth | "This is what the system believes right now." |
| `AsOf(eN)` | point-in-time truth | "This is what the system believed at that exact cut of history." |
| Resolver | semantic state builder | "It turns raw history into the state the system reasons over." |
| Derived tuple | computed operational fact | "This is something the system inferred from the known facts." |
| TupleSpace | explainable semantic blackboard | "AETHER can host a shared coordination space for agents and operators, with replay and proof." |
| Stratified negation | safe negative reasoning | "The system can rule something out without creating circular contradictions." |
| Semi-naive runtime | efficient repeated reasoning | "The engine avoids recomputing the whole world every time it iterates." |
| Lease fencing | stale-work rejection | "Old authority is rejected automatically once it is no longer valid." |
| Journal prefix | exact historical cut | "We can stop history at a precise point and ask what was true there." |

## Demonstration Pattern

When introducing a technical concept:

1. start with the operational question
2. describe the failure mode in ordinary systems
3. describe AETHER's answer in plain language
4. optionally name the technical concept afterward

Example:

- Question: "Can worker B act right now?"
- Plain answer: "AETHER follows the dependency and lease chain all the way through, then shows why the answer is yes or no."
- Technical note: "Under the hood, that is recursive closure plus lease semantics."

## Phrases To Prefer

- "full chain-of-effect reasoning"
- "complete dependency picture"
- "point-in-time replay"
- "proof-backed answer"
- "governed authority"
- "stale-work fencing"

## Phrases To Use Carefully

- "recursive closure"
- "fixed point"
- "Datalog"
- "stratified negation"
- "semi-naive evaluation"

These are accurate terms, but they should usually appear after the plain
language explanation rather than before it.

## Red-Flag Explanations

Avoid explanations that sound like this:

- "It's basically a graph database plus rules."
- "It's just a smarter workflow engine."
- "It's an agent memory layer."

Those analogies may help briefly, but they understate the coordination and proof
model that makes AETHER distinct.
