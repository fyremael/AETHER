# ADR 0002: Governed Incident Blackboard Is Demo Packaging, Not Product Identity

## Status

Accepted

## Context

AETHER's current kernel proof has become materially stronger than its
first-screen explanation. New readers can encounter terms like semantic kernel,
recursive closure, provenance, or TupleSpace before they encounter a concrete
answer to the question "what is this useful for?"

The repository already includes a credible adjacent-next pattern for a governed
shared workspace across agents and operators. That pattern is commercially
helpful, but it risks creating a different confusion if it is mistaken for a
full product renaming or a new semantic contract.

## Decision

We will package the product-facing exemplar as a **governed incident
blackboard**.

That exemplar is:

- a documentation and demo layer over existing kernel proof
- a design-partner-facing adjacent-next story
- a reference pattern that makes current utility legible quickly

That exemplar is not:

- a new top-level semantic contract
- a stable TupleSpace API promise
- a replacement for AETHER's primary identity as a semantic coordination fabric

## Consequences

Positive:

- first-time readers get a concrete, self-contained use case before technical jargon
- design-partner conversations can stay anchored to present proof while still opening the agentic future story
- AETHER keeps the benefit of the blackboard / TupleSpace pattern without shrinking the company story to that label

Constraints:

- demo and site copy must keep the incident blackboard framed as adjacent-next packaging
- docs must explicitly distinguish client-facing blackboard language from technical TupleSpace references
- no kernel, DSL, or HTTP contract changes should be justified solely by this packaging layer
