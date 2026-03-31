# ADR 0008: AI Support Resolution Desk Is Reference App Packaging, Not Product Identity

## Status

Accepted

## Context

AETHER's current proof surface is strongest in governed coordination, replay,
sidecar memory, and explanation. Focus-group feedback now says that the project
still feels abstract for ML-oriented evaluators unless it is packaged in an
end-user workflow they already recognize.

Support exception handling is commercially relevant, close to the current pilot,
and naturally exercises retrieved evidence, candidate actions, assignment
handoff, replay, and proof. It is therefore a strong packaging wedge, but it
would create a new distortion if that wedge were mistaken for a full product
renaming or a new core platform claim.

## Decision

We will package the flagship ML-facing application exemplar as an **AI support
resolution desk**.

That exemplar is:

- a working app pack over existing v1 pilot proof
- a buyer-facing entry point for ML relevance and end-user utility
- a reference application that makes sidecar memory, governed action, and
  explanation concrete

That exemplar is not:

- a new kernel, DSL, or HTTP semantic contract
- a claim that vector retrieval is the authority layer
- a replacement for AETHER's primary identity as a semantic coordination fabric
- a claim that AETHER is already a finished general ML orchestration platform

## Consequences

Positive:

- ML-oriented evaluators get a relevant application before abstract platform language
- the repo gains a concrete answer to "what does this look like for AI-enabled operations?"
- commercialization surfaces can lead with a believable working app instead of a generic platform claim

Constraints:

- docs and site copy must keep the support desk framed as a working app pack over current proof
- retrieval must remain clearly subordinate to semantic control in both docs and demos
- no kernel, DSL, or stable HTTP changes should be justified solely by this packaging layer
