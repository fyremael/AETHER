# Documentation Standard

This document defines the quality bar for AETHER documentation.

The goal is not merely to avoid stale docs. The goal is to make documentation one of the project’s competitive advantages: precise, current, navigable, and trustworthy.

## Principle

Documentation is part of the product surface of AETHER.

In a project built around semantics, process, and explanation, poor documentation is not a cosmetic flaw. It is a semantic defect. It makes the system harder to interpret and easier to misuse.

## The Core Rules

### 1. Current truth before future intent

Every major document should make it obvious what is implemented now before it talks about what is planned later.

Good:

- “The runtime currently supports stratified negation for the present slice. Bounded aggregation is not yet implemented.”

Bad:

- “The runtime will eventually support many advanced features.”

### 2. Governing text and current-state text must not blur together

Specification documents, architecture guides, status guides, and limitations guides do different jobs.

Do not write current-state caveats into governing texts when they belong in `docs/STATUS.md` or `docs/KNOWN_LIMITATIONS.md`. Do not write architectural principles into status documents when they belong in `SPEC.md` or `docs/ARCHITECTURE.md`.

### 3. Public docs must be specific

Avoid vague claims like:

- “robust”
- “powerful”
- “enterprise-ready”
- “intuitive”

Replace them with concrete statements about behavior, guarantees, boundaries, or current support.

### 4. Every meaningful surface needs an entry point

If a person cannot quickly discover how to:

- understand the architecture
- contribute safely
- run the demos
- interpret the current implementation boundary

then the documentation system is incomplete.

### 5. Docs move with code

Behavior changes, workflow changes, operator-path changes, and public-story changes should update the relevant docs in the same commit whenever feasible.

## Required Update Triggers

Update docs when a change affects:

- public behavior
- semantic guarantees
- crate boundaries
- operator workflows
- demo outputs or runner behavior
- contributor workflow
- the implemented versus deferred boundary

At minimum, consider whether the change requires updates to:

- `README.md`
- `CONTRIBUTING.md`
- `docs/STATUS.md`
- `docs/KNOWN_LIMITATIONS.md`
- `docs/ARCHITECTURE.md`
- `docs/OPERATIONS.md`
- `examples/README.md`
- `scripts/README.md`

## Document Categories

### Front door docs

These orient newcomers quickly.

Examples:

- `README.md`
- `docs/README.md`

### Governing docs

These define architectural or structural direction.

Examples:

- `SPEC.md`
- `RULES.md`
- `INTERFACES.md`
- `REPO_LAYOUT.md`

### Current-state docs

These explain the present implementation.

Examples:

- `docs/STATUS.md`
- `docs/KNOWN_LIMITATIONS.md`
- `docs/ARCHITECTURE.md`

### Process docs

These explain how people work in and around the repository.

Examples:

- `CONTRIBUTING.md`
- `docs/DEVELOPER_WORKFLOW.md`
- `docs/OPERATIONS.md`

### Reference docs

These keep vocabulary and navigation stable.

Examples:

- `docs/GLOSSARY.md`
- `examples/README.md`
- `scripts/README.md`

## Writing Style

The expected style is:

- declarative
- specific
- calm
- technically exact
- honest about uncertainty and incompleteness

Prefer:

- short sentences with explicit nouns
- sharp contrasts between implemented and deferred behavior
- concrete lists and tables when they improve navigation

Avoid:

- inflated language
- marketing adjectives without evidence
- hedging that makes clear boundaries harder to see
- prose that is long only because it is repetitive

## Review Questions

When reviewing documentation, ask:

- Does this say what is true today?
- Does it distinguish implemented behavior from roadmap intent?
- Does it reduce ambiguity?
- Does it help the intended audience act correctly?
- Does it match the code and the operator path?

If the answer to any of those is no, the document is not ready.

## Definition Of Documentation Done

Documentation work is done when:

- the right document exists
- the document is easy to find
- the content matches the current code and process
- the intended audience can act from it without guessing

That is the standard we want associated with AETHER.
