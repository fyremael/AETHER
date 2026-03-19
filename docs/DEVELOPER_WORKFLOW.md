# Developer Workflow

This guide describes the day-to-day engineering loop for AETHER.

It is not a style guide in disguise. Its job is to make sure we change the system without degrading determinism, semantics, or documentation quality.

## The Standard Loop

The expected loop for a nontrivial change is:

1. Orient yourself in the current architecture.
2. Make one coherent change.
3. Verify it on the full local gate.
4. Update the relevant documentation.
5. Commit the code and docs together.

If a change cannot survive that loop cleanly, it is usually too broad or insufficiently defined.

## Step 1: Orient

Before editing code, answer these questions:

- Which crate owns this behavior?
- Is this a substrate change, compiler change, runtime change, explainability change, or boundary change?
- Does the change affect determinism, replay, provenance, or public semantics?
- Which docs will need to move when the code moves?

If you cannot answer those questions quickly, read `docs/ARCHITECTURE.md` and `docs/GLOSSARY.md` before touching code.

## Step 2: Change One Thing Coherently

AETHER benefits from narrow, coherent slices.

Prefer changes that:

- solve one problem clearly
- preserve crate boundaries
- add tests that make the new semantic claim concrete
- update documentation in the same slice

Be cautious with bundles that mix runtime semantics, API reshaping, documentation rewrites, and unrelated cleanup. Those are harder to reason about and harder to review.

## Step 3: Verify

The standing local gate is:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test
```

The expected cross-platform baseline is:

- Windows MSVC
- WSL Ubuntu GNU

For anything nontrivial, verify both. A change is not “done locally” if it only works on one side of the supported toolchain contract.

## Step 4: Update Documentation

Documentation should move with the code, not after it.

The short rule is:

- if behavior changed, update the relevant behavior docs
- if the public story changed, update `README.md`
- if contributor workflow changed, update `CONTRIBUTING.md` or this guide
- if operator behavior changed, update `docs/OPERATIONS.md`, `examples/README.md`, or `scripts/README.md`
- if the implemented boundary moved, update `docs/STATUS.md` and `docs/KNOWN_LIMITATIONS.md` when appropriate

For the full standard, read `docs/DOCUMENTATION_STANDARD.md`.

## Step 5: Commit As A Phase Boundary

Commits should describe a coherent unit of value, not a sequence of half-finished experiments.

Good commit shapes usually look like:

- “Implement coordination runtime vertical slice”
- “Add flagship coordination showcase demo”
- “Tighten extensional binding validation”

Less good commit shapes are vague or mixed:

- “updates”
- “fix stuff”
- “more changes”

The point of a commit is not only to preserve history. It is to preserve legibility.

## Checklists By Change Type

### Semantics change checklist

- Are the new semantics explicit in tests?
- Did determinism, replay, provenance, or materialization behavior change?
- Did `README.md`, `docs/STATUS.md`, or `docs/KNOWN_LIMITATIONS.md` need updates?

### API boundary checklist

- Is the boundary additive or breaking?
- Is the service contract clear in code and docs?
- Did examples or operator paths need updates?

### Demo or operator path checklist

- Can a non-technical operator run it?
- Does it write a report or produce durable output?
- Is the narrative guide consistent with the actual output?

### Documentation-only checklist

- Does the new documentation say what is true today?
- Does it distinguish current implementation from future intent?
- Does it reduce ambiguity instead of adding prose weight?

## Definition Of Done

A change is done when:

- the code is correct enough to survive the verification gate
- the tests prove the intended claim
- the docs tell the truth about the new state of the system
- the commit history records the change as a coherent unit

That bar is intentionally higher than “the code compiles.” AETHER is trying to become a system people can trust, not merely one they can run.

## Recommended Reading While Working

- `docs/ARCHITECTURE.md` for system shape
- `docs/GLOSSARY.md` for canonical terms
- `CONTRIBUTING.md` for contributor norms
- `docs/STATUS.md` for current implementation line
- `docs/KNOWN_LIMITATIONS.md` for the intentional frontier
