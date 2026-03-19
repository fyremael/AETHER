# Contributing to AETHER

Thank you for taking the project seriously enough to improve it.

AETHER is trying to become a semantic kernel, not merely a repository of useful code. That means contributions are judged not only by whether they work, but by whether they preserve the center of gravity of the system: deterministic semantics, explicit provenance, recursive closure, and clean language boundaries.

## Start Here

Before opening a pull request, read these documents in order:

1. `README.md`
2. `docs/README.md`
3. `SPEC.md`
4. `RULES.md`
5. `INTERFACES.md`
6. `REPO_LAYOUT.md`

If your change touches architecture, semantics, or crate boundaries, also read `IMPLEMENTATION_DECISION.md`.
If your change affects public behavior, contributor flow, or operator flow, also read `docs/DOCUMENTATION_STANDARD.md`.

## Development Baseline

The current development baseline is:

- Rust stable
- Windows MSVC support
- WSL Ubuntu support

Run the full local gate before asking anyone else to spend time on your change:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test
```

If you work primarily on Windows, it is still worth checking the Linux path in WSL for anything nontrivial.

## Working Agreements

These are not style preferences. They are project constraints.

### Rust remains authoritative

The semantic kernel lives in Rust. Go and Python are important, but they are boundary layers. Contributions must not move semantic authority out of the Rust crates by accident.

### The DSL remains canonical

Even while AST builders are the present authoring path, the intended public semantics surface is still the AETHER DSL. Do not design features as though host-language-only APIs are the long-term center of the system.

### Determinism matters

For a fixed schema, journal prefix, and compiled program, results should be deterministic. If a change could weaken that property, call it out explicitly and do not merge it casually.

### Explainability matters

Derived tuples should remain explainable. If you add new derivation behavior, think about how provenance, iteration metadata, or proof surfaces will expose it.

### Boundaries are real

The crate split in `REPO_LAYOUT.md` exists for a reason. Please do not collapse responsibilities into a convenience crate unless there is a clear architectural justification.

## What Makes A Good Contribution

A strong contribution usually does four things:

- solves one coherent problem
- includes tests that prove the intended behavior
- updates documentation when the public surface or architectural story changes
- leaves the codebase easier to reason about than it was before

Small, disciplined pull requests are easier to review than ambitious bundles of loosely related work.

## Change Categories

### Semantics changes

If you are changing resolution behavior, rule compilation, runtime derivation, or explainability semantics:

- include tests that make the semantic claim concrete
- say whether the change affects determinism, replay, provenance, or materialization
- note whether it widens or narrows the currently supported semantics

### API and boundary changes

If you change `aether_api`, public traits, or cross-language assumptions:

- explain the boundary impact clearly
- avoid widening process-boundary commitments casually
- prefer additive evolution unless a breaking change is clearly justified

### Documentation changes

If you improve docs, please prefer specificity over aspiration. Public docs should say what the system does today, what it intends to do later, and where the line currently sits.

## Pull Request Checklist

Before opening a pull request, make sure the answer to each question is yes:

- Does the change fit the architectural direction in `SPEC.md`?
- Does it preserve Rust as the semantic authority?
- Does it include tests or a clear explanation of why tests are not applicable?
- Does it update docs if behavior, boundaries, or contributor workflow changed?
- Does it meet the quality bar in `docs/DOCUMENTATION_STANDARD.md` when docs were touched?
- Does it avoid unrelated drive-by refactors?

## Issues

Please use the issue templates when they fit.

- Use the bug report template for incorrect behavior, regressions, or semantic mismatches.
- Use the feature request template for new capabilities or major expansions.
- Use the semantics discussion template when the main question is conceptual or architectural.

Good issues describe the observed behavior, the expected behavior, and the semantic impact. In a project like this, the impact on meaning often matters more than the raw stack trace.

## First Contributions

Good first contributions usually look like:

- tightening tests around an existing invariant
- improving explanation or provenance plumbing
- sharpening docs so the public story matches the code
- adding narrowly scoped examples

Less good first contributions are broad rewrites, boundary reshuffles, or speculative abstractions that outpace the current implementation.

## Code Of Conduct

The standard here is simple: be rigorous, be kind, and assume good faith. Strong technical disagreement is normal. Contempt is not.
