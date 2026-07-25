# ADR 0022: Product and qualification-tooling identities are independent

- Status: Accepted
- Date: 2026-07-21

## Context

A release candidate previously used one commit SHA for product source, package
bytes, release workflows, and the verifier. A defect in qualification tooling
therefore burned otherwise unchanged product bytes and forced every expensive
prerequisite to restart.

## Decision

Release evidence v2 records two immutable identities:

1. Product identity binds the protected-main source commit/tree/ref and the
   canonical package SHA-256. CI, Supply Chain, Pages, and Capacity runs must
   have the exact product SHA and a protected `main` source branch.
2. Qualification-tooling identity binds the protected-main tooling
   commit/tree/ref, release workflow run/attempt, verifier version, and
attestation signer. The product commit must be its ancestor.
The candidate-to-tooling diff must also remain entirely inside the policy's
release-control allowlist; product source changes require a new candidate.

An old product candidate may be reverified with repaired tooling. The new
bundle retains the product/package identity and records the new tooling
identity; it is not a mutation or silent retry of the earlier verdict. Current
protected main must equal the tooling SHA at preflight, after approval, and
during independent verification.

Generic quality gates are projected from immutable exact-SHA prerequisite job
outcomes and independently requeried. Package-bound operational qualification
still executes against the canonical package. Legacy v1 bundles remain
historical artifacts and are rejected by the v2 verifier.

## Consequences

- Verifier-only repairs do not require rebuilding identical product bytes.
- Provenance shows both what was qualified and which control code decided it.
- PR checks can be fast while protected-main integration stays complete.
- Preflight rejects deterministic contract defects before expensive work.
