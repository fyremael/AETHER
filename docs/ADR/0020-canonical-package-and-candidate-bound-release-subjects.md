# ADR 0020: Canonical package and candidate-bound release subjects

- Status: Accepted
- Date: 2026-07-15

## Context

The first exact-candidate workflow bound its gate envelopes and final artifact
to one commit, but it rebuilt a package and accepted subject files by name.
Release Readiness separately built and tested another package. A successful
workflow declaration and a present file therefore did not prove that every
commercial-beta requirement passed for the bytes being promoted.

## Decision

The successful exact-SHA Supply Chain package is the one canonical candidate
package. Release Readiness downloads that artifact by immutable ID, verifies
the API byte size and SHA-256, and tests those bytes without rebuilding them.

Every required bundle subject uses `aether.release-subject.v1`. The envelope
binds a semantic observation to the candidate commit, tree and protected
`main` ref; producer workflow, job, run and attempt; canonical package digest;
source workflow runs; immutable artifact IDs, names, sizes and digests;
status, expiry and metrics; and a canonical identity. The verifier applies a
subject-specific validator and, for official evidence, redownloads source
artifacts and requeries source runs and security jobs. File presence is never
sufficient.

Operational readiness runs before bundle assembly. Its immutable subjects are
passed to the reusable signer job, which adds package provenance, assembles the
bundle, and delegates independent verification to a dependent job. Capacity is
read from its SHA/run/attempt-named report, never its `latest` navigation copy.
The qualification artifact also retains the nine immutable operational outputs;
their byte sizes and SHA-256 values must match the candidate-bound readiness
manifest before their subject envelopes are accepted.

Commercial promotion is a separate generated record. The ledger remains
policy-only. `aether.commercial-beta-promotion.v1` requires byte-identical
official and independently recomputed passing verdicts and records the exact
candidate and artifact receipts. It preserves four distinct GA blockers.

## Consequences

- A release qualification restarts if protected `main` advances.
- A branch run, stale `latest` file, rebuilt package, failed or expired subject,
  cross-candidate/run/package artifact, or authored verdict cannot promote.
- Qualification costs more CI time and artifact bandwidth because independent
  verification deliberately redownloads immutable bytes.
- The beta boundary remains Windows x86_64, single node, SQLite by default,
  optional `verify_full` Postgres journal with local SQLite sidecars, and
  loopback HTTP or isolated trusted TLS ingress.
- GA remains blocked on support/incident posture, multi-platform distribution,
  signed promotion, and distributed-truth qualification.
