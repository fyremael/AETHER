# ADR 0011: Execution Receipts And Trace Handles

- Status: accepted
- Date: 2026-07-11
- Programme gate: `semantic.trace_handle_identity`

## Context

`TupleId` is intentionally allocated by one runtime evaluation. It is useful
inside the Rust kernel but cannot identify a proof after another run, append,
restart, namespace selection, or policy change. Selecting the most recent
derived set for a bare tuple number can therefore return a valid proof for the
wrong computation.

## Decision

Every service evaluation persists an immutable execution manifest and the
canonical replay inputs that created it. `ExecutionId` is the SHA-256 digest of
the R1 evaluation descriptor: namespace, requested temporal view, visible
journal prefix, schema, scoped compiled program, effective policy, ordered
imported cuts and epochs, and the engine-semantics version. Equivalent scoped
inputs therefore have the same internal identity.

Each derived tuple is exposed through a freshly generated 256-bit random
`TraceHandle`. The handle is an opaque locator. It contains no readable
namespace, policy, cut, tuple number, or digest and is not authorization by
possession. Resolution first checks the request namespace and current policy
against the immutable original execution policy. A revoked token or narrowed
policy fails before returning the proof record.

The manifest binds visible journal cut and prefix digest, schema reference,
document and compiled-program digests, policy digest, imported federation
material, engine version, creation time, and retention metadata. The trace
record binds execution ID, local tuple ID, tuple digest, trace digest, and the
stored trace. Optional replay verification recomputes from the stored exact
visible history, schema, and compiled program and compares digests.

Kernel-local APIs may continue to use `TupleId`. Service clients and reports
must carry a trace handle or state that the row is not safely explainable. The
legacy `/v1/explain/tuple` endpoint returns `409`; it never searches a current
or recent execution. The endpoint is scheduled to become `410` after the
compatibility window.

## Persistence And Retention

Ephemeral library services use a bounded in-memory store. Packaged SQLite
services use a separate `<journal>.executions.sqlite` database. Postgres
journal mode deliberately uses local SQLite execution metadata beside the
configured sidecar path; it is derived proof metadata, not journal authority.
The default bound is 1,024 immutable executions per store. Eviction removes
the execution and trace records while retaining handle tombstones, so an old
handle returns an explicit expired result and can never alias a later proof.

SQLite journal, sidecar, execution-store, WAL/SHM companions, audit log,
configuration, and token files form one package backup/restore set. Operators
must stop or quiesce the service before file-copy backup. Postgres operators
must combine the database-native journal backup with the local execution and
sidecar metadata backup at the same operational cut.

## Federation

Federated identities bind the ordered partition cuts, leader epochs,
visible-prefix digests, and imported-fact digests used by the scoped program.
Federated trace handles are stored by the partition service and resolved under
the same namespace and policy checks as local handles. Promotion or a later
partition append creates a different descriptor; it cannot change an existing
proof.

## Cache And Record Lifecycle

Execution records are content-addressed and immutable. Equivalent executions
reuse the manifest but issue fresh external handles. Appends and promotions may
clear computation caches, but they do not mutate retained execution records.
Corrupted manifests, corrupted trace digests, missing execution records, or an
incompatible engine version fail closed. Rollback must preserve the execution
database or make handles explicitly unavailable; it may not restore a
`last_derived` fallback.

## Consequences

- Proof identity remains stable through later semantic activity and restart.
- Authorization is evaluated at resolution time without rewriting proof data.
- Exact replay requires bounded additional storage for canonical inputs.
- Backup/restore must treat execution metadata as a first-class durable
  component.
- Tuple-oriented clients must migrate in lockstep with receipt-bearing server
  responses.
