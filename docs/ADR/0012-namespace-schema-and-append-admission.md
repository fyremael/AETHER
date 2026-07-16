# ADR 0012: Namespace Schema and Append Admission

- Status: Accepted
- Date: 2026-07-11
- Programme gate: `storage.transactional_schema_append`

## Context

AETHER previously accepted datoms at the service boundary and discovered
schema, dependency, provenance, or sequence defects during later replay. That
made journal authority easier to poison than to repair. It also allowed schema
activation and append to race without one linearizable decision.

The journal is an immutable semantic authority. Validation therefore belongs
before commit, and the exact admission decision must remain inspectable after
the process exits.

## Decision

Each namespace has one active immutable schema revision. A `SchemaRef` binds a
version to a SHA-256 digest of canonical JSON for the namespace attribute
schema. Documents may add predicates and rules, but their journal attributes
must be exactly compatible with the active namespace schema.

Schema revisions are immutable catalog records. Initial activation and every
compare-and-swap successor activation certify the complete existing journal
prefix at an exact `JournalCutRef`. Additive evolution may add attributes but
may not remove or redefine an existing attribute. Type or merge-class changes
require a new attribute ID or an offline namespace-generation migration.

Every externally reachable append is represented by
`AppendAdmissionRequest`. A private validated-batch type is constructed only
after the complete batch passes:

- active schema identity and recursive value typing;
- operation/attribute-class rules;
- sequence-anchor and unique-element checks;
- causal-frontier and provenance-parent precedence;
- policy-dependency closure;
- finite confidence and required provenance/schema identity; and
- optional expected-cut and idempotency preconditions.

Storage remains unaware of attribute semantics. It receives only an admitted
batch, expected cut, active schema digest, and receipt draft. In-memory,
SQLite, and Postgres implementations compare the active schema and journal cut,
append the full batch, and persist the receipt as one atomic operation.
SQLite uses an immediate transaction; Postgres uses the existing per-namespace
row lock. Schema activation checks the same cut under the same authority lock,
so activation and append have one serial order.

Each committed batch has a durable receipt containing its random batch ID,
schema reference, prior and committed cuts, canonical batch digest, principal,
admission-engine version, appended count, and whether the schema reference was
implicitly negotiated. Reusing an idempotency key is accepted only for the
same schema and batch digest.

For the compatibility release, an omitted schema reference binds to the active
schema. A namespace with no schema may infer a conservative legacy schema only
through the same validator and atomic append path. The receipt marks this use
as implicit. Strict callers discover, register, activate, dry-run, and append
with explicit schema and cut preconditions. There is no public unvalidated
bypass; raw `Journal::append` remains a low-level storage and test primitive.

Existing histories enter a read-only certification flow. A valid prefix seals
an immutable `SchemaBaselineReceipt`. An invalid prefix seals a visible
quarantine receipt and activation fails without rewriting or skipping datoms.
Repairs require a new namespace generation and an immutable migration manifest;
the original generation stays read-only. Baselines can carry that manifest,
and authority backups include the journal, schema catalog and activation state,
append receipts, baseline/migration records, and execution store.

The leader is the semantic admission authority for replicated partitions.
Followers receive the leader revision, datoms, and receipt; they verify the
batch digest and exact prior/committed cuts, then persist the leader batch ID
and receipt identity. Followers do not make an independent semantic decision.

## API consequences

The HTTP boundary exposes schema catalog, registration, activation, append
dry-run, append receipts, and receipt-returning append endpoints. Schema, cut,
and idempotency conflicts use structured `409` codes while retaining a human
message. Status advertises `trace_handles_v1`, `namespace_schema_ref_v1`,
`append_receipts_v1`, and `structured_errors_v1`.

The Go and Python clients carry the typed/new admission contract. The existing
single-argument Python append remains source-compatible by negotiating the
temporary implicit-schema path.

## Consequences and deferred work

- Bad batches cannot partially mutate history, receipts, schema state, or
  sidecars.
- Schema activation may fail with a stale cut and must be retried from a fresh
  certified prefix.
- Online mixed-schema history and in-place semantic redefinition are excluded
  from beta.
- Postgres transport security, immutable release evidence, and full offline
  generation-migration tooling remain later programme gates; none permits
  bypassing admission.

## Verification

The contract is covered by in-memory and SQLite admission matrices, durable
restart and backup/restore tests, quarantine tests, SQLite and optional
Postgres append/activation race tests, structured HTTP tests, and replicated
leader/follower receipt equality tests.
