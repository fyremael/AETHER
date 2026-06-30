# ADR 0009: Service v2 Namespaces And Postgres Journal

## Status

Accepted.

## Context

The v1 semantic kernel is closed around exact journal replay, recursive
derivation, provenance, and explanation. The next service release needs a
design-partner deployment path without reopening kernel semantics or pretending
that the current replicated authority-partition prototype is already a
generalized distributed platform.

SQLite remains the best default for local packages and single-node bundles.
Some service deployments, however, need an operational database boundary for
the authoritative source journal. That storage widening must preserve the same
append/history/prefix contract used by the Rust kernel.

## Decision

AETHER Service v2 adds service-plane namespaces selected by
`X-Aether-Namespace`, defaulting to `default`. Namespaces are HTTP/storage/auth
isolation boundaries. They are not authority partitions, not DSL semantics, and
not cross-partition transaction scopes.

Service v2 keeps SQLite as the package default and adds an optional
`PostgresJournal` behind the existing `Journal` trait. Postgres stores datoms as
JSONB payloads with a namespace column and uses a per-namespace append lock row
so committed journal order remains deterministic for concurrent writers inside
that namespace.

Sidecars remain local SQLite catalogs in this slice, including for Postgres
journal deployments.

## Consequences

- Existing clients and routes remain valid because absent namespace headers use
  `default`.
- Token configs can bind allowed namespaces; missing bindings normalize to
  `default`.
- `/v1/status` can report storage backend, active namespace count, namespace
  policy summary, sidecar mode, and Postgres schema/url-presence without
  leaking secrets.
- Postgres is authority for source journal order only. It does not become a SQL
  rule engine, a global `AsOf`, or an authority for derived tuples.
- A future consensus or multi-host protocol must sit below the same partition
  append/history contract rather than changing the semantic surface.
