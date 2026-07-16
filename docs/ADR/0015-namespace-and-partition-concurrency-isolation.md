# ADR 0015: Namespace and Partition Concurrency Isolation

- Status: Accepted
- Date: 2026-07-12
- Programme gate: `service.beta_boundary`

## Context

The HTTP service previously kept every namespace service behind one global
mutex. PostgreSQL requests added a request-local `std::thread::spawn(...).join()`
around that same mutex, so one slow namespace still stopped every other
namespace and blocked a Tokio worker. Replicated authority partitions had the
same global serialization shape. Audit entries held their in-memory lock while
performing file I/O.

## Decision

The namespace store is now a directory of stable, independently locked
namespace handles. The directory lock only locates or creates a handle.
Initialization and every kernel/storage operation occur under that namespace's
handle lock, so initialization happens once and same-namespace ordering remains
deterministic without serializing unrelated namespaces.

All synchronous kernel/storage work is submitted to one bounded blocking
executor. It has a configured worker count and a separate bounded queue.
Admission uses a non-blocking permit; saturation returns HTTP `503` with code
`namespace_busy` and `Retry-After: 1`. Worker panic and executor closure release
their permits through RAII and return typed failures. Request paths do not spawn
and join operating-system threads.

Replicated authority partitions now hold an immutable directory of
independently locked partition runtimes. Single-partition reads, writes, and
promotion take only that partition lock. Federated operations acquire the
participating partition locks through the existing explicit-cut sequence, and
the derived execution store has its own lock.

Audit memory and disk ownership are separated. Requests append to the in-memory
ledger, then use `try_send` into a bounded single-writer queue. Slow or failed
disk I/O never holds the entry lock. Saturation and writer loss create visible
`audit_write_failed` entries instead of unbounded buffering. The in-memory
ledger is a bounded FIFO using the configured audit limit; inserting beyond the
limit evicts the oldest entry. The persisted JSONL remains the durable audit
record and is subject to operator retention/rotation policy.

The deployment defaults are eight workers, 64 queued namespace operations, and
1,024 queued audit writes and retained in-memory entries. They are explicit in package configuration and
cannot be changed by auth-only reload.

## Consequences

- A blocked namespace or partition no longer delays work in another namespace
  or partition.
- Health, directory status, and auth state do not require a namespace handle
  lock.
- Same-namespace and same-partition semantic mutation remains serialized.
- Audit persistence is asynchronous; operators and tests that inspect the file
  directly must allow the bounded writer to drain. The API audit snapshot is
  immediately updated in memory and exposes only the retained FIFO window.
- The executor is intentionally process-wide and bounded. Per-namespace quotas
  and operation deadlines are added under R5.6.

## Verification

Unit and HTTP tests prove independent namespace progress, deterministic
same-namespace ordering, stable one-time handles, bounded saturation with
`Retry-After`, permit recovery after panic, visible audit backpressure, and
independent partition progress. The full HTTP contract suite also covers auth
reload, health/status, audit persistence, namespace isolation, and partitioned
federated surfaces.
