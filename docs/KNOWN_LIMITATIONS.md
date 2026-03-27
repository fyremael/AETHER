# KNOWN_LIMITATIONS

The v1 **single-node semantic thesis** is now closed. The limitations below are
the remaining post-v1, platform-breadth, or operability gaps around that core.

These are real boundaries, not hidden footnotes. They mark the edge of the
implemented system.

## Language And Runtime Scope

- The runtime now executes semi-naive recursion, stratified negation, and bounded aggregation for the current v1 slice, but aggregation is intentionally frozen at non-recursive grouped head-term `count`, `sum`, `min`, and `max` rules rather than richer post-v1 aggregate syntax.
- Extensional predicate binding is inferred by name against schema attributes and is therefore deliberately conservative.
- Explain traces currently reconstruct one merged proof graph per tuple; they do not yet distinguish alternative proof families for the same derived tuple.
- The DSL now covers the current canonical v1 surface, but it still lacks post-v1 ergonomic features such as richer type aliases, broader document modularity, and more generalized explain/query composition.

## Service, Governance, And Operator Surface

- The kernel service now has both in-memory and SQLite-backed execution paths, and the pilot HTTP path now supports bearer-token auth plus persisted audit logs, but the boundary is still not multi-tenant or production-hardened.
- Coordination semantics now cover heartbeats and execution outcomes in the pilot slice, but expiry still relies on explicit semantic state rather than clock-driven timeout windows or distributed failure detection.
- HTTP authorization still uses coarse endpoint scopes, but tokens now also bind maximum semantic policy visibility for history, state, documents, explanation, sidecar access, and reports. The remaining gap is richer governance ergonomics, not the absence of token-bound semantic policy or policy-aware derivation.
- Audit entries now capture effective policy decisions plus requested, granted, and effective semantic visibility, but they still do not capture full operator intent or semantic diffs between cuts.
- Operator reports are now policy-aware fixed-format incident summaries in markdown and JSON, but they are still not interactive investigation tools.
- Coordination delta reports now compare explicit cuts and carry trace handles where visible, but they still summarize fixed pilot sections rather than arbitrary user-defined investigative views.
- The Go operator TUI is now implemented as the primary live pilot cockpit, but it is intentionally pilot-focused and read-only in v1 rather than a general workflow IDE or mutation surface.
- The pilot service now has a packaged deployment path with config-backed startup, package-local rotation tooling, backup/restore helpers, auth reload, explicit token/principal identities, and secret-file/env/command token resolution, but it is still a single-node bundle rather than a fully managed deployment story with automated rotation services, distributed revocation, or native cloud secret-manager integrations.

## Performance, Storage, And Release Discipline

- The performance suite now supports local baseline capture, drift comparison, stress fixtures, and a required CI launch/drift gate, but it does not yet maintain historical benchmark trends beyond uploaded workflow artifacts.
- The structured release-readiness suite now produces a coherent QA evidence pack, but it is still a pre-release verification flow rather than a signed artifact and promotion pipeline.
- Memory figures in the performance report are structural lower-bound estimates rather than allocator-exact telemetry.
- Durable storage is still rooted in local SQLite files. Snapshotting and restore now exist for the packaged pilot path, but there is still no general compaction, remote backup service, or platform-wide storage control plane.

## Boundary Clients And Scaling

- The Go shell and Python SDK are now real, but both remain early boundary clients rather than mature ecosystem surfaces with richer async/notebook/admin layers.
- Artifact and vector sidecar federation is now journal-subordinated and temporally exact on the SQLite-backed pilot path, but it is still a single-node backend and does not yet replicate or fail over independently of the kernel process.
- Vector search can now project provenance-bearing semantic facts back into the rule layer, but the current projection is deliberately narrow: a three-field `(query_entity, matched_entity, score)` extensional fact shape.
- The first partition-aware distributed-truth slice now includes imported-fact reasoning, federated explain/report surfaces, a SQLite-backed durable backend, and a single-host leader/follower replicated authority-partition prototype with manual promotion. What it still does not include is automatic election, quorum consensus, multi-host replication, or a managed failover plane.
- Imported-fact federation is semantically exact for the current slice, but that slice is intentionally narrow: imported queries must currently be single-goal tuple-producing reads rather than arbitrary joined row shapes.
- Sidecars remain partition-local and journal-subordinated in the replicated prototype. They do not replicate or fail over independently.
