# KNOWN_LIMITATIONS

- The runtime now executes semi-naive recursion, stratified negation, and bounded aggregation for the current v1 slice, but aggregation is still intentionally limited to non-recursive grouped head-term `count`, `sum`, `min`, and `max` rules rather than richer post-v1 aggregate syntax.
- Extensional predicate binding is inferred by name against schema attributes and is therefore deliberately conservative.
- Explain traces currently reconstruct one merged proof graph per tuple; they do not yet distinguish alternative proof families for the same derived tuple.
- The DSL now covers the current canonical v1 surface, but it still lacks post-v1 ergonomic features such as richer type aliases, broader document modularity, and more generalized explain/query composition.
- The kernel service now has both in-memory and SQLite-backed execution paths, and the pilot HTTP path now supports bearer-token auth plus persisted audit logs, but the boundary is still not multi-tenant or production-hardened.
- Artifact and vector sidecar federation is now journal-subordinated and temporally exact on the SQLite-backed pilot path, but it is still a single-node backend and does not yet replicate or fail over independently of the kernel process.
- Vector search can now project provenance-bearing semantic facts back into the rule layer, but the current projection is deliberately narrow: a three-field `(query_entity, matched_entity, score)` extensional fact shape.
- Coordination semantics now cover heartbeats and execution outcomes in the pilot slice, but expiry still relies on explicit semantic state rather than clock-driven timeout windows or distributed failure detection.
- HTTP authorization still uses coarse endpoint scopes, but tokens now also bind maximum semantic policy visibility for history, state, documents, explanation, sidecar access, and reports. The remaining gap is finer-grained policy governance, not the absence of token-bound semantic policy.
- Audit entries now capture effective policy decisions plus requested, granted, and effective semantic visibility, but they still do not capture full operator intent or semantic diffs between cuts.
- Operator reports are now policy-aware fixed-format incident summaries in markdown and JSON, but they are still not interactive investigation tools.
- The pilot service now has a packaged deployment path with config-backed startup, package-local rotation tooling, and secret-file/env/command token resolution, but it is still a single-node bundle rather than a fully managed deployment story with automated rotation services, revocation, or native cloud secret-manager integrations.
- The performance suite now supports local baseline capture, drift comparison, stress fixtures, and a required CI launch/drift gate, but it does not yet maintain historical benchmark trends beyond uploaded workflow artifacts.
- Memory figures in the performance report are structural lower-bound estimates rather than allocator-exact telemetry.
- Durable storage is currently limited to a local SQLite journal; there is no snapshotting, replication, compaction, or backup story yet.
- The Go shell and Python SDK are now real, but both remain early boundary clients rather than mature ecosystem surfaces with richer async/notebook/admin layers.
