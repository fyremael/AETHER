# KNOWN_LIMITATIONS

- The runtime now executes semi-naive recursion, stratified negation, and bounded aggregation for the current slice, but aggregation is still limited to non-recursive head-term `count`, `sum`, `min`, and `max` rules rather than a fuller generalized aggregate surface.
- Extensional predicate binding is inferred by name against schema attributes and is therefore deliberately conservative.
- Explain traces currently reconstruct one merged proof graph per tuple; they do not yet distinguish alternative proof families for the same derived tuple.
- The DSL now covers the current canonical v1 surface, but it still lacks post-v1 ergonomic features such as richer type aliases, broader document modularity, and more generalized explain/query composition.
- The kernel service now has both in-memory and SQLite-backed execution paths, and the pilot HTTP path now supports bearer-token auth plus persisted audit logs, but the boundary is still not multi-tenant or production-hardened.
- Artifact and vector sidecar federation is now durable on the SQLite-backed pilot path, but it is still a single-node backend and does not yet replicate or fail over independently of the kernel process.
- Vector search can now project provenance-bearing semantic facts back into the rule layer, but the current projection is deliberately narrow: a three-field `(query_entity, matched_entity, score)` extensional fact shape.
- Coordination semantics now cover heartbeats and execution outcomes in the pilot slice, but expiry still relies on explicit semantic state rather than clock-driven timeout windows or distributed failure detection.
- HTTP authorization is currently coarse endpoint scope enforcement; it is not yet semantic row-level or policy-envelope-aware authorization.
- Audit entries now capture semantic cut, query goal, tuple ID, datom counts, and basic result counts, but they still do not capture full operator intent, row-level policy context, or semantic diffs between cuts.
- Operator reports are now saved as markdown and JSON artifacts, but they are still fixed-format incident summaries rather than interactive investigation tools.
- The performance suite now supports local baseline capture, drift comparison, and stress fixtures, but it does not yet maintain historical benchmark trends or CI-enforced drift budgets.
- The pilot launch validation pack now has a scheduled/manual GitHub Actions workflow, but it is not yet a required gate on every mainline change.
- Memory figures in the performance report are structural lower-bound estimates rather than allocator-exact telemetry.
- Durable storage is currently limited to a local SQLite journal; there is no snapshotting, replication, compaction, or backup story yet.
- Go remains a placeholder, and the Python boundary is a minimal HTTP client rather than a broader typed SDK with fixtures, notebooks, or async support.
