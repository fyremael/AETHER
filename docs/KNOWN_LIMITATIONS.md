# KNOWN_LIMITATIONS

- The runtime now executes semi-naive recursion and stratified negation for the current slice, but bounded aggregation and deeper optimizer-grade planning are still unimplemented.
- Extensional predicate binding is inferred by name against schema attributes and is therefore deliberately conservative.
- Explain traces currently reconstruct one merged proof graph per tuple; they do not yet distinguish alternative proof families for the same derived tuple.
- The DSL parser now supports facts, queries, `AsOf`, and policy annotations, but it is still a focused slice rather than the full canonical language.
- The kernel service now has both in-memory and SQLite-backed execution paths, and the pilot HTTP path now supports bearer-token auth plus persisted audit logs, but the boundary is still not multi-tenant or production-hardened.
- HTTP authorization is currently coarse endpoint scope enforcement; it is not yet semantic row-level or policy-envelope-aware authorization.
- Audit entries currently capture principal, method, path, scope, status, and time; they do not yet capture richer semantic-cut context or full operator intent.
- Operator reports are now saved as markdown and JSON artifacts, but they are still fixed-format incident summaries rather than interactive investigation tools.
- The performance suite now supports local baseline capture, drift comparison, and stress fixtures, but it does not yet maintain historical benchmark trends or CI-enforced drift budgets.
- Memory figures in the performance report are structural lower-bound estimates rather than allocator-exact telemetry.
- Durable storage is currently limited to a local SQLite journal; there is no snapshotting, replication, compaction, or backup story yet.
- Go and Python directories remain boundary placeholders rather than stable clients.
