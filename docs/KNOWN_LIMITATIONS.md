# KNOWN_LIMITATIONS

- The runtime now executes semi-naive recursion and stratified negation for the current slice, but bounded aggregation and deeper optimizer-grade planning are still unimplemented.
- Extensional predicate binding is inferred by name against schema attributes and is therefore deliberately conservative.
- Explain traces currently reconstruct one merged proof graph per tuple; they do not yet distinguish alternative proof families for the same derived tuple.
- The DSL parser now supports facts, queries, `AsOf`, and policy annotations, but it is still a focused slice rather than the full canonical language.
- The kernel service now has both in-memory and SQLite-backed execution paths, but the HTTP boundary is not yet authenticated, multi-tenant, or production-hardened.
- The performance suite now provides local baselines and stress fixtures, but it does not yet maintain historical benchmark trends or CI-enforced drift budgets.
- Memory figures in the performance report are structural lower-bound estimates rather than allocator-exact telemetry.
- Durable storage is currently limited to a local SQLite journal; there is no snapshotting, replication, compaction, or backup story yet.
- Go and Python directories remain boundary placeholders rather than stable clients.
