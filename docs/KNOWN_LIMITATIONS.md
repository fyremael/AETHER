# KNOWN_LIMITATIONS

- The runtime now executes semi-naive recursion and stratified negation for the current slice, but bounded aggregation and deeper optimizer-grade planning are still unimplemented.
- Extensional predicate binding is inferred by name against schema attributes and is therefore deliberately conservative.
- Explain traces currently reconstruct one merged proof graph per tuple; they do not yet distinguish alternative proof families for the same derived tuple.
- The DSL parser now supports facts, queries, `AsOf`, and policy annotations, but it is still a focused slice rather than the full canonical language.
- The kernel service now has a minimal in-memory-backed HTTP JSON boundary, but it is not yet durable, authenticated, multi-tenant, or production-hardened.
- The performance suite now provides local baselines and stress fixtures, but it does not yet maintain historical benchmark trends or CI-enforced drift budgets.
- Memory figures in the performance report are structural lower-bound estimates rather than allocator-exact telemetry.
- Storage is currently in-memory only.
- Go and Python directories remain boundary placeholders rather than stable clients.
