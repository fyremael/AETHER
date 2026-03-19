# KNOWN_LIMITATIONS

- The runtime currently supports a narrow positive monotone recursive slice; it does not yet execute stratified negation or bounded aggregation.
- The current fixed-point evaluator is intentionally simple and is not yet a fully optimized semi-naive engine.
- Extensional predicate binding is inferred by name against schema attributes and is therefore deliberately conservative.
- Explain traces currently reconstruct one merged proof graph per tuple; they do not yet distinguish alternative proof families for the same derived tuple.
- The DSL parser currently covers core `schema`, `predicates`, `rules`, and `materialize` sections, but not queries, temporal views, policy annotations, or domain-level type aliases.
- Storage is currently in-memory only.
- Go and Python directories remain boundary placeholders rather than stable clients.
