# KNOWN_LIMITATIONS

- The runtime currently supports a narrow positive monotone recursive slice; it does not yet execute stratified negation or bounded aggregation.
- The current fixed-point evaluator is intentionally simple and is not yet a fully optimized semi-naive engine.
- Extensional predicate binding is inferred by name against schema attributes and is therefore deliberately conservative.
- Derived tuples currently record parent derived tuple IDs, but `source_datom_ids` are not yet populated.
- The canonical AETHER DSL parser does not exist yet; rules are authored through the Rust AST surface for now.
- Storage is currently in-memory only.
- Go and Python directories remain boundary placeholders rather than stable clients.
