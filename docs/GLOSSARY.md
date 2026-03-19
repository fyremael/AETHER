# Glossary

This glossary defines the terms that appear throughout the AETHER codebase and documentation.

The intent is consistency. A project like AETHER becomes hard to reason about the moment every document uses slightly different words for the same thing.

## Core Terms

### Datom

The smallest append-only fact record in the journal.

A datom carries an entity, attribute, value, operation, element ID, replica ID, causal context, provenance, and optional policy envelope.

### Element

The causal event identity attached to a datom.

When people say `AsOf(e7)`, they mean “evaluate the journal prefix up to and including element `e7`.”

### Journal

The append-only history of datoms.

The journal is the historical source of truth. It is not already resolved state.

### Resolver

The component that turns journal history into semantic state.

It answers `Current` and `AsOf` questions under attribute-class-specific merge rules.

### `Current`

The resolved semantic state at the head of the journal.

### `AsOf`

The resolved semantic state at an inclusive journal prefix identified by an element ID.

## Schema And Rule Terms

### Attribute

A named field in the semantic substrate, such as `task.depends_on` or `task.status`.

Attributes are stored in the journal and resolved into state.

### Attribute class

The merge behavior attached to an attribute, such as scalar last-writer-wins, set, or sequence.

### Predicate

A logical relation used by the rule system, such as `task_ready(Entity)` or `lease_active(Entity, String, U64)`.

### Extensional predicate

A predicate whose facts come from resolved state or directly authored facts.

In practical terms, it is input to derivation rather than output from derivation.

### Intensional predicate

A predicate derived by rules.

### Materialized predicate

A predicate whose derived tuples are retained in the runtime output for querying or explanation.

### Rule

A logical implication of the form `head <- body`.

### Safety

The rule constraint that variables appearing in the head or negative literals must also appear in positive literals.

### Stratification

The property that allows negation to be evaluated safely in layers rather than in cycles.

### SCC

Strongly connected component.

In AETHER, SCCs group mutually recursive predicates so the runtime knows which rules must be iterated together.

### Stratum

A layer in the dependency ordering used to evaluate negation safely.

## Runtime Terms

### Semi-naive evaluation

An evaluation strategy that uses deltas rather than recomputing every recursive fact from scratch on each iteration.

### Derived tuple

A runtime-produced tuple that became true because of rule evaluation.

### Provenance

The metadata that explains where a fact or tuple came from.

At the runtime layer, this includes parent tuple references and source datom IDs.

### Proof trace

The explainer output that reconstructs why a derived tuple is true.

### Query

A logical request evaluated against extensional and derived relations after runtime evaluation.

## Boundary Terms

### Kernel service

The service-shaped boundary around the semantic kernel.

Today it is in-memory and lives in `aether_api`.

### Operator path

The non-developer way of exercising the system, usually through scripted demos and captured reports.

### Canonical language

The public semantics surface the project intends to stabilize around.

For AETHER, that language is the DSL, not host-language helper APIs.

## Documentation Terms

### Governing document

A document that defines architectural direction or required repository shape, such as `SPEC.md` or `REPO_LAYOUT.md`.

### Current-state document

A document that says what the system does today, such as `docs/STATUS.md`.

### Frontier document

A document that says where the system intentionally stops today, such as `docs/KNOWN_LIMITATIONS.md`.
