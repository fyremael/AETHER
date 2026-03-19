# Worked Example: Transitive Closure

This example demonstrates the first real recursive loop currently implemented in AETHER.

It shows how to:

1. define a schema in the AETHER DSL
2. express recursive rules in the same document
3. parse that document into `Schema + RuleProgram`
4. resolve current state from datoms
5. compile the parsed rule program
6. evaluate it to a fixpoint
7. inspect both derived tuples and iteration metadata

## Scenario

We model a simple dependency chain:

- task `1` depends on task `2`
- task `2` depends on task `3`
- task `3` depends on task `4`

From those extensional facts, we want to derive the transitive dependency relation `depends_transitive(x, y)`.

## DSL Document

```text
schema v1 {
  attr task.depends_on: RefSet<Entity>
}

predicates {
  task_depends_on(Entity, Entity)
  depends_transitive(Entity, Entity)
}

rules {
  depends_transitive(x, y) <- task_depends_on(x, y)
  depends_transitive(x, z) <- depends_transitive(x, y), task_depends_on(y, z)
}

materialize {
  depends_transitive
}
```

## Example

```rust
use aether_ast::{
    AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, ReplicaId, Value,
};
use aether_resolver::{MaterializedResolver, Resolver};
use aether_rules::{DefaultDslParser, DefaultRuleCompiler, DslParser, RuleCompiler};
use aether_runtime::{RuleRuntime, SemiNaiveRuntime};

fn dependency_datom(entity: u64, value: u64, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: AttributeId::new(1),
        value: Value::Entity(EntityId::new(value)),
        op: OperationKind::Add,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let document = DefaultDslParser.parse_document(
        r#"
        schema v1 {
          attr task.depends_on: RefSet<Entity>
        }

        predicates {
          task_depends_on(Entity, Entity)
          depends_transitive(Entity, Entity)
        }

        rules {
          depends_transitive(x, y) <- task_depends_on(x, y)
          depends_transitive(x, z) <- depends_transitive(x, y), task_depends_on(y, z)
        }

        materialize {
          depends_transitive
        }
        "#,
    )?;

    let datoms = vec![
        dependency_datom(1, 2, 1),
        dependency_datom(2, 3, 2),
        dependency_datom(3, 4, 3),
    ];

    let state = MaterializedResolver.current(&document.schema, &datoms)?;
    let compiled = DefaultRuleCompiler.compile(&document.schema, &document.program)?;
    let derived = SemiNaiveRuntime.evaluate(&state, &compiled)?;

    let mut pairs = derived
        .tuples
        .iter()
        .map(|tuple| {
            let [Value::Entity(left), Value::Entity(right)] = &tuple.tuple.values[..] else {
                unreachable!("binary entity tuple")
            };
            (left.0, right.0)
        })
        .collect::<Vec<_>>();
    pairs.sort_unstable();

    assert_eq!(pairs, vec![(1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4)]);
    assert_eq!(
        derived
            .iterations
            .iter()
            .map(|iteration| iteration.delta_size)
            .collect::<Vec<_>>(),
        vec![3, 2, 1, 0]
    );

    Ok(())
}
```

## What The Example Proves

This example proves several important things about the current implementation:

- the textual DSL can already express core schema, predicate, rule, and materialization sections
- the parser lowers that document deterministically into the Rust semantic core
- extensional predicates can be lifted from resolved attributes
- recursive intensional predicates can be compiled and evaluated
- the runtime converges deterministically
- derived tuples carry iteration metadata
- the project has crossed the line from structural scaffold to executable semantic slice

## Where To See It In Executable Form

Runnable example:

```bash
cargo run -p aether_rules --example dsl_transitive_closure
```

Code:

- `crates/aether_rules/examples/dsl_transitive_closure.rs`
- `crates/aether_runtime/src/lib.rs`
