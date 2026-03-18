# Worked Example: Transitive Closure

This example demonstrates the first real recursive loop currently implemented in AETHER.

It shows how to:

1. define a schema with an extensional relationship
2. express recursive rules over that relationship
3. resolve current state from datoms
4. compile the rule program
5. evaluate it to a fixpoint
6. inspect both derived tuples and iteration metadata

Until the DSL parser exists, the example is authored through the Rust AST surface.

## Scenario

We model a simple dependency chain:

- task `1` depends on task `2`
- task `2` depends on task `3`
- task `3` depends on task `4`

From those extensional facts, we want to derive the transitive dependency relation `depends_transitive(x, y)`.

## Example

```rust
use aether_ast::{
    Atom, AttributeId, Datom, DatomProvenance, ElementId, EntityId, Literal, OperationKind,
    PredicateId, PredicateRef, ReplicaId, RuleAst, RuleId, RuleProgram, Term, Value, Variable,
};
use aether_resolver::{MaterializedResolver, Resolver};
use aether_rules::{DefaultRuleCompiler, RuleCompiler};
use aether_runtime::{RuleRuntime, SemiNaiveRuntime};
use aether_schema::{
    AttributeClass, AttributeSchema, PredicateSignature, Schema, ValueType,
};

fn predicate(id: u64, name: &str, arity: usize) -> PredicateRef {
    PredicateRef {
        id: PredicateId::new(id),
        name: name.into(),
        arity,
    }
}

fn atom(predicate: PredicateRef, vars: &[&str]) -> Atom {
    Atom {
        predicate,
        terms: vars
            .iter()
            .map(|name| Term::Variable(Variable::new(*name)))
            .collect(),
    }
}

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
    let mut schema = Schema::new("v1");
    schema.register_attribute(AttributeSchema {
        id: AttributeId::new(1),
        name: "task.depends_on".into(),
        class: AttributeClass::RefSet,
        value_type: ValueType::Entity,
    })?;
    schema.register_predicate(PredicateSignature {
        id: PredicateId::new(1),
        name: "task_depends_on".into(),
        fields: vec![ValueType::Entity, ValueType::Entity],
    })?;
    schema.register_predicate(PredicateSignature {
        id: PredicateId::new(2),
        name: "depends_transitive".into(),
        fields: vec![ValueType::Entity, ValueType::Entity],
    })?;

    let task_depends_on = predicate(1, "task_depends_on", 2);
    let depends_transitive = predicate(2, "depends_transitive", 2);

    let program = RuleProgram {
        predicates: vec![task_depends_on.clone(), depends_transitive.clone()],
        rules: vec![
            RuleAst {
                id: RuleId::new(1),
                head: atom(depends_transitive.clone(), &["x", "y"]),
                body: vec![Literal::Positive(atom(task_depends_on.clone(), &["x", "y"]))],
            },
            RuleAst {
                id: RuleId::new(2),
                head: atom(depends_transitive.clone(), &["x", "z"]),
                body: vec![
                    Literal::Positive(atom(depends_transitive.clone(), &["x", "y"])),
                    Literal::Positive(atom(task_depends_on.clone(), &["y", "z"])),
                ],
            },
        ],
        materialized: vec![depends_transitive.id],
    };

    let datoms = vec![
        dependency_datom(1, 2, 1),
        dependency_datom(2, 3, 2),
        dependency_datom(3, 4, 3),
    ];

    let state = MaterializedResolver.current(&schema, &datoms)?;
    let compiled = DefaultRuleCompiler.compile(&schema, &program)?;
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

- extensional predicates can be lifted from resolved attributes
- recursive intensional predicates can be compiled and evaluated
- the runtime converges deterministically
- derived tuples carry iteration metadata
- the project has crossed the line from structural scaffold to executable semantic slice

## Where To See It In Executable Form

The same scenario is covered by the runtime unit tests in `crates/aether_runtime/src/lib.rs`.
