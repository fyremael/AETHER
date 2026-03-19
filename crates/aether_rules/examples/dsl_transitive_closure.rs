use aether_ast::{
    AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, ReplicaId, Value,
};
use aether_resolver::{MaterializedResolver, Resolver};
use aether_rules::{DefaultDslParser, DefaultRuleCompiler, DslParser, RuleCompiler};
use aether_runtime::{RuleRuntime, SemiNaiveRuntime};
use std::error::Error;

const PROGRAM: &str = r#"
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
"#;

fn main() -> Result<(), Box<dyn Error>> {
    let document = DefaultDslParser.parse_document(PROGRAM)?;
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
                panic!("expected binary entity tuple");
            };
            (left.0, right.0)
        })
        .collect::<Vec<_>>();
    pairs.sort_unstable();

    println!("AETHER DSL example: transitive closure");
    println!("pairs: {pairs:?}");
    println!(
        "iteration deltas: {:?}",
        derived
            .iterations
            .iter()
            .map(|iteration| iteration.delta_size)
            .collect::<Vec<_>>()
    );

    Ok(())
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
