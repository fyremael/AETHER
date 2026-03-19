use aether_ast::{
    Atom, AttributeId, Datom, DatomProvenance, ElementId, EntityId, Literal, OperationKind,
    PredicateId, PredicateRef, ReplicaId, RuleAst, RuleId, RuleProgram, Term, Value, Variable,
};
use aether_resolver::{MaterializedResolver, Resolver};
use aether_rules::{DefaultRuleCompiler, RuleCompiler};
use aether_runtime::{DerivedSet, RuleRuntime, SemiNaiveRuntime};
use aether_schema::{AttributeClass, AttributeSchema, PredicateSignature, Schema, ValueType};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let schema = dependency_schema()?;
    let program = dependency_program();
    let journal = dependency_journal();

    let compiler = DefaultRuleCompiler;
    let resolver = MaterializedResolver;
    let runtime = SemiNaiveRuntime;

    let compiled = compiler.compile(&schema, &program)?;
    let as_of = resolver.as_of(&schema, &journal, &ElementId::new(3))?;
    let current = resolver.current(&schema, &journal)?;

    let derived_as_of = runtime.evaluate(&as_of, &compiled)?;
    let derived_current = runtime.evaluate(&current, &compiled)?;

    println!("AETHER Demo 01: Temporal Dependency Horizon");
    println!("===========================================");
    println!(
        "One compiled recursive program, two semantic snapshots, and a visibly different fixed point."
    );
    println!();

    println!("Compiler view");
    println!(
        "  rules: {} | SCCs: {} | recursive phases: {}",
        compiled.rules.len(),
        compiled.sccs.len(),
        compiled
            .phase_graph
            .nodes
            .iter()
            .filter(|node| node.recursive_scc.is_some())
            .count()
    );
    println!();

    println!("Journal");
    for datom in &journal {
        println!(
            "  [e{}] {} depends_on {}",
            datom.element,
            entity_label(datom.entity),
            value_label(&datom.value)
        );
    }
    println!();

    print_snapshot("AsOf(e3)", &derived_as_of, as_of.as_of.unwrap_or_default());
    println!();
    print_snapshot(
        "Current(e5)",
        &derived_current,
        current.as_of.unwrap_or_default(),
    );

    Ok(())
}

fn dependency_schema() -> Result<Schema, Box<dyn Error>> {
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
    Ok(schema)
}

fn dependency_program() -> RuleProgram {
    let task_depends_on = predicate(1, "task_depends_on", 2);
    let depends_transitive = predicate(2, "depends_transitive", 2);

    RuleProgram {
        predicates: vec![task_depends_on.clone(), depends_transitive.clone()],
        rules: vec![
            RuleAst {
                id: RuleId::new(1),
                head: atom(depends_transitive.clone(), &["x", "y"]),
                body: vec![Literal::Positive(atom(
                    task_depends_on.clone(),
                    &["x", "y"],
                ))],
            },
            RuleAst {
                id: RuleId::new(2),
                head: atom(depends_transitive, &["x", "z"]),
                body: vec![
                    Literal::Positive(atom(predicate(2, "depends_transitive", 2), &["x", "y"])),
                    Literal::Positive(atom(task_depends_on, &["y", "z"])),
                ],
            },
        ],
        materialized: vec![PredicateId::new(2)],
        facts: Vec::new(),
    }
}

fn dependency_journal() -> Vec<Datom> {
    vec![
        dependency_datom(1, 2, 1),
        dependency_datom(2, 3, 2),
        dependency_datom(3, 4, 3),
        dependency_datom(4, 5, 4),
        dependency_datom(5, 6, 5),
    ]
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

fn print_snapshot(label: &str, derived: &DerivedSet, as_of: ElementId) {
    let rows = closure_rows(derived);
    let deepest_tuple = derived
        .tuples
        .iter()
        .max_by_key(|tuple| {
            let [Value::Entity(from), Value::Entity(to)] = &tuple.tuple.values[..] else {
                panic!("expected entity-valued binary tuples");
            };
            (tuple.metadata.iteration, from.0, to.0)
        })
        .expect("demo always derives at least one row");
    let [Value::Entity(deepest_from), Value::Entity(deepest_to)] = &deepest_tuple.tuple.values[..]
    else {
        panic!("expected entity-valued binary tuples");
    };

    println!("{label}");
    println!("  journal prefix: e{as_of}");
    println!("  derived tuples: {}", derived.tuples.len());
    println!(
        "  iterations: {:?}",
        derived
            .iterations
            .iter()
            .map(|iteration| iteration.delta_size)
            .collect::<Vec<_>>()
    );
    println!("  closure:");
    for (from, to, _) in &rows {
        println!("    {} -> {}", entity_label(*from), entity_label(*to));
    }
    println!(
        "  longest proof: {} -> {} | rule {} | iteration {} | parents {:?}",
        entity_label(*deepest_from),
        entity_label(*deepest_to),
        deepest_tuple.metadata.rule_id,
        deepest_tuple.metadata.iteration,
        deepest_tuple.metadata.parent_tuple_ids
    );
}

fn closure_rows(derived: &DerivedSet) -> Vec<(EntityId, EntityId, aether_ast::TupleId)> {
    let mut rows = derived
        .tuples
        .iter()
        .map(|tuple| {
            let [Value::Entity(from), Value::Entity(to)] = &tuple.tuple.values[..] else {
                panic!("expected entity-valued binary tuples");
            };
            (*from, *to, tuple.tuple.id)
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|(from, to, _)| (from.0, to.0));
    rows
}

fn entity_label(entity: EntityId) -> String {
    format!("task/{}", entity.0)
}

fn value_label(value: &Value) -> String {
    match value {
        Value::Entity(entity) => entity_label(*entity),
        other => format!("{other:?}"),
    }
}
