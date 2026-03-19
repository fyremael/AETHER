mod parser;

pub use parser::{DefaultDslParser, DslDocument, DslParser, ParseError};

use aether_ast::{
    Atom, AttributeId, ExtensionalFact, Literal, PredicateId, RuleAst, RuleProgram, Value, Variable,
};
use aether_plan::{CompiledProgram, DeltaRulePlan, DependencyGraph, StronglyConnectedComponent};
use aether_schema::{AttributeSchema, Schema, SchemaError, ValueType};
use indexmap::{IndexMap, IndexSet};
use thiserror::Error;

pub trait RuleCompiler {
    fn compile(
        &self,
        schema: &Schema,
        program: &RuleProgram,
    ) -> Result<CompiledProgram, CompileError>;
}

#[derive(Default)]
pub struct DefaultRuleCompiler;

impl RuleCompiler for DefaultRuleCompiler {
    fn compile(
        &self,
        schema: &Schema,
        program: &RuleProgram,
    ) -> Result<CompiledProgram, CompileError> {
        let mut dependency_graph = DependencyGraph::default();
        let mut all_predicates = IndexSet::new();
        let mut negative_edges = Vec::new();
        let mut delta_plans = Vec::new();

        for predicate in &program.predicates {
            schema.validate_predicate_arity(&predicate.id, predicate.arity)?;
            all_predicates.insert(predicate.id);
        }

        for fact in &program.facts {
            validate_fact(schema, fact)?;
            all_predicates.insert(fact.predicate.id);
        }

        for rule in &program.rules {
            validate_atom(schema, &rule.head)?;
            all_predicates.insert(rule.head.predicate.id);

            let positive_variables = positive_variables(rule);
            validate_rule_safety(rule, &positive_variables)?;

            let mut source_predicates = Vec::new();
            for literal in &rule.body {
                let atom = literal_atom(literal);
                validate_atom(schema, atom)?;
                all_predicates.insert(atom.predicate.id);

                match literal {
                    Literal::Positive(atom) => {
                        dependency_graph.add_edge(rule.head.predicate.id, atom.predicate.id);
                        source_predicates.push(atom.predicate.id);
                    }
                    Literal::Negative(atom) => {
                        negative_edges.push((rule.head.predicate.id, atom.predicate.id));
                    }
                }
            }

            delta_plans.push(DeltaRulePlan {
                rule_id: rule.id,
                target_predicate: rule.head.predicate.id,
                source_predicates,
            });
        }

        for predicate in &all_predicates {
            dependency_graph.edges.entry(*predicate).or_default();
        }

        let sccs = compute_sccs(&dependency_graph, &all_predicates);
        let scc_lookup = build_scc_lookup(&sccs);
        for (head, dependency) in &negative_edges {
            if scc_lookup.get(head) == scc_lookup.get(dependency) {
                return Err(CompileError::UnstratifiedNegation {
                    depender: predicate_label(schema, *head),
                    dependency: predicate_label(schema, *dependency),
                });
            }
        }
        let predicate_strata =
            compute_predicate_strata(schema, &dependency_graph, &scc_lookup, &negative_edges)?;

        let phase_graph = build_phase_graph(schema, &dependency_graph, &sccs, &scc_lookup);
        let extensional_bindings = infer_extensional_bindings(schema, program)?;

        Ok(CompiledProgram {
            dependency_graph,
            sccs,
            phase_graph,
            delta_plans,
            materialized: program.materialized.clone(),
            rules: program.rules.clone(),
            extensional_bindings,
            facts: program.facts.clone(),
            predicate_strata,
        })
    }
}

fn validate_atom(schema: &Schema, atom: &Atom) -> Result<(), CompileError> {
    schema.validate_predicate_arity(&atom.predicate.id, atom.terms.len())?;
    Ok(())
}

fn validate_fact(schema: &Schema, fact: &ExtensionalFact) -> Result<(), CompileError> {
    schema.validate_predicate_arity(&fact.predicate.id, fact.values.len())?;
    let signature = schema
        .predicate(&fact.predicate.id)
        .ok_or(SchemaError::UnknownPredicate(fact.predicate.id))?;
    for (value, expected) in fact.values.iter().zip(&signature.fields) {
        if !value_matches_type(value, expected) {
            return Err(CompileError::FactTypeMismatch {
                predicate: fact.predicate.name.clone(),
                expected: signature.fields.clone(),
                actual: fact.values.iter().map(value_type_of).collect(),
            });
        }
    }
    Ok(())
}

fn positive_variables(rule: &RuleAst) -> IndexSet<Variable> {
    let mut variables = IndexSet::new();
    for literal in &rule.body {
        if let Literal::Positive(atom) = literal {
            variables.extend(atom_variables(atom));
        }
    }
    variables
}

fn validate_rule_safety(
    rule: &RuleAst,
    positive_variables: &IndexSet<Variable>,
) -> Result<(), CompileError> {
    for variable in atom_variables(&rule.head) {
        if !positive_variables.contains(&variable) {
            return Err(CompileError::UnsafeVariable {
                rule_id: rule.id,
                variable: variable.0,
            });
        }
    }

    for literal in &rule.body {
        if let Literal::Negative(atom) = literal {
            for variable in atom_variables(atom) {
                if !positive_variables.contains(&variable) {
                    return Err(CompileError::UnsafeVariable {
                        rule_id: rule.id,
                        variable: variable.0,
                    });
                }
            }
        }
    }

    Ok(())
}

fn atom_variables(atom: &Atom) -> IndexSet<Variable> {
    atom.terms
        .iter()
        .filter_map(|term| match term {
            aether_ast::Term::Variable(variable) => Some(variable.clone()),
            aether_ast::Term::Value(_) => None,
        })
        .collect()
}

fn literal_atom(literal: &Literal) -> &Atom {
    match literal {
        Literal::Positive(atom) | Literal::Negative(atom) => atom,
    }
}

fn compute_sccs(
    graph: &DependencyGraph,
    predicates: &IndexSet<PredicateId>,
) -> Vec<StronglyConnectedComponent> {
    let mut visited = IndexSet::new();
    let mut order = Vec::new();

    for predicate in predicates {
        dfs_forward(*predicate, graph, &mut visited, &mut order);
    }

    let reversed = reverse_graph(graph, predicates);
    visited.clear();

    let mut sccs = Vec::new();
    let mut next_id = 0usize;
    while let Some(predicate) = order.pop() {
        if visited.contains(&predicate) {
            continue;
        }
        let mut component = Vec::new();
        dfs_reverse(predicate, &reversed, &mut visited, &mut component);
        component.sort();
        sccs.push(StronglyConnectedComponent {
            id: next_id,
            predicates: component,
        });
        next_id += 1;
    }

    sccs
}

fn dfs_forward(
    start: PredicateId,
    graph: &DependencyGraph,
    visited: &mut IndexSet<PredicateId>,
    order: &mut Vec<PredicateId>,
) {
    if !visited.insert(start) {
        return;
    }

    if let Some(neighbors) = graph.edges.get(&start) {
        for neighbor in neighbors {
            dfs_forward(*neighbor, graph, visited, order);
        }
    }

    order.push(start);
}

fn reverse_graph(
    graph: &DependencyGraph,
    predicates: &IndexSet<PredicateId>,
) -> IndexMap<PredicateId, Vec<PredicateId>> {
    let mut reversed: IndexMap<PredicateId, Vec<PredicateId>> = predicates
        .iter()
        .map(|predicate| (*predicate, Vec::new()))
        .collect();

    for (head, dependencies) in &graph.edges {
        for dependency in dependencies {
            reversed.entry(*dependency).or_default().push(*head);
        }
    }

    reversed
}

fn dfs_reverse(
    start: PredicateId,
    graph: &IndexMap<PredicateId, Vec<PredicateId>>,
    visited: &mut IndexSet<PredicateId>,
    component: &mut Vec<PredicateId>,
) {
    if !visited.insert(start) {
        return;
    }

    component.push(start);
    if let Some(neighbors) = graph.get(&start) {
        for neighbor in neighbors {
            dfs_reverse(*neighbor, graph, visited, component);
        }
    }
}

fn build_scc_lookup(sccs: &[StronglyConnectedComponent]) -> IndexMap<PredicateId, usize> {
    let mut lookup = IndexMap::new();
    for scc in sccs {
        for predicate in &scc.predicates {
            lookup.insert(*predicate, scc.id);
        }
    }
    lookup
}

fn build_phase_graph(
    schema: &Schema,
    graph: &DependencyGraph,
    sccs: &[StronglyConnectedComponent],
    scc_lookup: &IndexMap<PredicateId, usize>,
) -> aether_ast::PhaseGraph {
    let mut nodes = Vec::new();
    let mut edges = IndexSet::new();

    for scc in sccs {
        let provides: Vec<String> = scc
            .predicates
            .iter()
            .map(|predicate| predicate_label(schema, *predicate))
            .collect();
        let mut available = Vec::new();

        for predicate in &scc.predicates {
            if let Some(dependencies) = graph.edges.get(predicate) {
                for dependency in dependencies {
                    let dependency_scc = *scc_lookup
                        .get(dependency)
                        .expect("predicate present in scc lookup");
                    if dependency_scc != scc.id {
                        available.push(predicate_label(schema, *dependency));
                        edges.insert((dependency_scc, scc.id));
                    }
                }
            }
        }

        available.sort();
        available.dedup();

        let recursive = scc.predicates.len() > 1
            || scc.predicates.iter().any(|predicate| {
                graph
                    .edges
                    .get(predicate)
                    .is_some_and(|deps| deps.contains(predicate))
            });

        nodes.push(aether_ast::PhaseNode {
            id: format!("scc-{}", scc.id),
            signature: aether_ast::PhaseSignature {
                available,
                provides: provides.clone(),
                keep: provides,
            },
            recursive_scc: recursive.then_some(scc.id),
        });
    }

    let edges = edges
        .into_iter()
        .map(|(from, to)| aether_ast::PhaseEdge {
            from: format!("scc-{}", from),
            to: format!("scc-{}", to),
        })
        .collect();

    aether_ast::PhaseGraph { nodes, edges }
}

fn compute_predicate_strata(
    _schema: &Schema,
    graph: &DependencyGraph,
    scc_lookup: &IndexMap<PredicateId, usize>,
    negative_edges: &[(PredicateId, PredicateId)],
) -> Result<IndexMap<PredicateId, usize>, CompileError> {
    let mut condensed_edges: IndexMap<(usize, usize), usize> = IndexMap::new();
    let mut scc_ids = IndexSet::new();
    for scc_id in scc_lookup.values() {
        scc_ids.insert(*scc_id);
    }

    for (head, dependencies) in &graph.edges {
        let to = *scc_lookup
            .get(head)
            .expect("head predicate should be present in scc lookup");
        for dependency in dependencies {
            let from = *scc_lookup
                .get(dependency)
                .expect("dependency predicate should be present in scc lookup");
            if from != to {
                scc_ids.insert(from);
                scc_ids.insert(to);
                condensed_edges.entry((from, to)).or_insert(0);
            }
        }
    }

    for (head, dependency) in negative_edges {
        let to = *scc_lookup
            .get(head)
            .expect("negative head predicate should be present in scc lookup");
        let from = *scc_lookup
            .get(dependency)
            .expect("negative dependency predicate should be present in scc lookup");
        if from != to {
            scc_ids.insert(from);
            scc_ids.insert(to);
            condensed_edges
                .entry((from, to))
                .and_modify(|weight| *weight = (*weight).max(1))
                .or_insert(1);
        }
    }

    let mut outgoing: IndexMap<usize, Vec<(usize, usize)>> = scc_ids
        .iter()
        .copied()
        .map(|scc_id| (scc_id, Vec::new()))
        .collect();
    let mut indegree: IndexMap<usize, usize> = scc_ids
        .iter()
        .copied()
        .map(|scc_id| (scc_id, 0usize))
        .collect();

    for ((from, to), weight) in condensed_edges {
        outgoing.entry(from).or_default().push((to, weight));
        *indegree.entry(to).or_default() += 1;
    }

    let mut ready = indegree
        .iter()
        .filter_map(|(scc_id, degree)| (*degree == 0).then_some(*scc_id))
        .collect::<Vec<_>>();
    ready.sort_unstable();

    let mut order = Vec::new();
    while let Some(scc_id) = ready.first().copied() {
        ready.remove(0);
        order.push(scc_id);
        if let Some(edges) = outgoing.get(&scc_id) {
            for (to, _) in edges {
                let degree = indegree
                    .get_mut(to)
                    .expect("target scc should have indegree entry");
                *degree -= 1;
                if *degree == 0 {
                    ready.push(*to);
                    ready.sort_unstable();
                }
            }
        }
    }

    if order.len() != indegree.len() {
        return Err(CompileError::UnstratifiedNegation {
            depender: "program".into(),
            dependency: "negative cycle".into(),
        });
    }

    let mut scc_strata: IndexMap<usize, usize> = scc_ids
        .iter()
        .copied()
        .map(|scc_id| (scc_id, 0usize))
        .collect();
    for scc_id in order {
        let current = *scc_strata
            .get(&scc_id)
            .expect("source scc should have a stratum");
        if let Some(edges) = outgoing.get(&scc_id) {
            for (to, weight) in edges {
                let target = scc_strata
                    .get_mut(to)
                    .expect("target scc should have a stratum");
                *target = (*target).max(current + *weight);
            }
        }
    }

    Ok(scc_lookup
        .iter()
        .map(|(predicate, scc_id)| {
            (
                *predicate,
                *scc_strata
                    .get(scc_id)
                    .expect("predicate scc should have a stratum"),
            )
        })
        .collect())
}

fn infer_extensional_bindings(
    schema: &Schema,
    program: &RuleProgram,
) -> Result<IndexMap<PredicateId, AttributeId>, CompileError> {
    let mut bindings = IndexMap::new();

    for predicate in &program.predicates {
        if predicate.arity != 2 {
            continue;
        }

        if let Some(attribute) = matching_attribute(schema, &predicate.name) {
            validate_extensional_binding(schema, predicate.id, attribute)?;
            bindings.insert(predicate.id, attribute.id);
        }
    }

    Ok(bindings)
}

fn matching_attribute<'a>(schema: &'a Schema, predicate_name: &str) -> Option<&'a AttributeSchema> {
    let mut candidates = vec![predicate_name.to_owned()];
    if predicate_name.contains('_') {
        candidates.push(predicate_name.replacen('_', ".", 1));
        candidates.push(predicate_name.replace('_', "."));
    }

    candidates.dedup();

    candidates.into_iter().find_map(|candidate| {
        schema
            .attributes
            .values()
            .find(|attribute| attribute.name == candidate)
    })
}

fn validate_extensional_binding(
    schema: &Schema,
    predicate: PredicateId,
    attribute: &AttributeSchema,
) -> Result<(), CompileError> {
    let signature = schema
        .predicate(&predicate)
        .expect("validated predicates are present in schema");
    let expected_fields = vec![ValueType::Entity, attribute.value_type.clone()];

    if signature.fields != expected_fields {
        return Err(CompileError::IncompatibleExtensionalBinding {
            predicate: signature.name.clone(),
            attribute: attribute.name.clone(),
            expected_fields,
            actual_fields: signature.fields.clone(),
        });
    }

    Ok(())
}

fn predicate_label(schema: &Schema, predicate: PredicateId) -> String {
    schema
        .predicate(&predicate)
        .map(|signature| signature.name.clone())
        .unwrap_or_else(|| format!("predicate-{}", predicate))
}

fn value_matches_type(value: &Value, expected: &ValueType) -> bool {
    match (value, expected) {
        (Value::Null, _) => true,
        (Value::Bool(_), ValueType::Bool) => true,
        (Value::I64(_), ValueType::I64) => true,
        (Value::U64(_), ValueType::U64) => true,
        (Value::F64(_), ValueType::F64) => true,
        (Value::String(_), ValueType::String) => true,
        (Value::Bytes(_), ValueType::Bytes) => true,
        (Value::Entity(_), ValueType::Entity) => true,
        (Value::List(values), ValueType::List(inner)) => {
            values.iter().all(|value| value_matches_type(value, inner))
        }
        _ => false,
    }
}

fn value_type_of(value: &Value) -> ValueType {
    match value {
        Value::Null => ValueType::String,
        Value::Bool(_) => ValueType::Bool,
        Value::I64(_) => ValueType::I64,
        Value::U64(_) => ValueType::U64,
        Value::F64(_) => ValueType::F64,
        Value::String(_) => ValueType::String,
        Value::Bytes(_) => ValueType::Bytes,
        Value::Entity(_) => ValueType::Entity,
        Value::List(values) => ValueType::List(Box::new(
            values
                .first()
                .map(value_type_of)
                .unwrap_or(ValueType::String),
        )),
    }
}

#[derive(Debug, Error)]
pub enum CompileError {
    #[error(transparent)]
    Schema(#[from] SchemaError),
    #[error(
        "predicate {predicate} cannot bind to attribute {attribute}: expected {expected_fields:?}, found {actual_fields:?}"
    )]
    IncompatibleExtensionalBinding {
        predicate: String,
        attribute: String,
        expected_fields: Vec<ValueType>,
        actual_fields: Vec<ValueType>,
    },
    #[error(
        "fact for predicate {predicate} does not match the declared types: expected {expected:?}, found {actual:?}"
    )]
    FactTypeMismatch {
        predicate: String,
        expected: Vec<ValueType>,
        actual: Vec<ValueType>,
    },
    #[error("rule {rule_id} uses unsafe variable {variable}")]
    UnsafeVariable {
        rule_id: aether_ast::RuleId,
        variable: String,
    },
    #[error("unstratified negation detected: {depender} depends negatively on {dependency}")]
    UnstratifiedNegation {
        depender: String,
        dependency: String,
    },
}

#[cfg(test)]
mod tests {
    use super::{CompileError, DefaultRuleCompiler, RuleCompiler};
    use aether_ast::{
        Atom, AttributeId, ExtensionalFact, Literal, PredicateId, PredicateRef, RuleAst, RuleId,
        RuleProgram, Term, Value, Variable,
    };
    use aether_schema::{AttributeClass, AttributeSchema, PredicateSignature, Schema, ValueType};

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

    fn schema(predicates: &[(u64, &str, usize)]) -> Schema {
        let mut schema = Schema::new("v1");
        for (id, name, arity) in predicates {
            schema
                .register_predicate(PredicateSignature {
                    id: PredicateId::new(*id),
                    name: (*name).into(),
                    fields: vec![ValueType::Entity; *arity],
                })
                .expect("register predicate");
        }
        schema
    }

    #[test]
    fn safe_recursive_program_builds_expected_graph_and_phase_boundaries() {
        let edge = predicate(1, "edge", 2);
        let reach = predicate(2, "reach", 2);
        let schema = schema(&[(1, "edge", 2), (2, "reach", 2)]);
        let program = RuleProgram {
            predicates: vec![edge.clone(), reach.clone()],
            rules: vec![
                RuleAst {
                    id: RuleId::new(1),
                    head: atom(reach.clone(), &["x", "y"]),
                    body: vec![Literal::Positive(atom(edge.clone(), &["x", "y"]))],
                },
                RuleAst {
                    id: RuleId::new(2),
                    head: atom(reach.clone(), &["x", "z"]),
                    body: vec![
                        Literal::Positive(atom(reach.clone(), &["x", "y"])),
                        Literal::Positive(atom(edge.clone(), &["y", "z"])),
                    ],
                },
            ],
            materialized: vec![reach.id],
            facts: Vec::new(),
        };

        let compiled = DefaultRuleCompiler
            .compile(&schema, &program)
            .expect("compile recursive program");
        let reach_edges = compiled
            .dependency_graph
            .edges
            .get(&reach.id)
            .expect("reach edges");

        assert!(reach_edges.contains(&edge.id));
        assert!(reach_edges.contains(&reach.id));
        assert_eq!(compiled.sccs.len(), 2);

        let reach_scc = compiled
            .sccs
            .iter()
            .find(|scc| scc.predicates.contains(&reach.id))
            .expect("reach scc");
        let edge_scc = compiled
            .sccs
            .iter()
            .find(|scc| scc.predicates.contains(&edge.id))
            .expect("edge scc");
        let reach_node = compiled
            .phase_graph
            .nodes
            .iter()
            .find(|node| node.id == format!("scc-{}", reach_scc.id))
            .expect("reach phase node");
        let edge_node = compiled
            .phase_graph
            .nodes
            .iter()
            .find(|node| node.id == format!("scc-{}", edge_scc.id))
            .expect("edge phase node");

        assert_eq!(reach_node.recursive_scc, Some(reach_scc.id));
        assert_eq!(edge_node.recursive_scc, None);
        assert_eq!(compiled.predicate_strata.get(&edge.id).copied(), Some(0));
        assert_eq!(compiled.predicate_strata.get(&reach.id).copied(), Some(0));
        assert!(compiled.phase_graph.edges.iter().any(|edge_ref| {
            edge_ref.from == format!("scc-{}", edge_scc.id)
                && edge_ref.to == format!("scc-{}", reach_scc.id)
        }));
        assert_eq!(compiled.rules, program.rules);
    }

    #[test]
    fn extensional_predicates_bind_to_matching_attribute_names() {
        let task_depends_on = predicate(10, "task_depends_on", 2);
        let depends_transitive = predicate(11, "depends_transitive", 2);
        let mut schema = schema(&[(10, "task_depends_on", 2), (11, "depends_transitive", 2)]);
        schema
            .register_attribute(AttributeSchema {
                id: AttributeId::new(21),
                name: "task.depends_on".into(),
                class: AttributeClass::RefSet,
                value_type: ValueType::Entity,
            })
            .expect("register attribute");

        let compiled = DefaultRuleCompiler
            .compile(
                &schema,
                &RuleProgram {
                    predicates: vec![task_depends_on.clone(), depends_transitive.clone()],
                    rules: vec![RuleAst {
                        id: RuleId::new(1),
                        head: atom(depends_transitive, &["x", "y"]),
                        body: vec![Literal::Positive(atom(
                            task_depends_on.clone(),
                            &["x", "y"],
                        ))],
                    }],
                    materialized: vec![task_depends_on.id],
                    facts: Vec::new(),
                },
            )
            .expect("compile program");

        assert_eq!(
            compiled.extensional_bindings.get(&task_depends_on.id),
            Some(&AttributeId::new(21))
        );
    }

    #[test]
    fn extensional_binding_rejects_type_mismatches() {
        let task_depends_on = predicate(10, "task_depends_on", 2);
        let mut schema = Schema::new("v1");
        schema
            .register_predicate(PredicateSignature {
                id: task_depends_on.id,
                name: task_depends_on.name.clone(),
                fields: vec![ValueType::String, ValueType::Entity],
            })
            .expect("register predicate");
        schema
            .register_attribute(AttributeSchema {
                id: AttributeId::new(21),
                name: "task.depends_on".into(),
                class: AttributeClass::RefSet,
                value_type: ValueType::Entity,
            })
            .expect("register attribute");

        let error = DefaultRuleCompiler
            .compile(
                &schema,
                &RuleProgram {
                    predicates: vec![task_depends_on],
                    rules: Vec::new(),
                    materialized: Vec::new(),
                    facts: Vec::new(),
                },
            )
            .expect_err("type-mismatched binding should fail");

        assert!(matches!(
            error,
            CompileError::IncompatibleExtensionalBinding {
                predicate,
                attribute,
                expected_fields,
                actual_fields,
            } if predicate == "task_depends_on"
                && attribute == "task.depends_on"
                && expected_fields == vec![ValueType::Entity, ValueType::Entity]
                && actual_fields == vec![ValueType::String, ValueType::Entity]
        ));
    }

    #[test]
    fn unsafe_variables_are_rejected() {
        let ready = predicate(1, "ready", 1);
        let edge = predicate(2, "edge", 2);
        let schema = schema(&[(1, "ready", 1), (2, "edge", 2)]);
        let program = RuleProgram {
            predicates: vec![ready.clone(), edge.clone()],
            rules: vec![RuleAst {
                id: RuleId::new(7),
                head: atom(ready, &["x"]),
                body: vec![Literal::Positive(atom(edge, &["y", "z"]))],
            }],
            materialized: Vec::new(),
            facts: Vec::new(),
        };

        let error = DefaultRuleCompiler
            .compile(&schema, &program)
            .expect_err("unsafe rule should fail");
        assert!(matches!(
            error,
            CompileError::UnsafeVariable { variable, .. } if variable == "x"
        ));
    }

    #[test]
    fn unstratified_negation_in_recursive_component_is_rejected() {
        let p = predicate(1, "p", 1);
        let q = predicate(2, "q", 1);
        let schema = schema(&[(1, "p", 1), (2, "q", 1)]);
        let program = RuleProgram {
            predicates: vec![p.clone(), q.clone()],
            rules: vec![
                RuleAst {
                    id: RuleId::new(1),
                    head: atom(p.clone(), &["x"]),
                    body: vec![Literal::Positive(atom(q.clone(), &["x"]))],
                },
                RuleAst {
                    id: RuleId::new(2),
                    head: atom(q.clone(), &["x"]),
                    body: vec![
                        Literal::Positive(atom(p.clone(), &["x"])),
                        Literal::Negative(atom(p, &["x"])),
                    ],
                },
            ],
            materialized: Vec::new(),
            facts: Vec::new(),
        };

        let error = DefaultRuleCompiler
            .compile(&schema, &program)
            .expect_err("unstratified negation should fail");
        assert!(matches!(
            error,
            CompileError::UnstratifiedNegation { depender, dependency }
                if depender == "q" && dependency == "p"
        ));
    }

    #[test]
    fn stratified_negation_assigns_higher_strata() {
        let task = predicate(1, "task", 1);
        let task_status = predicate(2, "task_status", 2);
        let task_complete = predicate(3, "task_complete", 1);
        let task_ready = predicate(4, "task_ready", 1);
        let mut schema = Schema::new("v1");
        for signature in [
            PredicateSignature {
                id: task.id,
                name: task.name.clone(),
                fields: vec![ValueType::Entity],
            },
            PredicateSignature {
                id: task_status.id,
                name: task_status.name.clone(),
                fields: vec![ValueType::Entity, ValueType::String],
            },
            PredicateSignature {
                id: task_complete.id,
                name: task_complete.name.clone(),
                fields: vec![ValueType::Entity],
            },
            PredicateSignature {
                id: task_ready.id,
                name: task_ready.name.clone(),
                fields: vec![ValueType::Entity],
            },
        ] {
            schema
                .register_predicate(signature)
                .expect("register predicate");
        }
        schema
            .register_attribute(AttributeSchema {
                id: AttributeId::new(20),
                name: "task.status".into(),
                class: AttributeClass::ScalarLww,
                value_type: ValueType::String,
            })
            .expect("register attribute");

        let compiled = DefaultRuleCompiler
            .compile(
                &schema,
                &RuleProgram {
                    predicates: vec![
                        task.clone(),
                        task_status.clone(),
                        task_complete.clone(),
                        task_ready.clone(),
                    ],
                    rules: vec![
                        RuleAst {
                            id: RuleId::new(1),
                            head: atom(task_complete.clone(), &["x"]),
                            body: vec![Literal::Positive(Atom {
                                predicate: task_status.clone(),
                                terms: vec![
                                    Term::Variable(Variable::new("x")),
                                    Term::Value(Value::String("done".into())),
                                ],
                            })],
                        },
                        RuleAst {
                            id: RuleId::new(2),
                            head: atom(task_ready.clone(), &["x"]),
                            body: vec![
                                Literal::Positive(atom(task.clone(), &["x"])),
                                Literal::Negative(atom(task_complete.clone(), &["x"])),
                            ],
                        },
                    ],
                    materialized: vec![task_ready.id],
                    facts: vec![ExtensionalFact {
                        predicate: task,
                        values: vec![Value::Entity(aether_ast::EntityId::new(1))],
                        policy: None,
                    }],
                },
            )
            .expect("compile stratified program");

        assert_eq!(
            compiled.predicate_strata.get(&task_complete.id).copied(),
            Some(0)
        );
        assert_eq!(
            compiled.predicate_strata.get(&task_ready.id).copied(),
            Some(1)
        );
        assert_eq!(compiled.facts.len(), 1);
    }
}
