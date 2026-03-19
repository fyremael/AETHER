use aether_ast::{
    DerivedTuple, DerivedTupleMetadata, ElementId, Literal, PredicateId, RuleId, Term, Tuple,
    TupleId, Value, Variable,
};
use aether_plan::CompiledProgram;
use aether_resolver::ResolvedState;
use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub trait RuleRuntime {
    fn evaluate(
        &self,
        state: &ResolvedState,
        program: &CompiledProgram,
    ) -> Result<DerivedSet, RuntimeError>;
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RuntimeIteration {
    pub iteration: usize,
    pub delta_size: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DerivedSet {
    pub tuples: Vec<DerivedTuple>,
    pub iterations: Vec<RuntimeIteration>,
    pub predicate_index: IndexMap<PredicateId, Vec<TupleId>>,
}

impl DerivedSet {
    pub fn has_converged(&self) -> bool {
        match self.iterations.last() {
            Some(iteration) => iteration.delta_size == 0,
            None => true,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct RelationRow {
    values: Vec<Value>,
    tuple_id: Option<TupleId>,
    source_datom_ids: Vec<ElementId>,
}

#[derive(Clone, Debug, Default)]
struct MatchState {
    bindings: IndexMap<Variable, Value>,
    parent_tuple_ids: Vec<TupleId>,
    source_datom_ids: Vec<ElementId>,
}

#[derive(Default)]
pub struct SemiNaiveRuntime;

impl RuleRuntime for SemiNaiveRuntime {
    fn evaluate(
        &self,
        state: &ResolvedState,
        program: &CompiledProgram,
    ) -> Result<DerivedSet, RuntimeError> {
        let extensional_rows = build_extensional_rows(state, program);
        let intensional_predicates: IndexSet<PredicateId> = program
            .rules
            .iter()
            .map(|rule| rule.head.predicate.id)
            .collect();
        let scc_lookup = build_scc_lookup(program);

        let mut derived_by_predicate: IndexMap<PredicateId, Vec<RelationRow>> = IndexMap::new();
        let mut tuple_keys = IndexSet::new();
        let mut tuples = Vec::new();
        let mut iterations = Vec::new();
        let mut next_tuple_id = 1u64;
        let mut iteration = 1usize;

        loop {
            let snapshot = derived_by_predicate.clone();
            let mut delta_by_predicate: IndexMap<PredicateId, Vec<RelationRow>> = IndexMap::new();
            let mut delta_tuples = Vec::new();

            for rule in &program.rules {
                let matches = evaluate_rule_body(
                    rule,
                    &snapshot,
                    &extensional_rows,
                    &intensional_predicates,
                )?;
                let scc_id = scc_lookup
                    .get(&rule.head.predicate.id)
                    .copied()
                    .unwrap_or_default();
                // This runtime slice executes only positive-rule programs. In that subset,
                // every evaluated rule belongs to stratum 0.
                let stratum = 0usize;

                for matched in matches {
                    let values = materialize_head(rule.id, &rule.head.terms, &matched.bindings)?;
                    let key = tuple_key(rule.head.predicate.id, &values);
                    if tuple_keys.contains(&key) {
                        continue;
                    }

                    let tuple_id = TupleId::new(next_tuple_id);
                    next_tuple_id += 1;
                    tuple_keys.insert(key);

                    delta_by_predicate
                        .entry(rule.head.predicate.id)
                        .or_default()
                        .push(RelationRow {
                            values: values.clone(),
                            tuple_id: Some(tuple_id),
                            source_datom_ids: matched.source_datom_ids.clone(),
                        });
                    delta_tuples.push(DerivedTuple {
                        tuple: Tuple {
                            id: tuple_id,
                            predicate: rule.head.predicate.id,
                            values,
                        },
                        metadata: DerivedTupleMetadata {
                            rule_id: rule.id,
                            predicate_id: rule.head.predicate.id,
                            stratum,
                            scc_id,
                            iteration,
                            parent_tuple_ids: matched.parent_tuple_ids,
                            source_datom_ids: matched.source_datom_ids,
                        },
                    });
                }
            }

            let delta_size = delta_tuples.len();
            iterations.push(RuntimeIteration {
                iteration,
                delta_size,
            });

            if delta_size == 0 {
                break;
            }

            for (predicate, rows) in delta_by_predicate {
                derived_by_predicate
                    .entry(predicate)
                    .or_default()
                    .extend(rows);
            }
            tuples.extend(delta_tuples);
            iteration += 1;
        }

        let mut predicate_index = program
            .materialized
            .iter()
            .copied()
            .map(|predicate| (predicate, Vec::new()))
            .collect::<IndexMap<_, _>>();
        for tuple in &tuples {
            predicate_index
                .entry(tuple.tuple.predicate)
                .or_default()
                .push(tuple.tuple.id);
        }

        Ok(DerivedSet {
            tuples,
            iterations,
            predicate_index,
        })
    }
}

fn build_extensional_rows(
    state: &ResolvedState,
    program: &CompiledProgram,
) -> IndexMap<PredicateId, Vec<RelationRow>> {
    let mut rows = IndexMap::new();

    for (predicate, attribute) in &program.extensional_bindings {
        let mut predicate_rows = Vec::new();
        for (entity_id, entity_state) in &state.entities {
            predicate_rows.extend(entity_state.facts(attribute).iter().cloned().map(|fact| {
                RelationRow {
                    values: vec![Value::Entity(*entity_id), fact.value],
                    tuple_id: None,
                    source_datom_ids: fact.source_datom_ids,
                }
            }));
        }
        rows.insert(*predicate, predicate_rows);
    }

    rows
}

fn evaluate_rule_body(
    rule: &aether_ast::RuleAst,
    derived_rows: &IndexMap<PredicateId, Vec<RelationRow>>,
    extensional_rows: &IndexMap<PredicateId, Vec<RelationRow>>,
    intensional_predicates: &IndexSet<PredicateId>,
) -> Result<Vec<MatchState>, RuntimeError> {
    let mut states = vec![MatchState::default()];

    for literal in &rule.body {
        let atom = match literal {
            Literal::Positive(atom) => atom,
            Literal::Negative(_) => return Err(RuntimeError::UnsupportedNegation(rule.id)),
        };
        let rows = relation_rows(
            atom.predicate.id,
            derived_rows,
            extensional_rows,
            intensional_predicates,
        )?;
        let mut next_states = Vec::new();

        for state in &states {
            for row in rows {
                if let Some(bindings) = unify_terms(&state.bindings, &atom.terms, &row.values) {
                    let mut parent_tuple_ids = state.parent_tuple_ids.clone();
                    if let Some(tuple_id) = row.tuple_id {
                        if !parent_tuple_ids.contains(&tuple_id) {
                            parent_tuple_ids.push(tuple_id);
                        }
                    }
                    let mut source_datom_ids = state.source_datom_ids.clone();
                    extend_unique(&mut source_datom_ids, &row.source_datom_ids);
                    next_states.push(MatchState {
                        bindings,
                        parent_tuple_ids,
                        source_datom_ids,
                    });
                }
            }
        }

        states = next_states;
        if states.is_empty() {
            break;
        }
    }

    Ok(states)
}

fn relation_rows<'a>(
    predicate: PredicateId,
    derived_rows: &'a IndexMap<PredicateId, Vec<RelationRow>>,
    extensional_rows: &'a IndexMap<PredicateId, Vec<RelationRow>>,
    intensional_predicates: &IndexSet<PredicateId>,
) -> Result<&'a [RelationRow], RuntimeError> {
    if intensional_predicates.contains(&predicate) {
        Ok(derived_rows
            .get(&predicate)
            .map(Vec::as_slice)
            .unwrap_or(&[]))
    } else {
        extensional_rows
            .get(&predicate)
            .map(Vec::as_slice)
            .ok_or(RuntimeError::MissingExtensionalBinding(predicate))
    }
}

fn unify_terms(
    bindings: &IndexMap<Variable, Value>,
    terms: &[Term],
    values: &[Value],
) -> Option<IndexMap<Variable, Value>> {
    if terms.len() != values.len() {
        return None;
    }

    let mut next_bindings = bindings.clone();
    for (term, value) in terms.iter().zip(values) {
        match term {
            Term::Variable(variable) => match next_bindings.get(variable) {
                Some(bound) if bound != value => return None,
                Some(_) => {}
                None => {
                    next_bindings.insert(variable.clone(), value.clone());
                }
            },
            Term::Value(expected) if expected != value => return None,
            Term::Value(_) => {}
        }
    }

    Some(next_bindings)
}

fn materialize_head(
    rule_id: RuleId,
    terms: &[Term],
    bindings: &IndexMap<Variable, Value>,
) -> Result<Vec<Value>, RuntimeError> {
    terms
        .iter()
        .map(|term| match term {
            Term::Variable(variable) => {
                bindings
                    .get(variable)
                    .cloned()
                    .ok_or_else(|| RuntimeError::UnboundVariable {
                        rule_id,
                        variable: variable.0.clone(),
                    })
            }
            Term::Value(value) => Ok(value.clone()),
        })
        .collect()
}

fn build_scc_lookup(program: &CompiledProgram) -> IndexMap<PredicateId, usize> {
    let mut lookup = IndexMap::new();
    for scc in &program.sccs {
        for predicate in &scc.predicates {
            lookup.insert(*predicate, scc.id);
        }
    }
    lookup
}

fn tuple_key(predicate: PredicateId, values: &[Value]) -> String {
    let mut key = format!("{}#", predicate.0);
    for value in values {
        key.push_str(&value_key(value));
        key.push('|');
    }
    key
}

fn extend_unique<T>(target: &mut Vec<T>, additions: &[T])
where
    T: Copy + Eq,
{
    for addition in additions {
        if !target.contains(addition) {
            target.push(*addition);
        }
    }
}

fn value_key(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(inner) => format!("bool:{inner}"),
        Value::I64(inner) => format!("i64:{inner}"),
        Value::U64(inner) => format!("u64:{inner}"),
        Value::F64(inner) => format!("f64:{:016x}", inner.to_bits()),
        Value::String(inner) => format!("string:{}:{inner}", inner.len()),
        Value::Bytes(inner) => format!("bytes:{inner:?}"),
        Value::Entity(inner) => format!("entity:{}", inner.0),
        Value::List(inner) => {
            let mut rendered = String::from("list:[");
            for value in inner {
                rendered.push_str(&value_key(value));
                rendered.push(',');
            }
            rendered.push(']');
            rendered
        }
    }
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("predicate {0} has no extensional binding in the compiled program")]
    MissingExtensionalBinding(PredicateId),
    #[error("rule {0} uses negation, which is not implemented in this runtime slice")]
    UnsupportedNegation(RuleId),
    #[error("rule {rule_id} references unbound variable {variable}")]
    UnboundVariable { rule_id: RuleId, variable: String },
}

#[cfg(test)]
mod tests {
    use super::{RuleRuntime, RuntimeError, SemiNaiveRuntime};
    use aether_ast::{
        Atom, AttributeId, Datom, DatomProvenance, ElementId, EntityId, Literal, OperationKind,
        PredicateId, PredicateRef, ReplicaId, RuleAst, RuleId, RuleProgram, Term, Value, Variable,
    };
    use aether_resolver::{MaterializedResolver, Resolver};
    use aether_rules::{DefaultRuleCompiler, RuleCompiler};
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

    fn transitive_schema() -> Schema {
        let mut schema = Schema::new("v1");
        schema
            .register_attribute(AttributeSchema {
                id: AttributeId::new(1),
                name: "task.depends_on".into(),
                class: AttributeClass::RefSet,
                value_type: ValueType::Entity,
            })
            .expect("register attribute");
        schema
            .register_predicate(PredicateSignature {
                id: PredicateId::new(1),
                name: "task_depends_on".into(),
                fields: vec![ValueType::Entity, ValueType::Entity],
            })
            .expect("register extensional predicate");
        schema
            .register_predicate(PredicateSignature {
                id: PredicateId::new(2),
                name: "depends_transitive".into(),
                fields: vec![ValueType::Entity, ValueType::Entity],
            })
            .expect("register recursive predicate");
        schema
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

    #[test]
    fn monotone_transitive_closure_converges_with_iteration_metadata() {
        let schema = transitive_schema();
        let program = RuleProgram {
            predicates: vec![
                predicate(1, "task_depends_on", 2),
                predicate(2, "depends_transitive", 2),
            ],
            rules: vec![
                RuleAst {
                    id: RuleId::new(1),
                    head: atom(predicate(2, "depends_transitive", 2), &["x", "y"]),
                    body: vec![Literal::Positive(atom(
                        predicate(1, "task_depends_on", 2),
                        &["x", "y"],
                    ))],
                },
                RuleAst {
                    id: RuleId::new(2),
                    head: atom(predicate(2, "depends_transitive", 2), &["x", "z"]),
                    body: vec![
                        Literal::Positive(atom(predicate(2, "depends_transitive", 2), &["x", "y"])),
                        Literal::Positive(atom(predicate(1, "task_depends_on", 2), &["y", "z"])),
                    ],
                },
            ],
            materialized: vec![PredicateId::new(2)],
        };
        let datoms = vec![
            dependency_datom(1, 2, 1),
            dependency_datom(2, 3, 2),
            dependency_datom(3, 4, 3),
        ];
        let state = MaterializedResolver
            .current(&schema, &datoms)
            .expect("resolve current state");
        let compiled = DefaultRuleCompiler
            .compile(&schema, &program)
            .expect("compile recursive program");

        let derived = SemiNaiveRuntime
            .evaluate(&state, &compiled)
            .expect("evaluate recursive closure");

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

        assert_eq!(pairs, vec![(1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4)]);
        assert_eq!(
            derived
                .iterations
                .iter()
                .map(|iteration| iteration.delta_size)
                .collect::<Vec<_>>(),
            vec![3, 2, 1, 0]
        );
        assert!(derived.has_converged());
        assert_eq!(
            derived
                .predicate_index
                .get(&PredicateId::new(2))
                .map(Vec::len),
            Some(6)
        );

        let longest_path = derived
            .tuples
            .iter()
            .find(|tuple| {
                tuple.tuple.values
                    == vec![
                        Value::Entity(EntityId::new(1)),
                        Value::Entity(EntityId::new(4)),
                    ]
            })
            .expect("longest-path tuple");
        assert_eq!(longest_path.metadata.rule_id, RuleId::new(2));
        assert_eq!(longest_path.metadata.iteration, 3);
        assert_eq!(longest_path.metadata.stratum, 0);
        assert!(!longest_path.metadata.parent_tuple_ids.is_empty());
        assert_eq!(
            longest_path.metadata.source_datom_ids,
            vec![ElementId::new(1), ElementId::new(2), ElementId::new(3)]
        );
        assert!(derived
            .tuples
            .iter()
            .all(|tuple| tuple.metadata.stratum == 0));
        let base_edge = derived
            .tuples
            .iter()
            .find(|tuple| {
                tuple.tuple.values
                    == vec![
                        Value::Entity(EntityId::new(1)),
                        Value::Entity(EntityId::new(2)),
                    ]
            })
            .expect("base edge tuple");
        assert_eq!(base_edge.metadata.source_datom_ids, vec![ElementId::new(1)]);
    }

    #[test]
    fn missing_extensional_binding_is_reported() {
        let mut schema = Schema::new("v1");
        schema
            .register_predicate(PredicateSignature {
                id: PredicateId::new(10),
                name: "edge".into(),
                fields: vec![ValueType::Entity, ValueType::Entity],
            })
            .expect("register edge");
        schema
            .register_predicate(PredicateSignature {
                id: PredicateId::new(11),
                name: "reach".into(),
                fields: vec![ValueType::Entity, ValueType::Entity],
            })
            .expect("register reach");
        let program = RuleProgram {
            predicates: vec![predicate(10, "edge", 2), predicate(11, "reach", 2)],
            rules: vec![RuleAst {
                id: RuleId::new(1),
                head: atom(predicate(11, "reach", 2), &["x", "y"]),
                body: vec![Literal::Positive(atom(
                    predicate(10, "edge", 2),
                    &["x", "y"],
                ))],
            }],
            materialized: vec![PredicateId::new(11)],
        };
        let compiled = DefaultRuleCompiler
            .compile(&schema, &program)
            .expect("compile unbound program");

        let error = SemiNaiveRuntime
            .evaluate(&Default::default(), &compiled)
            .expect_err("missing extensional binding should fail");
        assert!(matches!(
            error,
            RuntimeError::MissingExtensionalBinding(id) if id == PredicateId::new(10)
        ));
    }
}
