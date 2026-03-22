use aether_ast::{
    AggregateFunction, AggregateTerm, DerivedTuple, DerivedTupleMetadata, ElementId, Literal,
    PredicateId, QueryAst, QueryResult, QueryRow, RuleAst, RuleId, Term, Tuple, TupleId, Value,
    Variable,
};
use aether_plan::CompiledProgram;
use aether_resolver::ResolvedState;
use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
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
    query_tuple_id: Option<TupleId>,
}

#[derive(Clone, Debug)]
struct AggregatedMatch {
    values: Vec<Value>,
    parent_tuple_ids: Vec<TupleId>,
    source_datom_ids: Vec<ElementId>,
}

#[derive(Clone, Debug)]
struct AggregateGroup {
    values: Vec<Option<Value>>,
    accumulator: AggregateAccumulator,
    seen_bindings: IndexSet<String>,
    parent_tuple_ids: Vec<TupleId>,
    source_datom_ids: Vec<ElementId>,
}

#[derive(Clone, Debug)]
enum AggregateAccumulator {
    Count(u64),
    SumI64(i64),
    SumU64(u64),
    SumF64(f64),
    Min(Value),
    Max(Value),
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
        let scc_order = build_scc_evaluation_order(program, &scc_lookup);
        let rules_by_scc = build_rules_by_scc(program, &scc_lookup);

        let mut derived_by_predicate: IndexMap<PredicateId, Vec<RelationRow>> = IndexMap::new();
        let mut tuple_keys = IndexSet::new();
        let mut tuples = Vec::new();
        let mut iterations = Vec::new();
        let mut next_tuple_id = 1u64;
        let mut iteration = 1usize;

        for scc_id in scc_order {
            let Some(rules) = rules_by_scc.get(&scc_id) else {
                continue;
            };
            let current_scc_predicates: IndexSet<PredicateId> =
                rules.iter().map(|rule| rule.head.predicate.id).collect();
            let stratum = rules
                .first()
                .and_then(|rule| program.predicate_strata.get(&rule.head.predicate.id))
                .copied()
                .unwrap_or_default();

            let mut delta_rows: IndexMap<PredicateId, Vec<RelationRow>> = IndexMap::new();
            loop {
                let mut batch_rows: IndexMap<PredicateId, Vec<RelationRow>> = IndexMap::new();
                let mut batch_tuples = Vec::new();

                for rule in rules {
                    let aggregate = head_aggregate(rule);
                    let anchor_indices = if aggregate.is_some() {
                        Vec::new()
                    } else {
                        current_scc_positive_indices(rule, &current_scc_predicates)
                    };
                    let anchor_plan = if delta_rows.is_empty() {
                        if anchor_indices.is_empty() {
                            vec![None]
                        } else {
                            Vec::new()
                        }
                    } else if anchor_indices.is_empty() {
                        Vec::new()
                    } else {
                        anchor_indices.into_iter().map(Some).collect()
                    };

                    let mut aggregate_matches = Vec::new();

                    for anchor_index in anchor_plan {
                        let matches = evaluate_rule_body_variant(
                            rule,
                            anchor_index,
                            &derived_by_predicate,
                            &delta_rows,
                            &extensional_rows,
                            &intensional_predicates,
                            &current_scc_predicates,
                        )?;

                        if aggregate.is_some() {
                            aggregate_matches.extend(matches);
                            continue;
                        }

                        for matched in matches {
                            let values = materialize_non_aggregate_head(
                                rule.id,
                                &rule.head.terms,
                                &matched.bindings,
                            )?;
                            let key = tuple_key(rule.head.predicate.id, &values);
                            if tuple_keys.contains(&key) {
                                continue;
                            }

                            let tuple_id = TupleId::new(next_tuple_id);
                            next_tuple_id += 1;
                            tuple_keys.insert(key);

                            batch_rows.entry(rule.head.predicate.id).or_default().push(
                                RelationRow {
                                    values: values.clone(),
                                    tuple_id: Some(tuple_id),
                                    source_datom_ids: matched.source_datom_ids.clone(),
                                },
                            );
                            batch_tuples.push(DerivedTuple {
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

                    if let Some((aggregate_index, aggregate_term)) = aggregate {
                        let matches = materialize_aggregate_head(
                            rule.id,
                            &rule.head.terms,
                            aggregate_index,
                            aggregate_term,
                            &aggregate_matches,
                        )?;
                        for matched in matches {
                            let key = tuple_key(rule.head.predicate.id, &matched.values);
                            if tuple_keys.contains(&key) {
                                continue;
                            }

                            let tuple_id = TupleId::new(next_tuple_id);
                            next_tuple_id += 1;
                            tuple_keys.insert(key);

                            batch_rows.entry(rule.head.predicate.id).or_default().push(
                                RelationRow {
                                    values: matched.values.clone(),
                                    tuple_id: Some(tuple_id),
                                    source_datom_ids: matched.source_datom_ids.clone(),
                                },
                            );
                            batch_tuples.push(DerivedTuple {
                                tuple: Tuple {
                                    id: tuple_id,
                                    predicate: rule.head.predicate.id,
                                    values: matched.values,
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
                }

                if batch_tuples.is_empty() {
                    break;
                }

                iterations.push(RuntimeIteration {
                    iteration,
                    delta_size: batch_tuples.len(),
                });
                iteration += 1;

                for (predicate, rows) in &batch_rows {
                    derived_by_predicate
                        .entry(*predicate)
                        .or_default()
                        .extend(rows.iter().cloned());
                }
                tuples.extend(batch_tuples);
                delta_rows = batch_rows;
            }
        }

        iterations.push(RuntimeIteration {
            iteration,
            delta_size: 0,
        });

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

pub fn execute_query(
    state: &ResolvedState,
    program: &CompiledProgram,
    derived: &DerivedSet,
    query: &QueryAst,
) -> Result<QueryResult, RuntimeError> {
    let extensional_rows = build_extensional_rows(state, program);
    let intensional_predicates: IndexSet<PredicateId> = program
        .rules
        .iter()
        .map(|rule| rule.head.predicate.id)
        .collect();
    let derived_rows = build_derived_rows(derived);

    let mut states = vec![MatchState::default()];
    for goal in &query.goals {
        let rows = positive_relation_rows(
            goal.predicate.id,
            None,
            &derived_rows,
            &IndexMap::new(),
            &extensional_rows,
            &intensional_predicates,
            &IndexSet::new(),
        )?;
        let mut next_states = Vec::new();

        for state in &states {
            for row in &rows {
                if let Some(bindings) = unify_terms(&state.bindings, &goal.terms, &row.values) {
                    next_states.push(MatchState {
                        bindings,
                        parent_tuple_ids: state.parent_tuple_ids.clone(),
                        source_datom_ids: state.source_datom_ids.clone(),
                        query_tuple_id: row.tuple_id.or(state.query_tuple_id),
                    });
                }
            }
        }

        states = next_states;
        if states.is_empty() {
            break;
        }
    }

    let mut rows = states
        .into_iter()
        .map(|state| QueryRow {
            values: if query.keep.is_empty() {
                state.bindings.values().cloned().collect()
            } else {
                query
                    .keep
                    .iter()
                    .filter_map(|variable| state.bindings.get(variable).cloned())
                    .collect()
            },
            tuple_id: state.query_tuple_id,
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| {
        let mut key = String::new();
        for value in &row.values {
            key.push_str(&value_key(value));
            key.push('|');
        }
        key
    });

    Ok(QueryResult { rows })
}

fn build_extensional_rows(
    state: &ResolvedState,
    program: &CompiledProgram,
) -> IndexMap<PredicateId, Vec<RelationRow>> {
    let mut rows: IndexMap<PredicateId, Vec<RelationRow>> = IndexMap::new();

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
        rows.entry(*predicate).or_default().extend(predicate_rows);
    }

    for fact in &program.facts {
        rows.entry(fact.predicate.id)
            .or_default()
            .push(RelationRow {
                values: fact.values.clone(),
                tuple_id: None,
                source_datom_ids: fact
                    .provenance
                    .as_ref()
                    .map(|provenance| provenance.source_datom_ids.clone())
                    .unwrap_or_default(),
            });
    }

    rows
}

fn build_derived_rows(derived: &DerivedSet) -> IndexMap<PredicateId, Vec<RelationRow>> {
    let mut rows: IndexMap<PredicateId, Vec<RelationRow>> = IndexMap::new();
    for tuple in &derived.tuples {
        rows.entry(tuple.tuple.predicate)
            .or_default()
            .push(RelationRow {
                values: tuple.tuple.values.clone(),
                tuple_id: Some(tuple.tuple.id),
                source_datom_ids: tuple.metadata.source_datom_ids.clone(),
            });
    }
    rows
}

fn build_rules_by_scc<'a>(
    program: &'a CompiledProgram,
    scc_lookup: &IndexMap<PredicateId, usize>,
) -> IndexMap<usize, Vec<&'a RuleAst>> {
    let mut rules = IndexMap::new();
    for rule in &program.rules {
        let scc_id = *scc_lookup
            .get(&rule.head.predicate.id)
            .expect("rule head predicate should be present in scc lookup");
        rules.entry(scc_id).or_insert_with(Vec::new).push(rule);
    }
    rules
}

fn build_scc_evaluation_order(
    program: &CompiledProgram,
    scc_lookup: &IndexMap<PredicateId, usize>,
) -> Vec<usize> {
    let mut edges = IndexSet::new();
    let mut indegree = program
        .sccs
        .iter()
        .map(|scc| (scc.id, 0usize))
        .collect::<IndexMap<_, _>>();
    let mut outgoing = program
        .sccs
        .iter()
        .map(|scc| (scc.id, Vec::new()))
        .collect::<IndexMap<_, _>>();

    for (head, dependencies) in &program.dependency_graph.edges {
        let head_scc = *scc_lookup
            .get(head)
            .expect("head predicate should be present in scc lookup");
        for dependency in dependencies {
            let dependency_scc = *scc_lookup
                .get(dependency)
                .expect("dependency predicate should be present in scc lookup");
            if dependency_scc != head_scc && edges.insert((dependency_scc, head_scc)) {
                outgoing.entry(dependency_scc).or_default().push(head_scc);
                *indegree.entry(head_scc).or_default() += 1;
            }
        }
    }

    let scc_strata = program
        .sccs
        .iter()
        .map(|scc| {
            let stratum = scc
                .predicates
                .first()
                .and_then(|predicate| program.predicate_strata.get(predicate))
                .copied()
                .unwrap_or_default();
            (scc.id, stratum)
        })
        .collect::<IndexMap<_, _>>();
    let mut ready = indegree
        .iter()
        .filter_map(|(scc_id, degree)| (*degree == 0).then_some(*scc_id))
        .collect::<Vec<_>>();
    ready.sort_by_key(|scc_id| (scc_strata.get(scc_id).copied().unwrap_or_default(), *scc_id));

    let mut order = Vec::new();
    while let Some(scc_id) = ready.first().copied() {
        ready.remove(0);
        order.push(scc_id);
        if let Some(neighbors) = outgoing.get(&scc_id) {
            for neighbor in neighbors {
                let degree = indegree
                    .get_mut(neighbor)
                    .expect("neighbor scc should have indegree");
                *degree -= 1;
                if *degree == 0 {
                    ready.push(*neighbor);
                    ready.sort_by_key(|candidate| {
                        (
                            scc_strata.get(candidate).copied().unwrap_or_default(),
                            *candidate,
                        )
                    });
                }
            }
        }
    }

    order
}

fn current_scc_positive_indices(
    rule: &RuleAst,
    current_scc_predicates: &IndexSet<PredicateId>,
) -> Vec<usize> {
    rule.body
        .iter()
        .enumerate()
        .filter_map(|(index, literal)| match literal {
            Literal::Positive(atom) if current_scc_predicates.contains(&atom.predicate.id) => {
                Some(index)
            }
            _ => None,
        })
        .collect()
}

fn evaluate_rule_body_variant(
    rule: &RuleAst,
    delta_anchor_index: Option<usize>,
    derived_rows: &IndexMap<PredicateId, Vec<RelationRow>>,
    delta_rows: &IndexMap<PredicateId, Vec<RelationRow>>,
    extensional_rows: &IndexMap<PredicateId, Vec<RelationRow>>,
    intensional_predicates: &IndexSet<PredicateId>,
    current_scc_predicates: &IndexSet<PredicateId>,
) -> Result<Vec<MatchState>, RuntimeError> {
    let mut states = vec![MatchState::default()];

    for (literal_index, literal) in ordered_rule_body(rule) {
        match literal {
            Literal::Positive(atom) => {
                let rows = positive_relation_rows(
                    atom.predicate.id,
                    (delta_anchor_index == Some(literal_index)).then_some(()),
                    derived_rows,
                    delta_rows,
                    extensional_rows,
                    intensional_predicates,
                    current_scc_predicates,
                )?;
                let mut next_states = Vec::new();

                for state in &states {
                    for row in &rows {
                        if let Some(bindings) =
                            unify_terms(&state.bindings, &atom.terms, &row.values)
                        {
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
                                query_tuple_id: row.tuple_id.or(state.query_tuple_id),
                            });
                        }
                    }
                }

                states = next_states;
            }
            Literal::Negative(atom) => {
                if current_scc_predicates.contains(&atom.predicate.id) {
                    return Err(RuntimeError::UnsupportedIntraStratumNegation(rule.id));
                }
                let rows = negative_relation_rows(
                    atom.predicate.id,
                    derived_rows,
                    extensional_rows,
                    intensional_predicates,
                )?;
                states.retain(|state| {
                    !rows
                        .iter()
                        .any(|row| unify_terms(&state.bindings, &atom.terms, &row.values).is_some())
                });
            }
        }

        if states.is_empty() {
            break;
        }
    }

    Ok(states)
}

fn ordered_rule_body(rule: &RuleAst) -> Vec<(usize, &Literal)> {
    let mut positives = Vec::new();
    let mut negatives = Vec::new();
    for (index, literal) in rule.body.iter().enumerate() {
        match literal {
            Literal::Positive(_) => positives.push((index, literal)),
            Literal::Negative(_) => negatives.push((index, literal)),
        }
    }
    positives.extend(negatives);
    positives
}

fn positive_relation_rows(
    predicate: PredicateId,
    use_delta: Option<()>,
    derived_rows: &IndexMap<PredicateId, Vec<RelationRow>>,
    delta_rows: &IndexMap<PredicateId, Vec<RelationRow>>,
    extensional_rows: &IndexMap<PredicateId, Vec<RelationRow>>,
    intensional_predicates: &IndexSet<PredicateId>,
    _current_scc_predicates: &IndexSet<PredicateId>,
) -> Result<Vec<RelationRow>, RuntimeError> {
    if use_delta.is_some() {
        return Ok(delta_rows.get(&predicate).cloned().unwrap_or_default());
    }
    if intensional_predicates.contains(&predicate) {
        return Ok(derived_rows.get(&predicate).cloned().unwrap_or_default());
    }

    extensional_rows
        .get(&predicate)
        .cloned()
        .ok_or(RuntimeError::MissingExtensionalBinding(predicate))
}

fn negative_relation_rows(
    predicate: PredicateId,
    derived_rows: &IndexMap<PredicateId, Vec<RelationRow>>,
    extensional_rows: &IndexMap<PredicateId, Vec<RelationRow>>,
    intensional_predicates: &IndexSet<PredicateId>,
) -> Result<Vec<RelationRow>, RuntimeError> {
    if intensional_predicates.contains(&predicate) {
        Ok(derived_rows.get(&predicate).cloned().unwrap_or_default())
    } else {
        extensional_rows
            .get(&predicate)
            .cloned()
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
            Term::Aggregate(_) => return None,
        }
    }

    Some(next_bindings)
}

fn materialize_non_aggregate_head(
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
            Term::Aggregate(_) => Err(RuntimeError::UnexpectedAggregate(rule_id)),
        })
        .collect()
}

fn materialize_aggregate_head(
    rule_id: RuleId,
    terms: &[Term],
    aggregate_index: usize,
    aggregate_term: &AggregateTerm,
    matches: &[MatchState],
) -> Result<Vec<AggregatedMatch>, RuntimeError> {
    let mut groups: IndexMap<String, AggregateGroup> = IndexMap::new();

    for matched in matches {
        let binding_key = bindings_key(&matched.bindings);
        let group_values =
            materialize_group_values(rule_id, terms, aggregate_index, &matched.bindings)?;
        let group_key = values_key(&group_values);
        let aggregate_value = matched
            .bindings
            .get(&aggregate_term.variable)
            .ok_or_else(|| RuntimeError::UnboundVariable {
                rule_id,
                variable: aggregate_term.variable.0.clone(),
            })?;

        if !groups.contains_key(&group_key) {
            let accumulator = AggregateAccumulator::from_value(
                rule_id,
                aggregate_term.function,
                aggregate_value,
            )?;
            groups.insert(
                group_key.clone(),
                AggregateGroup {
                    values: group_values.into_iter().map(Some).collect(),
                    accumulator,
                    seen_bindings: IndexSet::new(),
                    parent_tuple_ids: Vec::new(),
                    source_datom_ids: Vec::new(),
                },
            );
        }
        let group = groups
            .get_mut(&group_key)
            .expect("aggregate group should exist after insertion");

        if !group.seen_bindings.insert(binding_key) {
            continue;
        }

        if group.seen_bindings.len() > 1 {
            group
                .accumulator
                .add(rule_id, aggregate_term.function, aggregate_value)?;
        }
        extend_unique(&mut group.parent_tuple_ids, &matched.parent_tuple_ids);
        extend_unique(&mut group.source_datom_ids, &matched.source_datom_ids);
    }

    let mut aggregated = groups
        .into_values()
        .map(|group| {
            let mut values = group
                .values
                .into_iter()
                .map(|value| value.expect("group values are initialized"))
                .collect::<Vec<_>>();
            values[aggregate_index] = group.accumulator.finalize();
            AggregatedMatch {
                values,
                parent_tuple_ids: group.parent_tuple_ids,
                source_datom_ids: group.source_datom_ids,
            }
        })
        .collect::<Vec<_>>();

    aggregated.sort_by_key(|group| values_key(&group.values));
    Ok(aggregated)
}

fn materialize_group_values(
    rule_id: RuleId,
    terms: &[Term],
    aggregate_index: usize,
    bindings: &IndexMap<Variable, Value>,
) -> Result<Vec<Value>, RuntimeError> {
    terms
        .iter()
        .enumerate()
        .map(|(index, term)| {
            if index == aggregate_index {
                return Ok(Value::Null);
            }
            match term {
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
                Term::Aggregate(_) => Err(RuntimeError::UnexpectedAggregate(rule_id)),
            }
        })
        .collect()
}

fn head_aggregate(rule: &RuleAst) -> Option<(usize, &AggregateTerm)> {
    rule.head
        .terms
        .iter()
        .enumerate()
        .find_map(|(index, term)| match term {
            Term::Aggregate(aggregate) => Some((index, aggregate)),
            _ => None,
        })
}

fn bindings_key(bindings: &IndexMap<Variable, Value>) -> String {
    let mut entries = bindings
        .iter()
        .map(|(variable, value)| (variable.0.as_str(), value_key(value)))
        .collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.0.cmp(right.0));

    let mut rendered = String::new();
    for (variable, value) in entries {
        rendered.push_str(variable);
        rendered.push('=');
        rendered.push_str(&value);
        rendered.push('|');
    }
    rendered
}

fn values_key(values: &[Value]) -> String {
    let mut rendered = String::new();
    for value in values {
        rendered.push_str(&value_key(value));
        rendered.push('|');
    }
    rendered
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

impl AggregateAccumulator {
    fn from_value(
        rule_id: RuleId,
        function: AggregateFunction,
        value: &Value,
    ) -> Result<Self, RuntimeError> {
        match function {
            AggregateFunction::Count => Ok(Self::Count(1)),
            AggregateFunction::Sum => match value {
                Value::I64(inner) => Ok(Self::SumI64(*inner)),
                Value::U64(inner) => Ok(Self::SumU64(*inner)),
                Value::F64(inner) => Ok(Self::SumF64(*inner)),
                other => Err(RuntimeError::UnsupportedAggregateInput {
                    rule_id,
                    function,
                    actual: runtime_value_type(other),
                }),
            },
            AggregateFunction::Min => {
                validate_orderable_input(rule_id, function, value).map(|_| Self::Min(value.clone()))
            }
            AggregateFunction::Max => {
                validate_orderable_input(rule_id, function, value).map(|_| Self::Max(value.clone()))
            }
        }
    }

    fn add(
        &mut self,
        rule_id: RuleId,
        function: AggregateFunction,
        value: &Value,
    ) -> Result<(), RuntimeError> {
        match self {
            Self::Count(count) => {
                *count += 1;
                Ok(())
            }
            Self::SumI64(total) => match value {
                Value::I64(inner) => {
                    *total += inner;
                    Ok(())
                }
                other => Err(RuntimeError::AggregateInputTypeMismatch {
                    rule_id,
                    function,
                    expected: "I64".into(),
                    actual: runtime_value_type(other),
                }),
            },
            Self::SumU64(total) => match value {
                Value::U64(inner) => {
                    *total += inner;
                    Ok(())
                }
                other => Err(RuntimeError::AggregateInputTypeMismatch {
                    rule_id,
                    function,
                    expected: "U64".into(),
                    actual: runtime_value_type(other),
                }),
            },
            Self::SumF64(total) => match value {
                Value::F64(inner) => {
                    *total += inner;
                    Ok(())
                }
                other => Err(RuntimeError::AggregateInputTypeMismatch {
                    rule_id,
                    function,
                    expected: "F64".into(),
                    actual: runtime_value_type(other),
                }),
            },
            Self::Min(current) => {
                validate_orderable_input(rule_id, function, value)?;
                if compare_values(current, value)? == Ordering::Greater {
                    *current = value.clone();
                }
                Ok(())
            }
            Self::Max(current) => {
                validate_orderable_input(rule_id, function, value)?;
                if compare_values(current, value)? == Ordering::Less {
                    *current = value.clone();
                }
                Ok(())
            }
        }
    }

    fn finalize(self) -> Value {
        match self {
            Self::Count(inner) => Value::U64(inner),
            Self::SumI64(inner) => Value::I64(inner),
            Self::SumU64(inner) => Value::U64(inner),
            Self::SumF64(inner) => Value::F64(inner),
            Self::Min(inner) | Self::Max(inner) => inner,
        }
    }
}

fn validate_orderable_input(
    rule_id: RuleId,
    function: AggregateFunction,
    value: &Value,
) -> Result<(), RuntimeError> {
    match value {
        Value::I64(_) | Value::U64(_) | Value::F64(_) | Value::String(_) | Value::Entity(_) => {
            Ok(())
        }
        other => Err(RuntimeError::UnsupportedAggregateInput {
            rule_id,
            function,
            actual: runtime_value_type(other),
        }),
    }
}

fn compare_values(left: &Value, right: &Value) -> Result<Ordering, RuntimeError> {
    match (left, right) {
        (Value::I64(left_inner), Value::I64(right_inner)) => Ok(left_inner.cmp(right_inner)),
        (Value::U64(left_inner), Value::U64(right_inner)) => Ok(left_inner.cmp(right_inner)),
        (Value::F64(left_inner), Value::F64(right_inner)) => left_inner
            .partial_cmp(right_inner)
            .ok_or_else(|| RuntimeError::NonComparableAggregateValues {
                left: runtime_value_type(left),
                right: runtime_value_type(right),
            }),
        (Value::String(left_inner), Value::String(right_inner)) => Ok(left_inner.cmp(right_inner)),
        (Value::Entity(left_inner), Value::Entity(right_inner)) => Ok(left_inner.cmp(right_inner)),
        _ => Err(RuntimeError::NonComparableAggregateValues {
            left: runtime_value_type(left),
            right: runtime_value_type(right),
        }),
    }
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

fn runtime_value_type(value: &Value) -> String {
    match value {
        Value::Null => "Null".into(),
        Value::Bool(_) => "Bool".into(),
        Value::I64(_) => "I64".into(),
        Value::U64(_) => "U64".into(),
        Value::F64(_) => "F64".into(),
        Value::String(_) => "String".into(),
        Value::Bytes(_) => "Bytes".into(),
        Value::Entity(_) => "Entity".into(),
        Value::List(_) => "List".into(),
    }
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("predicate {0} has no extensional binding or fact rows in the compiled program")]
    MissingExtensionalBinding(PredicateId),
    #[error("rule {0} uses same-stratum negation, which is not supported")]
    UnsupportedIntraStratumNegation(RuleId),
    #[error("rule {rule_id} references unbound variable {variable}")]
    UnboundVariable { rule_id: RuleId, variable: String },
    #[error(
        "rule {0} requires grouped aggregate materialization, but was evaluated as a plain rule"
    )]
    UnexpectedAggregate(RuleId),
    #[error(
        "rule {rule_id} uses aggregate {function} over unsupported runtime value type {actual}"
    )]
    UnsupportedAggregateInput {
        rule_id: RuleId,
        function: AggregateFunction,
        actual: String,
    },
    #[error(
        "rule {rule_id} uses aggregate {function} with mixed runtime input types: expected {expected}, found {actual}"
    )]
    AggregateInputTypeMismatch {
        rule_id: RuleId,
        function: AggregateFunction,
        expected: String,
        actual: String,
    },
    #[error("aggregate comparison requires comparable values, found {left} and {right}")]
    NonComparableAggregateValues { left: String, right: String },
}

#[cfg(test)]
mod tests {
    use super::{execute_query, RuleRuntime, RuntimeError, SemiNaiveRuntime};
    use aether_ast::{
        AggregateFunction, AggregateTerm, Atom, AttributeId, Datom, DatomProvenance, ElementId,
        EntityId, ExtensionalFact, Literal, PredicateId, PredicateRef, QueryAst, QueryRow, RuleAst,
        RuleId, RuleProgram, Term, Value, Variable,
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

    fn aggregate(function: AggregateFunction, variable: &str) -> Term {
        Term::Aggregate(AggregateTerm {
            function,
            variable: Variable::new(variable),
        })
    }

    fn dependency_datom(entity: u64, value: u64, element: u64) -> Datom {
        Datom {
            entity: EntityId::new(entity),
            attribute: AttributeId::new(1),
            value: Value::Entity(EntityId::new(value)),
            op: aether_ast::OperationKind::Add,
            element: ElementId::new(element),
            replica: aether_ast::ReplicaId::new(1),
            causal_context: Default::default(),
            provenance: DatomProvenance::default(),
            policy: None,
        }
    }

    #[test]
    fn monotone_transitive_closure_converges_with_iteration_metadata() {
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
            facts: Vec::new(),
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
        assert_eq!(
            longest_path.metadata.source_datom_ids,
            vec![ElementId::new(1), ElementId::new(2), ElementId::new(3)]
        );
    }

    #[test]
    fn bounded_aggregation_materializes_counts_sums_and_maxima() {
        let mut schema = Schema::new("v1");
        for signature in [
            PredicateSignature {
                id: PredicateId::new(1),
                name: "edge".into(),
                fields: vec![ValueType::Entity, ValueType::Entity],
            },
            PredicateSignature {
                id: PredicateId::new(2),
                name: "reach".into(),
                fields: vec![ValueType::Entity, ValueType::Entity],
            },
            PredicateSignature {
                id: PredicateId::new(3),
                name: "reachable_count".into(),
                fields: vec![ValueType::Entity, ValueType::U64],
            },
            PredicateSignature {
                id: PredicateId::new(4),
                name: "project_task".into(),
                fields: vec![ValueType::Entity, ValueType::Entity],
            },
            PredicateSignature {
                id: PredicateId::new(5),
                name: "task_hours".into(),
                fields: vec![ValueType::Entity, ValueType::U64],
            },
            PredicateSignature {
                id: PredicateId::new(6),
                name: "project_hours".into(),
                fields: vec![ValueType::Entity, ValueType::U64],
            },
            PredicateSignature {
                id: PredicateId::new(7),
                name: "execution_attempt".into(),
                fields: vec![ValueType::Entity, ValueType::String, ValueType::U64],
            },
            PredicateSignature {
                id: PredicateId::new(8),
                name: "latest_epoch".into(),
                fields: vec![ValueType::Entity, ValueType::U64],
            },
        ] {
            schema
                .register_predicate(signature)
                .expect("register predicate");
        }

        let program = RuleProgram {
            predicates: vec![
                predicate(1, "edge", 2),
                predicate(2, "reach", 2),
                predicate(3, "reachable_count", 2),
                predicate(4, "project_task", 2),
                predicate(5, "task_hours", 2),
                predicate(6, "project_hours", 2),
                predicate(7, "execution_attempt", 3),
                predicate(8, "latest_epoch", 2),
            ],
            rules: vec![
                RuleAst {
                    id: RuleId::new(1),
                    head: atom(predicate(2, "reach", 2), &["x", "y"]),
                    body: vec![Literal::Positive(atom(
                        predicate(1, "edge", 2),
                        &["x", "y"],
                    ))],
                },
                RuleAst {
                    id: RuleId::new(2),
                    head: atom(predicate(2, "reach", 2), &["x", "z"]),
                    body: vec![
                        Literal::Positive(atom(predicate(2, "reach", 2), &["x", "y"])),
                        Literal::Positive(atom(predicate(1, "edge", 2), &["y", "z"])),
                    ],
                },
                RuleAst {
                    id: RuleId::new(3),
                    head: Atom {
                        predicate: predicate(3, "reachable_count", 2),
                        terms: vec![
                            Term::Variable(Variable::new("x")),
                            aggregate(AggregateFunction::Count, "y"),
                        ],
                    },
                    body: vec![Literal::Positive(atom(
                        predicate(2, "reach", 2),
                        &["x", "y"],
                    ))],
                },
                RuleAst {
                    id: RuleId::new(4),
                    head: Atom {
                        predicate: predicate(6, "project_hours", 2),
                        terms: vec![
                            Term::Variable(Variable::new("project")),
                            aggregate(AggregateFunction::Sum, "hours"),
                        ],
                    },
                    body: vec![
                        Literal::Positive(atom(
                            predicate(4, "project_task", 2),
                            &["project", "task"],
                        )),
                        Literal::Positive(atom(predicate(5, "task_hours", 2), &["task", "hours"])),
                    ],
                },
                RuleAst {
                    id: RuleId::new(5),
                    head: Atom {
                        predicate: predicate(8, "latest_epoch", 2),
                        terms: vec![
                            Term::Variable(Variable::new("task")),
                            aggregate(AggregateFunction::Max, "epoch"),
                        ],
                    },
                    body: vec![Literal::Positive(atom(
                        predicate(7, "execution_attempt", 3),
                        &["task", "worker", "epoch"],
                    ))],
                },
            ],
            materialized: vec![
                PredicateId::new(2),
                PredicateId::new(3),
                PredicateId::new(6),
                PredicateId::new(8),
            ],
            facts: vec![
                ExtensionalFact {
                    predicate: predicate(1, "edge", 2),
                    values: vec![
                        Value::Entity(EntityId::new(1)),
                        Value::Entity(EntityId::new(2)),
                    ],
                    policy: None,
                    provenance: None,
                },
                ExtensionalFact {
                    predicate: predicate(1, "edge", 2),
                    values: vec![
                        Value::Entity(EntityId::new(2)),
                        Value::Entity(EntityId::new(3)),
                    ],
                    policy: None,
                    provenance: None,
                },
                ExtensionalFact {
                    predicate: predicate(1, "edge", 2),
                    values: vec![
                        Value::Entity(EntityId::new(3)),
                        Value::Entity(EntityId::new(4)),
                    ],
                    policy: None,
                    provenance: None,
                },
                ExtensionalFact {
                    predicate: predicate(4, "project_task", 2),
                    values: vec![
                        Value::Entity(EntityId::new(10)),
                        Value::Entity(EntityId::new(101)),
                    ],
                    policy: None,
                    provenance: None,
                },
                ExtensionalFact {
                    predicate: predicate(4, "project_task", 2),
                    values: vec![
                        Value::Entity(EntityId::new(10)),
                        Value::Entity(EntityId::new(102)),
                    ],
                    policy: None,
                    provenance: None,
                },
                ExtensionalFact {
                    predicate: predicate(5, "task_hours", 2),
                    values: vec![Value::Entity(EntityId::new(101)), Value::U64(3)],
                    policy: None,
                    provenance: None,
                },
                ExtensionalFact {
                    predicate: predicate(5, "task_hours", 2),
                    values: vec![Value::Entity(EntityId::new(102)), Value::U64(5)],
                    policy: None,
                    provenance: None,
                },
                ExtensionalFact {
                    predicate: predicate(7, "execution_attempt", 3),
                    values: vec![
                        Value::Entity(EntityId::new(1)),
                        Value::String("worker-a".into()),
                        Value::U64(1),
                    ],
                    policy: None,
                    provenance: None,
                },
                ExtensionalFact {
                    predicate: predicate(7, "execution_attempt", 3),
                    values: vec![
                        Value::Entity(EntityId::new(1)),
                        Value::String("worker-b".into()),
                        Value::U64(4),
                    ],
                    policy: None,
                    provenance: None,
                },
            ],
        };

        let compiled = DefaultRuleCompiler
            .compile(&schema, &program)
            .expect("compile aggregate program");
        let derived = SemiNaiveRuntime
            .evaluate(&Default::default(), &compiled)
            .expect("evaluate aggregate program");

        let reachable_count = execute_query(
            &Default::default(),
            &compiled,
            &derived,
            &QueryAst {
                goals: vec![atom(predicate(3, "reachable_count", 2), &["x", "count"])],
                keep: vec![Variable::new("x"), Variable::new("count")],
            },
        )
        .expect("query reachable count");
        assert_eq!(
            reachable_count.rows,
            vec![
                QueryRow {
                    values: vec![Value::Entity(EntityId::new(1)), Value::U64(3)],
                    tuple_id: reachable_count.rows[0].tuple_id,
                },
                QueryRow {
                    values: vec![Value::Entity(EntityId::new(2)), Value::U64(2)],
                    tuple_id: reachable_count.rows[1].tuple_id,
                },
                QueryRow {
                    values: vec![Value::Entity(EntityId::new(3)), Value::U64(1)],
                    tuple_id: reachable_count.rows[2].tuple_id,
                },
            ]
        );

        let project_hours = execute_query(
            &Default::default(),
            &compiled,
            &derived,
            &QueryAst {
                goals: vec![atom(
                    predicate(6, "project_hours", 2),
                    &["project", "hours"],
                )],
                keep: vec![Variable::new("project"), Variable::new("hours")],
            },
        )
        .expect("query project hours");
        assert_eq!(
            project_hours.rows[0].values,
            vec![Value::Entity(EntityId::new(10)), Value::U64(8)]
        );

        let latest_epoch = execute_query(
            &Default::default(),
            &compiled,
            &derived,
            &QueryAst {
                goals: vec![atom(predicate(8, "latest_epoch", 2), &["task", "epoch"])],
                keep: vec![Variable::new("task"), Variable::new("epoch")],
            },
        )
        .expect("query latest epoch");
        assert_eq!(
            latest_epoch.rows[0].values,
            vec![Value::Entity(EntityId::new(1)), Value::U64(4)]
        );
    }

    #[test]
    fn stratified_negation_supports_readiness_and_stale_rejection() {
        let mut schema = Schema::new("v1");
        for attribute in [
            AttributeSchema {
                id: AttributeId::new(1),
                name: "task.depends_on".into(),
                class: AttributeClass::RefSet,
                value_type: ValueType::Entity,
            },
            AttributeSchema {
                id: AttributeId::new(2),
                name: "task.status".into(),
                class: AttributeClass::ScalarLww,
                value_type: ValueType::String,
            },
            AttributeSchema {
                id: AttributeId::new(3),
                name: "task.claimed_by".into(),
                class: AttributeClass::ScalarLww,
                value_type: ValueType::String,
            },
            AttributeSchema {
                id: AttributeId::new(4),
                name: "task.lease_epoch".into(),
                class: AttributeClass::ScalarLww,
                value_type: ValueType::U64,
            },
            AttributeSchema {
                id: AttributeId::new(5),
                name: "task.lease_state".into(),
                class: AttributeClass::ScalarLww,
                value_type: ValueType::String,
            },
        ] {
            schema
                .register_attribute(attribute)
                .expect("register attribute");
        }

        for signature in [
            PredicateSignature {
                id: PredicateId::new(1),
                name: "task".into(),
                fields: vec![ValueType::Entity],
            },
            PredicateSignature {
                id: PredicateId::new(2),
                name: "execution_attempt".into(),
                fields: vec![ValueType::Entity, ValueType::String, ValueType::U64],
            },
            PredicateSignature {
                id: PredicateId::new(3),
                name: "task_depends_on".into(),
                fields: vec![ValueType::Entity, ValueType::Entity],
            },
            PredicateSignature {
                id: PredicateId::new(4),
                name: "task_status".into(),
                fields: vec![ValueType::Entity, ValueType::String],
            },
            PredicateSignature {
                id: PredicateId::new(5),
                name: "task_claimed_by".into(),
                fields: vec![ValueType::Entity, ValueType::String],
            },
            PredicateSignature {
                id: PredicateId::new(6),
                name: "task_lease_epoch".into(),
                fields: vec![ValueType::Entity, ValueType::U64],
            },
            PredicateSignature {
                id: PredicateId::new(7),
                name: "task_lease_state".into(),
                fields: vec![ValueType::Entity, ValueType::String],
            },
            PredicateSignature {
                id: PredicateId::new(8),
                name: "task_complete".into(),
                fields: vec![ValueType::Entity],
            },
            PredicateSignature {
                id: PredicateId::new(9),
                name: "dependency_blocked".into(),
                fields: vec![ValueType::Entity],
            },
            PredicateSignature {
                id: PredicateId::new(10),
                name: "lease_active".into(),
                fields: vec![ValueType::Entity, ValueType::String, ValueType::U64],
            },
            PredicateSignature {
                id: PredicateId::new(11),
                name: "active_claim".into(),
                fields: vec![ValueType::Entity],
            },
            PredicateSignature {
                id: PredicateId::new(12),
                name: "task_ready".into(),
                fields: vec![ValueType::Entity],
            },
            PredicateSignature {
                id: PredicateId::new(13),
                name: "execution_rejected_stale".into(),
                fields: vec![ValueType::Entity, ValueType::String, ValueType::U64],
            },
        ] {
            schema
                .register_predicate(signature)
                .expect("register predicate");
        }

        let program = RuleProgram {
            predicates: vec![
                predicate(1, "task", 1),
                predicate(2, "execution_attempt", 3),
                predicate(3, "task_depends_on", 2),
                predicate(4, "task_status", 2),
                predicate(5, "task_claimed_by", 2),
                predicate(6, "task_lease_epoch", 2),
                predicate(7, "task_lease_state", 2),
                predicate(8, "task_complete", 1),
                predicate(9, "dependency_blocked", 1),
                predicate(10, "lease_active", 3),
                predicate(11, "active_claim", 1),
                predicate(12, "task_ready", 1),
                predicate(13, "execution_rejected_stale", 3),
            ],
            rules: vec![
                RuleAst {
                    id: RuleId::new(1),
                    head: atom(predicate(8, "task_complete", 1), &["t"]),
                    body: vec![Literal::Positive(Atom {
                        predicate: predicate(4, "task_status", 2),
                        terms: vec![
                            Term::Variable(Variable::new("t")),
                            Term::Value(Value::String("done".into())),
                        ],
                    })],
                },
                RuleAst {
                    id: RuleId::new(2),
                    head: atom(predicate(9, "dependency_blocked", 1), &["t"]),
                    body: vec![
                        Literal::Positive(atom(predicate(3, "task_depends_on", 2), &["t", "dep"])),
                        Literal::Negative(atom(predicate(8, "task_complete", 1), &["dep"])),
                    ],
                },
                RuleAst {
                    id: RuleId::new(3),
                    head: atom(predicate(10, "lease_active", 3), &["t", "worker", "epoch"]),
                    body: vec![
                        Literal::Positive(atom(
                            predicate(5, "task_claimed_by", 2),
                            &["t", "worker"],
                        )),
                        Literal::Positive(atom(
                            predicate(6, "task_lease_epoch", 2),
                            &["t", "epoch"],
                        )),
                        Literal::Positive(Atom {
                            predicate: predicate(7, "task_lease_state", 2),
                            terms: vec![
                                Term::Variable(Variable::new("t")),
                                Term::Value(Value::String("active".into())),
                            ],
                        }),
                    ],
                },
                RuleAst {
                    id: RuleId::new(4),
                    head: atom(predicate(11, "active_claim", 1), &["t"]),
                    body: vec![Literal::Positive(atom(
                        predicate(10, "lease_active", 3),
                        &["t", "worker", "epoch"],
                    ))],
                },
                RuleAst {
                    id: RuleId::new(5),
                    head: atom(predicate(12, "task_ready", 1), &["t"]),
                    body: vec![
                        Literal::Positive(atom(predicate(1, "task", 1), &["t"])),
                        Literal::Negative(atom(predicate(9, "dependency_blocked", 1), &["t"])),
                        Literal::Negative(atom(predicate(11, "active_claim", 1), &["t"])),
                    ],
                },
                RuleAst {
                    id: RuleId::new(6),
                    head: atom(
                        predicate(13, "execution_rejected_stale", 3),
                        &["t", "worker", "epoch"],
                    ),
                    body: vec![
                        Literal::Positive(atom(
                            predicate(2, "execution_attempt", 3),
                            &["t", "worker", "epoch"],
                        )),
                        Literal::Negative(atom(
                            predicate(10, "lease_active", 3),
                            &["t", "worker", "epoch"],
                        )),
                    ],
                },
            ],
            materialized: vec![PredicateId::new(12), PredicateId::new(13)],
            facts: vec![
                ExtensionalFact {
                    predicate: predicate(1, "task", 1),
                    values: vec![Value::Entity(EntityId::new(1))],
                    policy: None,
                    provenance: None,
                },
                ExtensionalFact {
                    predicate: predicate(1, "task", 1),
                    values: vec![Value::Entity(EntityId::new(2))],
                    policy: None,
                    provenance: None,
                },
                ExtensionalFact {
                    predicate: predicate(2, "execution_attempt", 3),
                    values: vec![
                        Value::Entity(EntityId::new(1)),
                        Value::String("worker-a".into()),
                        Value::U64(1),
                    ],
                    policy: None,
                    provenance: None,
                },
            ],
        };
        let datoms = vec![
            dependency_datom(1, 2, 1),
            datom(2, 2, Value::String("done".into()), 2),
            datom(1, 3, Value::String("worker-a".into()), 3),
            datom(1, 4, Value::U64(1), 4),
            datom(1, 5, Value::String("active".into()), 5),
            datom(1, 5, Value::String("expired".into()), 6),
        ];

        let compiled = DefaultRuleCompiler
            .compile(&schema, &program)
            .expect("compile coordination program");
        let as_of_state = MaterializedResolver
            .as_of(&schema, &datoms, &ElementId::new(5))
            .expect("resolve as_of");
        let current_state = MaterializedResolver
            .current(&schema, &datoms)
            .expect("resolve current");
        let as_of_derived = SemiNaiveRuntime
            .evaluate(&as_of_state, &compiled)
            .expect("evaluate as_of");
        let current_derived = SemiNaiveRuntime
            .evaluate(&current_state, &compiled)
            .expect("evaluate current");

        let as_of_ready = execute_query(
            &as_of_state,
            &compiled,
            &as_of_derived,
            &QueryAst {
                goals: vec![
                    atom(predicate(12, "task_ready", 1), &["t"]),
                    Atom {
                        predicate: predicate(5, "task_claimed_by", 2),
                        terms: vec![
                            Term::Variable(Variable::new("t")),
                            Term::Value(Value::String("worker-a".into())),
                        ],
                    },
                ],
                keep: vec![Variable::new("t")],
            },
        )
        .expect("query as_of ready");
        assert!(as_of_ready.rows.is_empty());

        let current_ready = execute_query(
            &current_state,
            &compiled,
            &current_derived,
            &QueryAst {
                goals: vec![
                    atom(predicate(12, "task_ready", 1), &["t"]),
                    Atom {
                        predicate: predicate(5, "task_claimed_by", 2),
                        terms: vec![
                            Term::Variable(Variable::new("t")),
                            Term::Value(Value::String("worker-a".into())),
                        ],
                    },
                ],
                keep: vec![Variable::new("t")],
            },
        )
        .expect("query current ready");
        assert_eq!(current_ready.rows.len(), 1);
        assert_eq!(
            current_ready.rows[0].values,
            vec![Value::Entity(EntityId::new(1))]
        );

        let stale_attempts = execute_query(
            &current_state,
            &compiled,
            &current_derived,
            &QueryAst {
                goals: vec![atom(
                    predicate(13, "execution_rejected_stale", 3),
                    &["t", "worker", "epoch"],
                )],
                keep: vec![
                    Variable::new("t"),
                    Variable::new("worker"),
                    Variable::new("epoch"),
                ],
            },
        )
        .expect("query stale attempts");
        assert_eq!(
            stale_attempts.rows,
            vec![QueryRow {
                values: vec![
                    Value::Entity(EntityId::new(1)),
                    Value::String("worker-a".into()),
                    Value::U64(1),
                ],
                tuple_id: stale_attempts.rows.first().and_then(|row| row.tuple_id),
            }]
        );
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
            facts: Vec::new(),
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

    fn datom(entity: u64, attribute: u64, value: Value, element: u64) -> Datom {
        Datom {
            entity: EntityId::new(entity),
            attribute: AttributeId::new(attribute),
            value,
            op: aether_ast::OperationKind::Assert,
            element: ElementId::new(element),
            replica: aether_ast::ReplicaId::new(1),
            causal_context: Default::default(),
            provenance: DatomProvenance::default(),
            policy: None,
        }
    }
}
