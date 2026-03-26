use aether_ast::{DerivationTrace, PhaseGraph, PlanExplanation, TupleId};
use aether_runtime::DerivedSet;
use indexmap::{IndexMap, IndexSet};
use thiserror::Error;

pub trait Explainer {
    fn explain_tuple(&self, id: &TupleId) -> Result<DerivationTrace, ExplainError>;
    fn explain_plan(&self, plan: &PhaseGraph) -> Result<PlanExplanation, ExplainError>;
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryExplainer {
    tuples: IndexMap<TupleId, aether_ast::DerivedTuple>,
}

impl InMemoryExplainer {
    pub fn from_derived_set(derived: &DerivedSet) -> Self {
        let tuples = derived
            .tuples
            .iter()
            .map(|tuple| (tuple.tuple.id, tuple.clone()))
            .collect();
        Self { tuples }
    }

    fn collect_trace(
        &self,
        tuple_id: TupleId,
        visited: &mut IndexSet<TupleId>,
        tuples: &mut Vec<aether_ast::DerivedTuple>,
    ) -> Result<(), ExplainError> {
        if !visited.insert(tuple_id) {
            return Ok(());
        }

        let tuple = self
            .tuples
            .get(&tuple_id)
            .cloned()
            .ok_or(ExplainError::UnknownTuple(tuple_id))?;

        tuples.push(tuple.clone());
        for parent in &tuple.metadata.parent_tuple_ids {
            if !self.tuples.contains_key(parent) {
                return Err(ExplainError::DanglingParentTuple {
                    tuple: tuple_id,
                    parent: *parent,
                });
            }
            self.collect_trace(*parent, visited, tuples)?;
        }

        Ok(())
    }
}

impl Explainer for InMemoryExplainer {
    fn explain_tuple(&self, id: &TupleId) -> Result<DerivationTrace, ExplainError> {
        let mut visited = IndexSet::new();
        let mut tuples = Vec::new();
        self.collect_trace(*id, &mut visited, &mut tuples)?;

        Ok(DerivationTrace { root: *id, tuples })
    }

    fn explain_plan(&self, plan: &PhaseGraph) -> Result<PlanExplanation, ExplainError> {
        Ok(PlanExplanation {
            summary: format!(
                "Phase graph with {} node(s) and {} edge(s)",
                plan.nodes.len(),
                plan.edges.len()
            ),
            phase_graph: plan.clone(),
        })
    }
}

#[derive(Debug, Error)]
pub enum ExplainError {
    #[error("unknown tuple {0}")]
    UnknownTuple(TupleId),
    #[error("tuple {tuple} references missing parent tuple {parent}")]
    DanglingParentTuple { tuple: TupleId, parent: TupleId },
}

#[cfg(test)]
mod tests {
    use super::{ExplainError, Explainer, InMemoryExplainer};
    use aether_ast::{
        DerivedTuple, DerivedTupleMetadata, PredicateId, RuleId, Tuple, TupleId, Value,
    };
    use aether_runtime::{DerivedSet, RuntimeIteration};

    fn tuple(
        id: u64,
        values: &[u64],
        parent_tuple_ids: &[u64],
        source_datom_ids: &[u64],
        iteration: usize,
    ) -> DerivedTuple {
        DerivedTuple {
            tuple: Tuple {
                id: TupleId::new(id),
                predicate: PredicateId::new(1),
                values: values.iter().copied().map(Value::U64).collect(),
            },
            metadata: DerivedTupleMetadata {
                rule_id: RuleId::new(1),
                predicate_id: PredicateId::new(1),
                stratum: 0,
                scc_id: 0,
                iteration,
                parent_tuple_ids: parent_tuple_ids.iter().copied().map(TupleId::new).collect(),
                source_datom_ids: source_datom_ids
                    .iter()
                    .copied()
                    .map(aether_ast::ElementId::new)
                    .collect(),
                imported_cuts: Vec::new(),
            },
            policy: None,
        }
    }

    #[test]
    fn explain_tuple_returns_recursive_trace() {
        let derived = DerivedSet {
            tuples: vec![
                tuple(1, &[1, 2], &[], &[11], 1),
                tuple(2, &[2, 3], &[], &[12], 1),
                tuple(3, &[1, 3], &[1, 2], &[11, 12], 2),
            ],
            iterations: vec![
                RuntimeIteration {
                    iteration: 1,
                    delta_size: 2,
                },
                RuntimeIteration {
                    iteration: 2,
                    delta_size: 1,
                },
                RuntimeIteration {
                    iteration: 3,
                    delta_size: 0,
                },
            ],
            predicate_index: Default::default(),
        };
        let explainer = InMemoryExplainer::from_derived_set(&derived);

        let trace = explainer
            .explain_tuple(&TupleId::new(3))
            .expect("explain recursive tuple");

        assert_eq!(trace.root, TupleId::new(3));
        assert_eq!(
            trace
                .tuples
                .iter()
                .map(|tuple| tuple.tuple.id)
                .collect::<Vec<_>>(),
            vec![TupleId::new(3), TupleId::new(1), TupleId::new(2)]
        );
        assert_eq!(trace.tuples[0].metadata.source_datom_ids.len(), 2);
    }

    #[test]
    fn explain_tuple_reports_unknown_roots() {
        let explainer = InMemoryExplainer::default();
        let error = explainer
            .explain_tuple(&TupleId::new(99))
            .expect_err("unknown tuple should fail");

        assert!(matches!(error, ExplainError::UnknownTuple(id) if id == TupleId::new(99)));
    }

    #[test]
    fn explain_tuple_reports_dangling_parent_references() {
        let derived = DerivedSet {
            tuples: vec![tuple(7, &[1, 4], &[8], &[21], 2)],
            iterations: Vec::new(),
            predicate_index: Default::default(),
        };
        let explainer = InMemoryExplainer::from_derived_set(&derived);

        let error = explainer
            .explain_tuple(&TupleId::new(7))
            .expect_err("dangling parent should fail");

        assert!(matches!(
            error,
            ExplainError::DanglingParentTuple { tuple, parent }
                if tuple == TupleId::new(7) && parent == TupleId::new(8)
        ));
    }
}
