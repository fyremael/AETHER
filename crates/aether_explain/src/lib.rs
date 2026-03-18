use aether_ast::{DerivationTrace, PhaseGraph, PlanExplanation, TupleId};
use aether_runtime::DerivedSet;
use indexmap::IndexMap;
use thiserror::Error;

pub trait Explainer {
    fn explain_tuple(&self, id: &TupleId) -> Result<DerivationTrace, ExplainError>;
    fn explain_plan(&self, plan: &PhaseGraph) -> Result<PlanExplanation, ExplainError>;
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryExplainer {
    traces: IndexMap<TupleId, DerivationTrace>,
}

impl InMemoryExplainer {
    pub fn from_derived_set(derived: &DerivedSet) -> Self {
        let traces = derived
            .tuples
            .iter()
            .map(|tuple| {
                (
                    tuple.tuple.id,
                    DerivationTrace {
                        root: tuple.tuple.id,
                        tuples: vec![tuple.clone()],
                    },
                )
            })
            .collect();
        Self { traces }
    }
}

impl Explainer for InMemoryExplainer {
    fn explain_tuple(&self, id: &TupleId) -> Result<DerivationTrace, ExplainError> {
        self.traces
            .get(id)
            .cloned()
            .ok_or(ExplainError::UnknownTuple(*id))
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
}
