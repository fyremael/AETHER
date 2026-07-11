use aether_ast::{
    AttributeId, ExtensionalFact, PhaseGraph, PolicyScope, PredicateId, RuleAst, RuleId,
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyGraph {
    pub edges: IndexMap<PredicateId, Vec<PredicateId>>,
}

impl DependencyGraph {
    pub fn add_edge(&mut self, head: PredicateId, dependency: PredicateId) {
        self.edges.entry(head).or_default().push(dependency);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StronglyConnectedComponent {
    pub id: usize,
    pub predicates: Vec<PredicateId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaRulePlan {
    pub rule_id: RuleId,
    pub target_predicate: PredicateId,
    pub source_predicates: Vec<PredicateId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhasePlan {
    pub phase_graph: PhaseGraph,
    pub sccs: Vec<StronglyConnectedComponent>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CompiledProgram {
    pub dependency_graph: DependencyGraph,
    pub sccs: Vec<StronglyConnectedComponent>,
    pub phase_graph: PhaseGraph,
    pub delta_plans: Vec<DeltaRulePlan>,
    pub materialized: Vec<PredicateId>,
    pub rules: Vec<RuleAst>,
    pub extensional_bindings: IndexMap<PredicateId, AttributeId>,
    pub facts: Vec<ExtensionalFact>,
    pub predicate_strata: IndexMap<PredicateId, usize>,
}

/// A compiled program whose extensional facts have been projected to one
/// canonical policy scope before compilation.
///
/// The private fields prevent callers from relabeling a compiled program by
/// constructing this security-bearing wrapper directly.
#[derive(Clone, Debug, PartialEq)]
pub struct ScopedProgram {
    program: CompiledProgram,
    scope: PolicyScope,
}

impl ScopedProgram {
    /// Constructs a scoped plan from compiler output and defensively enforces
    /// the scoped-fact invariant again at the type boundary.
    #[doc(hidden)]
    pub fn from_scoped_compilation(mut program: CompiledProgram, scope: PolicyScope) -> Self {
        program
            .facts
            .retain(|fact| scope.allows(fact.policy.as_ref()));
        Self { program, scope }
    }

    pub fn compiled(&self) -> &CompiledProgram {
        &self.program
    }

    pub fn scope(&self) -> &PolicyScope {
        &self.scope
    }
}
