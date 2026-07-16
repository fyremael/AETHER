use aether_ast::{
    AggregateFunction, AttributeId, ExtensionalFact, PhaseGraph, PolicyScope, PredicateId, RuleAst,
    RuleId, Variable,
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

pub const EXECUTABLE_PLAN_FORMAT_VERSION: &str = "aether-executable-plan-v1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaAnchorStrategy {
    SeedOnce,
    PositiveBodyIndices(Vec<usize>),
    AggregateFullInputOnce,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AggregatePlanNode {
    pub output_index: usize,
    pub function: AggregateFunction,
    pub input_variable: Variable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvenanceRequirement {
    CompleteParentsSourcesImportsAndPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuleExecutionPlan {
    pub rule_id: RuleId,
    pub scc_id: usize,
    pub stratum: usize,
    pub delta_anchor: DeltaAnchorStrategy,
    pub aggregates: Vec<AggregatePlanNode>,
    pub provenance: ProvenanceRequirement,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SccExecutionPlan {
    pub scc_id: usize,
    pub stratum: usize,
    pub rule_ids: Vec<RuleId>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutableSchedule {
    pub scc_order: Vec<usize>,
    pub sccs: Vec<SccExecutionPlan>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CompiledProgram {
    pub plan_format_version: String,
    pub dependency_graph: DependencyGraph,
    pub sccs: Vec<StronglyConnectedComponent>,
    pub phase_graph: PhaseGraph,
    pub delta_plans: Vec<DeltaRulePlan>,
    pub materialized: Vec<PredicateId>,
    pub rules: Vec<RuleAst>,
    pub extensional_bindings: IndexMap<PredicateId, AttributeId>,
    pub facts: Vec<ExtensionalFact>,
    pub predicate_strata: IndexMap<PredicateId, usize>,
    pub schedule: ExecutableSchedule,
    pub rule_plans: IndexMap<RuleId, RuleExecutionPlan>,
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
    empty_extensional_predicates: Vec<PredicateId>,
}

impl ScopedProgram {
    /// Constructs a scoped plan from compiler output and defensively enforces
    /// the scoped-fact invariant again at the type boundary.
    #[doc(hidden)]
    pub fn from_scoped_compilation(
        mut program: CompiledProgram,
        scope: PolicyScope,
        empty_extensional_predicates: Vec<PredicateId>,
    ) -> Self {
        program
            .facts
            .retain(|fact| scope.allows(fact.policy.as_ref()));
        Self {
            program,
            scope,
            empty_extensional_predicates,
        }
    }

    pub fn compiled(&self) -> &CompiledProgram {
        &self.program
    }

    pub fn scope(&self) -> &PolicyScope {
        &self.scope
    }

    pub fn empty_extensional_predicates(&self) -> &[PredicateId] {
        &self.empty_extensional_predicates
    }
}
