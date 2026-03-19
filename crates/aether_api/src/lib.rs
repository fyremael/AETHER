use aether_ast::{
    Datom, DerivationTrace, ElementId, PhaseGraph, PlanExplanation, QueryResult, QuerySpec,
    RuleProgram, TupleId,
};
use aether_explain::{ExplainError, Explainer, InMemoryExplainer};
use aether_plan::CompiledProgram;
use aether_resolver::{MaterializedResolver, ResolveError, ResolvedState, Resolver};
use aether_rules::{DefaultDslParser, DefaultRuleCompiler, DslParser, ParseError, RuleCompiler};
use aether_runtime::{execute_query, DerivedSet, RuleRuntime, RuntimeError, SemiNaiveRuntime};
use aether_schema::Schema;
use aether_storage::{InMemoryJournal, Journal, JournalError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub trait KernelService {
    fn append(&mut self, request: AppendRequest) -> Result<AppendResponse, ApiError>;
    fn history(&self, request: HistoryRequest) -> Result<HistoryResponse, ApiError>;
    fn current_state(&self, request: CurrentStateRequest)
        -> Result<CurrentStateResponse, ApiError>;
    fn as_of(&self, request: AsOfRequest) -> Result<AsOfResponse, ApiError>;
    fn compile_program(
        &self,
        request: CompileProgramRequest,
    ) -> Result<CompileProgramResponse, ApiError>;
    fn evaluate_program(
        &mut self,
        request: EvaluateProgramRequest,
    ) -> Result<EvaluateProgramResponse, ApiError>;
    fn explain_tuple(&self, request: ExplainTupleRequest)
        -> Result<ExplainTupleResponse, ApiError>;
    fn explain_plan(&self, request: ExplainPlanRequest) -> Result<ExplainPlanResponse, ApiError>;
    fn parse_document(
        &self,
        request: ParseDocumentRequest,
    ) -> Result<ParseDocumentResponse, ApiError>;
    fn run_document(
        &mut self,
        request: RunDocumentRequest,
    ) -> Result<RunDocumentResponse, ApiError>;
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryKernelService {
    journal: InMemoryJournal,
    last_derived: Option<DerivedSet>,
}

impl InMemoryKernelService {
    pub fn new() -> Self {
        Self::default()
    }

    fn datoms_or_history(&self, datoms: &[Datom]) -> Result<Vec<Datom>, ApiError> {
        if datoms.is_empty() {
            Ok(self.journal.history()?)
        } else {
            Ok(datoms.to_vec())
        }
    }

    fn cache_derived(&mut self, derived: DerivedSet) -> DerivedSet {
        self.last_derived = Some(derived.clone());
        derived
    }
}

impl KernelService for InMemoryKernelService {
    fn append(&mut self, request: AppendRequest) -> Result<AppendResponse, ApiError> {
        self.journal.append(&request.datoms)?;
        Ok(AppendResponse {
            appended: request.datoms.len(),
        })
    }

    fn history(&self, _request: HistoryRequest) -> Result<HistoryResponse, ApiError> {
        Ok(HistoryResponse {
            datoms: self.journal.history()?,
        })
    }

    fn current_state(
        &self,
        request: CurrentStateRequest,
    ) -> Result<CurrentStateResponse, ApiError> {
        Ok(CurrentStateResponse {
            state: MaterializedResolver
                .current(&request.schema, &self.datoms_or_history(&request.datoms)?)?,
        })
    }

    fn as_of(&self, request: AsOfRequest) -> Result<AsOfResponse, ApiError> {
        Ok(AsOfResponse {
            state: MaterializedResolver.as_of(
                &request.schema,
                &self.datoms_or_history(&request.datoms)?,
                &request.at,
            )?,
        })
    }

    fn compile_program(
        &self,
        request: CompileProgramRequest,
    ) -> Result<CompileProgramResponse, ApiError> {
        Ok(CompileProgramResponse {
            program: DefaultRuleCompiler.compile(&request.schema, &request.program)?,
        })
    }

    fn evaluate_program(
        &mut self,
        request: EvaluateProgramRequest,
    ) -> Result<EvaluateProgramResponse, ApiError> {
        let derived = SemiNaiveRuntime.evaluate(&request.state, &request.program)?;
        Ok(EvaluateProgramResponse {
            derived: self.cache_derived(derived),
        })
    }

    fn explain_tuple(
        &self,
        request: ExplainTupleRequest,
    ) -> Result<ExplainTupleResponse, ApiError> {
        let derived = self
            .last_derived
            .as_ref()
            .ok_or_else(|| ApiError::Validation("no derived tuples are cached".into()))?;
        let trace =
            InMemoryExplainer::from_derived_set(derived).explain_tuple(&request.tuple_id)?;
        Ok(ExplainTupleResponse { trace })
    }

    fn explain_plan(&self, request: ExplainPlanRequest) -> Result<ExplainPlanResponse, ApiError> {
        let explanation = InMemoryExplainer::default().explain_plan(&request.plan)?;
        Ok(ExplainPlanResponse { explanation })
    }

    fn parse_document(
        &self,
        request: ParseDocumentRequest,
    ) -> Result<ParseDocumentResponse, ApiError> {
        let document = DefaultDslParser.parse_document(&request.dsl)?;
        Ok(ParseDocumentResponse {
            schema: document.schema,
            program: document.program,
            query: document.query,
        })
    }

    fn run_document(
        &mut self,
        request: RunDocumentRequest,
    ) -> Result<RunDocumentResponse, ApiError> {
        let document = DefaultDslParser.parse_document(&request.dsl)?;
        let datoms = self.journal.history()?;
        let state = match document.query.as_ref().map(|query| &query.view) {
            Some(aether_ast::TemporalView::AsOf(element)) => {
                MaterializedResolver.as_of(&document.schema, &datoms, element)?
            }
            _ => MaterializedResolver.current(&document.schema, &datoms)?,
        };
        let program = DefaultRuleCompiler.compile(&document.schema, &document.program)?;
        let derived = SemiNaiveRuntime.evaluate(&state, &program)?;
        let query = match &document.query {
            Some(query) => Some(execute_query(&state, &program, &derived, &query.query)?),
            None => None,
        };
        let derived = self.cache_derived(derived);

        Ok(RunDocumentResponse {
            state,
            program,
            derived,
            query,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AppendRequest {
    pub datoms: Vec<Datom>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendResponse {
    pub appended: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HistoryRequest;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HistoryResponse {
    pub datoms: Vec<Datom>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CurrentStateRequest {
    pub schema: Schema,
    pub datoms: Vec<Datom>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CurrentStateResponse {
    pub state: ResolvedState,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AsOfRequest {
    pub schema: Schema,
    pub datoms: Vec<Datom>,
    pub at: ElementId,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AsOfResponse {
    pub state: ResolvedState,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CompileProgramRequest {
    pub schema: Schema,
    pub program: RuleProgram,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CompileProgramResponse {
    pub program: CompiledProgram,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EvaluateProgramRequest {
    pub state: ResolvedState,
    pub program: CompiledProgram,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EvaluateProgramResponse {
    pub derived: DerivedSet,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExplainTupleRequest {
    pub tuple_id: TupleId,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ExplainTupleResponse {
    pub trace: DerivationTrace,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ExplainPlanRequest {
    pub plan: PhaseGraph,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ExplainPlanResponse {
    pub explanation: PlanExplanation,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ParseDocumentRequest {
    pub dsl: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ParseDocumentResponse {
    pub schema: Schema,
    pub program: RuleProgram,
    pub query: Option<QuerySpec>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RunDocumentRequest {
    pub dsl: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RunDocumentResponse {
    pub state: ResolvedState,
    pub program: CompiledProgram,
    pub derived: DerivedSet,
    pub query: Option<QueryResult>,
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("validation error: {0}")]
    Validation(String),
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Resolve(#[from] ResolveError),
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error(transparent)]
    Compile(#[from] aether_rules::CompileError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Explain(#[from] ExplainError),
}

#[cfg(test)]
mod tests {
    use super::{
        AppendRequest, ExplainTupleRequest, InMemoryKernelService, KernelService,
        ParseDocumentRequest, RunDocumentRequest,
    };
    use aether_ast::{AttributeId, Datom, DatomProvenance, ElementId, EntityId, Value};

    #[test]
    fn service_runs_coordination_document_end_to_end() {
        let mut service = InMemoryKernelService::new();
        service
            .append(AppendRequest {
                datoms: vec![
                    dependency_datom(1, 2, 1),
                    datom(2, 2, Value::String("done".into()), 2),
                    datom(1, 3, Value::String("worker-a".into()), 3),
                    datom(1, 4, Value::U64(1), 4),
                    datom(1, 5, Value::String("active".into()), 5),
                    datom(1, 5, Value::String("expired".into()), 6),
                ],
            })
            .expect("append journal");

        let parsed = service
            .parse_document(ParseDocumentRequest {
                dsl: readiness_dsl(
                    "as_of e5",
                    "goal task_ready(t)\n  goal task_claimed_by(t, \"worker-a\")\n  keep t",
                ),
            })
            .expect("parse readiness document");
        assert_eq!(parsed.program.facts.len(), 3);
        assert_eq!(
            parsed.program.facts[2].policy,
            Some(aether_ast::PolicyEnvelope {
                capability: Some("executor".into()),
                visibility: Some("ops".into()),
            })
        );

        let as_of = service
            .run_document(RunDocumentRequest {
                dsl: readiness_dsl(
                    "as_of e5",
                    "goal task_ready(t)\n  goal task_claimed_by(t, \"worker-a\")\n  keep t",
                ),
            })
            .expect("run as_of readiness document");
        assert_eq!(as_of.state.as_of, Some(ElementId::new(5)));
        assert!(as_of
            .query
            .as_ref()
            .expect("query result should exist")
            .rows
            .is_empty());

        let current = service
            .run_document(RunDocumentRequest {
                dsl: readiness_dsl(
                    "current",
                    "goal task_ready(t)\n  goal task_claimed_by(t, \"worker-a\")\n  keep t",
                ),
            })
            .expect("run current readiness document");
        let ready_rows = &current
            .query
            .as_ref()
            .expect("query result should exist")
            .rows;
        assert_eq!(ready_rows.len(), 1);
        assert_eq!(ready_rows[0].values, vec![Value::Entity(EntityId::new(1))]);
        let trace = service
            .explain_tuple(ExplainTupleRequest {
                tuple_id: ready_rows[0].tuple_id.expect("task_ready tuple id"),
            })
            .expect("explain readiness tuple")
            .trace;
        assert!(!trace.tuples.is_empty());

        let stale = service
            .run_document(RunDocumentRequest {
                dsl: readiness_dsl(
                    "current",
                    "goal execution_rejected_stale(t, worker, epoch)\n  keep t, worker, epoch",
                ),
            })
            .expect("run stale-rejection document");
        let stale_rows = &stale
            .query
            .as_ref()
            .expect("query result should exist")
            .rows;
        assert_eq!(stale_rows.len(), 1);
        assert_eq!(
            stale_rows[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-a".into()),
                Value::U64(1),
            ]
        );
    }

    fn readiness_dsl(view: &str, query_body: &str) -> String {
        format!(
            r#"
schema v1 {{
  attr task.depends_on: RefSet<Entity>
  attr task.status: ScalarLWW<String>
  attr task.claimed_by: ScalarLWW<String>
  attr task.lease_epoch: ScalarLWW<U64>
  attr task.lease_state: ScalarLWW<String>
}}

predicates {{
  task(Entity)
  execution_attempt(Entity, String, U64)
  task_depends_on(Entity, Entity)
  task_status(Entity, String)
  task_claimed_by(Entity, String)
  task_lease_epoch(Entity, U64)
  task_lease_state(Entity, String)
  task_complete(Entity)
  dependency_blocked(Entity)
  lease_active(Entity, String, U64)
  active_claim(Entity)
  task_ready(Entity)
  execution_rejected_stale(Entity, String, U64)
}}

facts {{
  task(entity(1))
  task(entity(2))
  execution_attempt(entity(1), "worker-a", 1) @capability("executor") @visibility("ops")
}}

rules {{
  task_complete(t) <- task_status(t, "done")
  dependency_blocked(t) <- task_depends_on(t, dep), not task_complete(dep)
  lease_active(t, worker, epoch) <- task_claimed_by(t, worker), task_lease_epoch(t, epoch), task_lease_state(t, "active")
  active_claim(t) <- lease_active(t, worker, epoch)
  task_ready(t) <- task(t), not dependency_blocked(t), not active_claim(t)
  execution_rejected_stale(t, worker, epoch) <- execution_attempt(t, worker, epoch), not lease_active(t, worker, epoch)
}}

materialize {{
  task_ready
  execution_rejected_stale
}}

query {{
  {view}
  {query_body}
}}
"#
        )
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
