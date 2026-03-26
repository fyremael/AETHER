use aether_ast::{
    policy_allows, Datom, DerivationTrace, ElementId, ExplainSpec, ExplainTarget, ExtensionalFact,
    NamedExplainSpec, NamedQuerySpec, PhaseGraph, PlanExplanation, PolicyContext, QueryResult,
    QuerySpec, RuleProgram, TemporalView, Term, TupleId,
};
use aether_explain::{ExplainError, Explainer, InMemoryExplainer};
use aether_plan::CompiledProgram;
use aether_resolver::{MaterializedResolver, ResolveError, ResolvedState, ResolvedValue, Resolver};
use aether_rules::{DefaultDslParser, DefaultRuleCompiler, DslParser, ParseError, RuleCompiler};
use aether_runtime::{execute_query, DerivedSet, RuleRuntime, RuntimeError, SemiNaiveRuntime};
use aether_schema::Schema;
use aether_storage::{InMemoryJournal, Journal, JournalError, SqliteJournal};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

pub mod deployment;
pub mod http;
pub mod partitioned;
#[doc(hidden)]
pub mod perf;
pub mod pilot;
pub mod report;
pub mod sidecar;

pub use deployment::{
    default_audit_log_path, serve_pilot_http_service, DeploymentError, PilotAuthConfig,
    PilotServiceConfig, PilotTokenConfig, ResolvedPilotServiceConfig, ResolvedPilotTokenSummary,
};
pub use http::{
    http_router, http_router_with_options, AuditContext, AuditEntry, AuditLogResponse, AuthScope,
    HealthResponse, HttpAccessToken, HttpAuthConfig, HttpKernelOptions, HttpKernelState,
};
pub use partitioned::{
    render_federated_explain_report_markdown, FederatedExplainReport, FederatedHistoryRequest,
    FederatedHistoryResponse, FederatedImportedSourceSummary, FederatedNamedQuerySummary,
    FederatedReportRow, FederatedRunDocumentRequest, FederatedRunDocumentResponse,
    FederatedTraceSummary, FederatedTraceTupleSummary, ImportedFactQueryRequest,
    ImportedFactQueryResponse, PartitionAppendRequest, PartitionAppendResponse,
    PartitionHistoryRequest, PartitionHistoryResponse, PartitionStateRequest,
    PartitionStateResponse, PartitionedInMemoryKernelService,
};
pub use pilot::{
    coordination_pilot_dsl, coordination_pilot_seed_history,
    COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT, COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT,
};
pub use report::{
    build_coordination_pilot_report, build_coordination_pilot_report_with_policy,
    render_coordination_pilot_report_markdown, CoordinationPilotReport, ReportRow, TraceSummary,
    TraceTupleSummary,
};
pub use sidecar::{
    ArtifactReference, GetArtifactReferenceRequest, GetArtifactReferenceResponse,
    InMemorySidecarFederation, JournalCatalog, RegisterArtifactReferenceRequest,
    RegisterArtifactReferenceResponse, RegisterVectorRecordRequest, RegisterVectorRecordResponse,
    SearchVectorsRequest, SearchVectorsResponse, SidecarError, SidecarFederation,
    SqliteSidecarFederation, VectorFactProjection, VectorMetric, VectorRecordMetadata,
    VectorSearchMatch,
};

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
    fn register_artifact_reference(
        &mut self,
        request: RegisterArtifactReferenceRequest,
    ) -> Result<RegisterArtifactReferenceResponse, ApiError>;
    fn get_artifact_reference(
        &self,
        request: GetArtifactReferenceRequest,
    ) -> Result<GetArtifactReferenceResponse, ApiError>;
    fn register_vector_record(
        &mut self,
        request: RegisterVectorRecordRequest,
    ) -> Result<RegisterVectorRecordResponse, ApiError>;
    fn search_vectors(
        &self,
        request: SearchVectorsRequest,
    ) -> Result<SearchVectorsResponse, ApiError>;
}

pub type InMemoryKernelService = KernelServiceCore<InMemoryJournal, InMemorySidecarFederation>;
pub type SqliteKernelService = KernelServiceCore<SqliteJournal, SqliteSidecarFederation>;

#[derive(Debug)]
pub struct KernelServiceCore<J: Journal, S: SidecarFederation = InMemorySidecarFederation> {
    journal: J,
    sidecars: S,
    last_derived: Option<CachedDerivedSet>,
}

impl KernelServiceCore<InMemoryJournal, InMemorySidecarFederation> {
    pub fn new() -> Self {
        Self::from_journal(InMemoryJournal::new())
    }
}

impl Default for KernelServiceCore<InMemoryJournal, InMemorySidecarFederation> {
    fn default() -> Self {
        Self::new()
    }
}

impl KernelServiceCore<SqliteJournal, SqliteSidecarFederation> {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ApiError> {
        let path = path.as_ref();
        Ok(Self::from_parts(
            SqliteJournal::open(path)?,
            SqliteSidecarFederation::open(sidecar::sidecar_catalog_path_for_journal(path))?,
        ))
    }
}

impl<J: Journal, S: SidecarFederation> KernelServiceCore<J, S> {
    pub fn from_parts(journal: J, sidecars: S) -> Self {
        Self {
            journal,
            sidecars,
            last_derived: None,
        }
    }

    fn datoms_or_history(&self, datoms: &[Datom]) -> Result<Vec<Datom>, ApiError> {
        if datoms.is_empty() {
            Ok(self.journal.history()?)
        } else {
            Ok(datoms.to_vec())
        }
    }

    fn visible_history(
        &self,
        datoms: &[Datom],
        policy_context: Option<&PolicyContext>,
    ) -> Result<Vec<Datom>, ApiError> {
        Ok(filter_datoms(
            self.datoms_or_history(datoms)?,
            policy_context,
        ))
    }

    fn cache_derived(&mut self, derived: DerivedSet) {
        self.last_derived = Some(CachedDerivedSet { derived });
    }

    fn sidecar_journal_catalog(&self) -> Result<JournalCatalog, ApiError> {
        Ok(JournalCatalog::from_history(&self.journal.history()?))
    }

    fn document_evaluation<'a>(
        &self,
        cache: &'a mut Vec<DocumentEvaluation>,
        schema: &Schema,
        datoms: &[Datom],
        program: &CompiledProgram,
        view: &TemporalView,
    ) -> Result<&'a DocumentEvaluation, ApiError> {
        if let Some(index) = cache.iter().position(|evaluation| &evaluation.view == view) {
            return Ok(&cache[index]);
        }

        let state = match view {
            TemporalView::AsOf(element) => MaterializedResolver.as_of(schema, datoms, element)?,
            TemporalView::Current => MaterializedResolver.current(schema, datoms)?,
        };
        let derived = SemiNaiveRuntime.evaluate(&state, program)?;
        cache.push(DocumentEvaluation {
            view: view.clone(),
            state,
            derived,
        });
        Ok(cache
            .last()
            .expect("evaluation cache contains the inserted view"))
    }
}

fn filter_datoms(datoms: Vec<Datom>, policy_context: Option<&PolicyContext>) -> Vec<Datom> {
    datoms
        .into_iter()
        .filter(|datom| policy_allows(policy_context, datom.policy.as_ref()))
        .collect()
}

fn filter_extensional_facts(
    facts: Vec<ExtensionalFact>,
    policy_context: Option<&PolicyContext>,
) -> Vec<ExtensionalFact> {
    facts
        .into_iter()
        .filter(|fact| policy_allows(policy_context, fact.policy.as_ref()))
        .collect()
}

fn filter_compiled_program(
    program: &CompiledProgram,
    policy_context: Option<&PolicyContext>,
) -> CompiledProgram {
    let mut filtered = program.clone();
    filtered.facts = filter_extensional_facts(filtered.facts, policy_context);
    filtered
}

fn filter_resolved_state(
    state: &ResolvedState,
    policy_context: Option<&PolicyContext>,
) -> ResolvedState {
    let mut filtered = ResolvedState {
        entities: indexmap::IndexMap::new(),
        as_of: state.as_of,
    };

    for (entity_id, entity_state) in &state.entities {
        let mut visible_entity = aether_resolver::EntityState::default();
        for (attribute_id, value) in &entity_state.attributes {
            let visible_facts = entity_state
                .facts(attribute_id)
                .iter()
                .filter(|fact| policy_allows(policy_context, fact.policy.as_ref()))
                .cloned()
                .collect::<Vec<_>>();
            if visible_facts.is_empty() {
                continue;
            }
            let visible_value = match value {
                ResolvedValue::Scalar(_) => {
                    ResolvedValue::Scalar(visible_facts.last().map(|fact| fact.value.clone()))
                }
                ResolvedValue::Set(_) => ResolvedValue::Set(
                    visible_facts
                        .iter()
                        .map(|fact| fact.value.clone())
                        .collect::<Vec<_>>(),
                ),
                ResolvedValue::Sequence(_) => ResolvedValue::Sequence(
                    visible_facts
                        .iter()
                        .map(|fact| fact.value.clone())
                        .collect::<Vec<_>>(),
                ),
            };
            visible_entity
                .attributes
                .insert(*attribute_id, visible_value);
            visible_entity.facts.insert(*attribute_id, visible_facts);
        }
        if !visible_entity.attributes.is_empty() {
            filtered.entities.insert(*entity_id, visible_entity);
        }
    }

    filtered
}

fn filter_derived_set(derived: &DerivedSet, policy_context: Option<&PolicyContext>) -> DerivedSet {
    let tuples = derived
        .tuples
        .iter()
        .filter(|tuple| policy_allows(policy_context, tuple.policy.as_ref()))
        .cloned()
        .collect::<Vec<_>>();
    let visible_ids = tuples
        .iter()
        .map(|tuple| tuple.tuple.id)
        .collect::<std::collections::BTreeSet<_>>();
    let predicate_index = derived
        .predicate_index
        .iter()
        .map(|(predicate, tuple_ids)| {
            (
                *predicate,
                tuple_ids
                    .iter()
                    .copied()
                    .filter(|tuple_id| visible_ids.contains(tuple_id))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();

    DerivedSet {
        tuples,
        iterations: derived.iterations.clone(),
        predicate_index,
    }
}

fn filter_trace(
    trace: DerivationTrace,
    policy_context: Option<&PolicyContext>,
) -> Result<DerivationTrace, ApiError> {
    let tuples = trace
        .tuples
        .into_iter()
        .filter(|tuple| policy_allows(policy_context, tuple.policy.as_ref()))
        .collect::<Vec<_>>();
    if tuples.iter().all(|tuple| tuple.tuple.id != trace.root) {
        return Err(ApiError::Validation(
            "requested tuple is not visible under the current policy".into(),
        ));
    }
    Ok(DerivationTrace {
        root: trace.root,
        tuples,
    })
}

impl<J, S> KernelServiceCore<J, S>
where
    J: Journal,
    S: SidecarFederation + Default,
{
    pub fn from_journal(journal: J) -> Self {
        Self::from_parts(journal, S::default())
    }
}

#[derive(Clone, Debug)]
struct DocumentEvaluation {
    view: TemporalView,
    state: ResolvedState,
    derived: DerivedSet,
}

#[derive(Clone, Debug)]
struct CachedDerivedSet {
    derived: DerivedSet,
}

impl<J: Journal, S: SidecarFederation> KernelService for KernelServiceCore<J, S> {
    fn append(&mut self, request: AppendRequest) -> Result<AppendResponse, ApiError> {
        self.journal.append(&request.datoms)?;
        Ok(AppendResponse {
            appended: request.datoms.len(),
        })
    }

    fn history(&self, request: HistoryRequest) -> Result<HistoryResponse, ApiError> {
        Ok(HistoryResponse {
            datoms: self.visible_history(&[], request.policy_context.as_ref())?,
        })
    }

    fn current_state(
        &self,
        request: CurrentStateRequest,
    ) -> Result<CurrentStateResponse, ApiError> {
        let datoms = self.datoms_or_history(&request.datoms)?;
        Ok(CurrentStateResponse {
            state: filter_resolved_state(
                &MaterializedResolver.current(&request.schema, &datoms)?,
                request.policy_context.as_ref(),
            ),
        })
    }

    fn as_of(&self, request: AsOfRequest) -> Result<AsOfResponse, ApiError> {
        let datoms = self.datoms_or_history(&request.datoms)?;
        Ok(AsOfResponse {
            state: filter_resolved_state(
                &MaterializedResolver.as_of(&request.schema, &datoms, &request.at)?,
                request.policy_context.as_ref(),
            ),
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
        self.cache_derived(derived.clone());
        Ok(EvaluateProgramResponse {
            derived: filter_derived_set(&derived, request.policy_context.as_ref()),
        })
    }

    fn explain_tuple(
        &self,
        request: ExplainTupleRequest,
    ) -> Result<ExplainTupleResponse, ApiError> {
        let cached = self
            .last_derived
            .as_ref()
            .ok_or_else(|| ApiError::Validation("no derived tuples are cached".into()))?;
        let trace = InMemoryExplainer::from_derived_set(&cached.derived)
            .explain_tuple(&request.tuple_id)?;
        Ok(ExplainTupleResponse {
            trace: filter_trace(trace, request.policy_context.as_ref())?,
        })
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
            queries: document.queries,
            explains: document.explains,
        })
    }

    fn run_document(
        &mut self,
        request: RunDocumentRequest,
    ) -> Result<RunDocumentResponse, ApiError> {
        let document = DefaultDslParser.parse_document(&request.dsl)?;
        let datoms = self.datoms_or_history(&[])?;
        let program = DefaultRuleCompiler.compile(&document.schema, &document.program)?;
        let mut evaluations = Vec::new();
        let primary_view = document
            .query
            .as_ref()
            .map(|query| query.view.clone())
            .or_else(|| {
                document
                    .queries
                    .first()
                    .map(|query| query.spec.view.clone())
            })
            .or_else(|| {
                document
                    .explains
                    .first()
                    .map(|explain| explain.spec.view.clone())
            })
            .unwrap_or(TemporalView::Current);
        let primary = self.document_evaluation(
            &mut evaluations,
            &document.schema,
            &datoms,
            &program,
            &primary_view,
        )?;
        let primary_state = primary.state.clone();
        let primary_derived = primary.derived.clone();
        let query = match &document.query {
            Some(query) => Some(execute_query(
                &primary_state,
                &program,
                &primary_derived,
                &query.query,
                request.policy_context.as_ref(),
            )?),
            None => None,
        };
        let queries = document
            .queries
            .iter()
            .map(|named_query| {
                let evaluation = self.document_evaluation(
                    &mut evaluations,
                    &document.schema,
                    &datoms,
                    &program,
                    &named_query.spec.view,
                )?;
                Ok(NamedQueryResult {
                    name: named_query.name.clone(),
                    spec: named_query.spec.clone(),
                    result: execute_query(
                        &evaluation.state,
                        &program,
                        &evaluation.derived,
                        &named_query.spec.query,
                        request.policy_context.as_ref(),
                    )?,
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        let explains = document
            .explains
            .iter()
            .map(|named_explain| {
                let evaluation = self.document_evaluation(
                    &mut evaluations,
                    &document.schema,
                    &datoms,
                    &program,
                    &named_explain.spec.view,
                )?;
                Ok(NamedExplainResult {
                    name: named_explain.name.clone(),
                    spec: named_explain.spec.clone(),
                    result: execute_explain_spec(
                        &program,
                        evaluation,
                        &named_explain.spec,
                        request.policy_context.as_ref(),
                    )?,
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        self.cache_derived(primary_derived.clone());
        let derived = filter_derived_set(&primary_derived, request.policy_context.as_ref());

        Ok(RunDocumentResponse {
            state: filter_resolved_state(&primary_state, request.policy_context.as_ref()),
            program: filter_compiled_program(&program, request.policy_context.as_ref()),
            derived,
            query,
            queries,
            explains,
        })
    }

    fn register_artifact_reference(
        &mut self,
        request: RegisterArtifactReferenceRequest,
    ) -> Result<RegisterArtifactReferenceResponse, ApiError> {
        let journal = self.sidecar_journal_catalog()?;
        Ok(self
            .sidecars
            .register_artifact_reference(request, &journal)?)
    }

    fn get_artifact_reference(
        &self,
        request: GetArtifactReferenceRequest,
    ) -> Result<GetArtifactReferenceResponse, ApiError> {
        Ok(self.sidecars.get_artifact_reference(request)?)
    }

    fn register_vector_record(
        &mut self,
        request: RegisterVectorRecordRequest,
    ) -> Result<RegisterVectorRecordResponse, ApiError> {
        let journal = self.sidecar_journal_catalog()?;
        Ok(self.sidecars.register_vector_record(request, &journal)?)
    }

    fn search_vectors(
        &self,
        request: SearchVectorsRequest,
    ) -> Result<SearchVectorsResponse, ApiError> {
        let journal = self.sidecar_journal_catalog()?;
        Ok(self.sidecars.search_vectors(request, &journal)?)
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
pub struct HistoryRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HistoryResponse {
    pub datoms: Vec<Datom>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CurrentStateRequest {
    pub schema: Schema,
    pub datoms: Vec<Datom>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EvaluateProgramResponse {
    pub derived: DerivedSet,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExplainTupleRequest {
    pub tuple_id: TupleId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
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
    pub queries: Vec<NamedQuerySpec>,
    pub explains: Vec<NamedExplainSpec>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RunDocumentRequest {
    pub dsl: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RunDocumentResponse {
    pub state: ResolvedState,
    pub program: CompiledProgram,
    pub derived: DerivedSet,
    pub query: Option<QueryResult>,
    pub queries: Vec<NamedQueryResult>,
    pub explains: Vec<NamedExplainResult>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NamedQueryResult {
    pub name: Option<String>,
    pub spec: QuerySpec,
    pub result: QueryResult,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ExplainArtifact {
    Plan(PlanExplanation),
    Tuple(DerivationTrace),
}

impl Default for ExplainArtifact {
    fn default() -> Self {
        Self::Plan(PlanExplanation::default())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NamedExplainResult {
    pub name: Option<String>,
    pub spec: ExplainSpec,
    pub result: ExplainArtifact,
}

fn execute_explain_spec(
    program: &CompiledProgram,
    evaluation: &DocumentEvaluation,
    spec: &ExplainSpec,
    policy_context: Option<&PolicyContext>,
) -> Result<ExplainArtifact, ApiError> {
    match &spec.target {
        ExplainTarget::Plan => Ok(ExplainArtifact::Plan(
            InMemoryExplainer::default().explain_plan(&program.phase_graph)?,
        )),
        ExplainTarget::Tuple(atom) => {
            let visible = filter_derived_set(&evaluation.derived, policy_context);
            let tuple_id = find_matching_derived_tuple(&visible, atom).ok_or_else(|| {
                ApiError::Validation(format!(
                    "no derived tuple matched explain target {}",
                    atom.predicate.name
                ))
            })?;
            Ok(ExplainArtifact::Tuple(filter_trace(
                InMemoryExplainer::from_derived_set(&evaluation.derived)
                    .explain_tuple(&tuple_id)?,
                policy_context,
            )?))
        }
    }
}

fn find_matching_derived_tuple(derived: &DerivedSet, atom: &aether_ast::Atom) -> Option<TupleId> {
    derived.tuples.iter().find_map(|tuple| {
        if tuple.tuple.predicate != atom.predicate.id
            || tuple.tuple.values.len() != atom.terms.len()
        {
            return None;
        }
        let matches = atom
            .terms
            .iter()
            .zip(&tuple.tuple.values)
            .all(|(term, value)| match term {
                Term::Value(expected) => expected == value,
                Term::Variable(_) | Term::Aggregate(_) => false,
            });
        matches.then_some(tuple.tuple.id)
    })
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("validation error: {0}")]
    Validation(String),
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Sidecar(#[from] SidecarError),
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
        coordination_pilot_dsl, coordination_pilot_seed_history, ApiError, AppendRequest,
        CurrentStateRequest, ExplainArtifact, ExplainTupleRequest, InMemoryKernelService,
        KernelService, ParseDocumentRequest, RunDocumentRequest,
        COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT, COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT,
    };
    use aether_ast::{ElementId, EntityId, PolicyContext, PolicyEnvelope, Value};

    #[test]
    fn service_models_multi_worker_lease_handoff_and_fencing() {
        let mut service = InMemoryKernelService::new();
        service
            .append(AppendRequest {
                datoms: coordination_pilot_seed_history(),
            })
            .expect("append journal");

        let parsed = service
            .parse_document(ParseDocumentRequest {
                dsl: coordination_pilot_dsl(
                    &format!("as_of e{}", COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT),
                    "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
                ),
            })
            .expect("parse coordination document");
        assert_eq!(parsed.program.facts.len(), 7);

        let pre_heartbeat_authorized = service
            .run_document(RunDocumentRequest {
                dsl: coordination_pilot_dsl(
                    &format!("as_of e{}", COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT),
                    "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
                ),
                policy_context: None,
            })
            .expect("run pre-heartbeat authorization document");
        assert_eq!(
            pre_heartbeat_authorized.state.as_of,
            Some(ElementId::new(COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT))
        );
        assert!(pre_heartbeat_authorized
            .query
            .as_ref()
            .expect("query result should exist")
            .rows
            .is_empty());

        let as_of_authorized = service
            .run_document(RunDocumentRequest {
                dsl: coordination_pilot_dsl(
                    &format!("as_of e{}", COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT),
                    "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
                ),
                policy_context: None,
            })
            .expect("run as_of authorization document");
        assert_eq!(
            as_of_authorized.state.as_of,
            Some(ElementId::new(COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT))
        );
        let as_of_authorized_rows = &as_of_authorized
            .query
            .as_ref()
            .expect("query result should exist")
            .rows;
        assert_eq!(as_of_authorized_rows.len(), 1);
        assert_eq!(
            as_of_authorized_rows[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-a".into()),
                Value::U64(1),
            ]
        );

        let current_authorized = service
            .run_document(RunDocumentRequest {
                dsl: coordination_pilot_dsl(
                    "current",
                    "goal execution_authorized(t, worker, epoch)\n  keep t, worker, epoch",
                ),
                policy_context: None,
            })
            .expect("run current authorization document");
        let authorized_rows = &current_authorized
            .query
            .as_ref()
            .expect("query result should exist")
            .rows;
        assert_eq!(authorized_rows.len(), 1);
        assert_eq!(
            authorized_rows[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-b".into()),
                Value::U64(2),
            ]
        );
        let trace = service
            .explain_tuple(ExplainTupleRequest {
                tuple_id: authorized_rows[0]
                    .tuple_id
                    .expect("execution_authorized tuple id"),
                policy_context: None,
            })
            .expect("explain authorization tuple")
            .trace;
        assert!(!trace.tuples.is_empty());

        let claimable = service
            .run_document(RunDocumentRequest {
                dsl: coordination_pilot_dsl(
                    "current",
                    "goal worker_can_claim(t, worker)\n  keep t, worker",
                ),
                policy_context: None,
            })
            .expect("run claimability document");
        let claimable_rows = &claimable
            .query
            .as_ref()
            .expect("query result should exist")
            .rows;
        assert_eq!(claimable_rows.len(), 2);
        assert_eq!(
            claimable_rows
                .iter()
                .map(|row| row.values.clone())
                .collect::<Vec<_>>(),
            vec![
                vec![
                    Value::Entity(EntityId::new(3)),
                    Value::String("worker-a".into()),
                ],
                vec![
                    Value::Entity(EntityId::new(3)),
                    Value::String("worker-b".into()),
                ],
            ]
        );

        let accepted_outcomes = service
            .run_document(RunDocumentRequest {
                dsl: coordination_pilot_dsl(
                    "current",
                    "goal execution_outcome_accepted(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
                ),
                policy_context: None,
            })
            .expect("run accepted-outcome document");
        let accepted_rows = &accepted_outcomes
            .query
            .as_ref()
            .expect("query result should exist")
            .rows;
        assert_eq!(
            accepted_rows[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-b".into()),
                Value::U64(2),
                Value::String("completed".into()),
                Value::String("current-worker-b".into()),
            ]
        );

        let rejected_outcomes = service
            .run_document(RunDocumentRequest {
                dsl: coordination_pilot_dsl(
                    "current",
                    "goal execution_outcome_rejected_stale(t, worker, epoch, status, detail)\n  keep t, worker, epoch, status, detail",
                ),
                policy_context: None,
            })
            .expect("run rejected-outcome document");
        let rejected_rows = &rejected_outcomes
            .query
            .as_ref()
            .expect("query result should exist")
            .rows;
        assert_eq!(
            rejected_rows[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-a".into()),
                Value::U64(1),
                Value::String("completed".into()),
                Value::String("stale-worker-a".into()),
            ]
        );
    }

    #[test]
    fn service_parses_and_runs_named_queries_and_explain_directives() {
        let mut service = InMemoryKernelService::new();
        service
            .append(AppendRequest {
                datoms: vec![dependency_datom(1, 2, 1), dependency_datom(2, 3, 2)],
            })
            .expect("append transitive chain");

        let parsed = service
            .parse_document(ParseDocumentRequest {
                dsl: transitive_document_dsl(),
            })
            .expect("parse transitive document");
        assert_eq!(parsed.query, Some(parsed.queries[0].spec.clone()));
        assert_eq!(parsed.queries.len(), 2);
        assert_eq!(parsed.explains.len(), 2);

        let response = service
            .run_document(RunDocumentRequest {
                dsl: transitive_document_dsl(),
                policy_context: None,
            })
            .expect("run named-query document");
        assert_eq!(response.query, Some(response.queries[0].result.clone()));
        assert_eq!(response.queries.len(), 2);
        assert_eq!(response.explains.len(), 2);
        assert_eq!(
            response.queries[0].result.rows[0].values,
            vec![Value::Entity(EntityId::new(2))]
        );
        assert_eq!(
            response.queries[1]
                .result
                .rows
                .iter()
                .map(|row| row.values.clone())
                .collect::<Vec<_>>(),
            vec![
                vec![Value::Entity(EntityId::new(2))],
                vec![Value::Entity(EntityId::new(3))],
            ]
        );
        assert!(matches!(
            &response.explains[0].result,
            ExplainArtifact::Tuple(trace) if !trace.tuples.is_empty()
        ));
        assert!(matches!(
            &response.explains[1].result,
            ExplainArtifact::Plan(explanation) if !explanation.phase_graph.nodes.is_empty()
        ));
    }

    #[test]
    fn service_filters_state_and_derivation_by_policy_context() {
        let mut service = InMemoryKernelService::new();
        let dsl = r#"
schema {
  attr task.status: ScalarLWW<String>
}

predicates {
  task_status(Entity, String)
  protected_fact(Entity)
  visible_task(Entity)
}

rules {
  visible_task(t) <- task_status(t, "ready")
  visible_task(t) <- protected_fact(t)
}

materialize {
  visible_task
}

facts {
  protected_fact(entity(1))
  protected_fact(entity(2)) @capability("executor")
}

query current_cut {
  current
  goal visible_task(t)
  keep t
}
"#;

        let parsed = service
            .parse_document(ParseDocumentRequest { dsl: dsl.into() })
            .expect("parse policy document");
        service
            .append(AppendRequest {
                datoms: vec![
                    status_datom(1, "ready", 1, None),
                    status_datom(
                        3,
                        "ready",
                        2,
                        Some(PolicyEnvelope {
                            capabilities: vec!["executor".into()],
                            visibilities: Vec::new(),
                        }),
                    ),
                ],
            })
            .expect("append policy datoms");

        let default_state = service
            .current_state(CurrentStateRequest {
                schema: parsed.schema.clone(),
                datoms: Vec::new(),
                policy_context: None,
            })
            .expect("resolve default state");
        assert_eq!(default_state.state.entities.len(), 1);

        let executor_state = service
            .current_state(CurrentStateRequest {
                schema: parsed.schema.clone(),
                datoms: Vec::new(),
                policy_context: Some(PolicyContext {
                    capabilities: vec!["executor".into()],
                    visibilities: Vec::new(),
                }),
            })
            .expect("resolve executor state");
        assert_eq!(executor_state.state.entities.len(), 2);

        let default_result = service
            .run_document(RunDocumentRequest {
                dsl: dsl.into(),
                policy_context: None,
            })
            .expect("run default policy document");
        assert_eq!(
            default_result
                .query
                .expect("default query result")
                .rows
                .into_iter()
                .map(|row| row.values)
                .collect::<Vec<_>>(),
            vec![vec![Value::Entity(EntityId::new(1))]]
        );

        let executor_result = service
            .run_document(RunDocumentRequest {
                dsl: dsl.into(),
                policy_context: Some(PolicyContext {
                    capabilities: vec!["executor".into()],
                    visibilities: Vec::new(),
                }),
            })
            .expect("run executor policy document");
        let executor_rows = executor_result
            .query
            .as_ref()
            .expect("executor query result")
            .rows
            .clone();
        assert_eq!(
            executor_rows
                .into_iter()
                .map(|row| row.values)
                .collect::<Vec<_>>(),
            vec![
                vec![Value::Entity(EntityId::new(1))],
                vec![Value::Entity(EntityId::new(2))],
                vec![Value::Entity(EntityId::new(3))],
            ]
        );

        let protected_tuple = executor_result
            .query
            .as_ref()
            .expect("executor query result")
            .rows
            .iter()
            .find(|row| row.values == vec![Value::Entity(EntityId::new(3))])
            .and_then(|row| row.tuple_id)
            .expect("protected tuple id");
        let mismatch = service
            .explain_tuple(ExplainTupleRequest {
                tuple_id: protected_tuple,
                policy_context: None,
            })
            .expect_err("explain should reject mismatched policy context");
        assert!(matches!(
            mismatch,
            ApiError::Validation(message)
                if message == "requested tuple is not visible under the current policy"
        ));
        let executor_trace = service
            .explain_tuple(ExplainTupleRequest {
                tuple_id: protected_tuple,
                policy_context: Some(PolicyContext {
                    capabilities: vec!["executor".into()],
                    visibilities: Vec::new(),
                }),
            })
            .expect("explain protected tuple with matching policy")
            .trace;
        assert!(!executor_trace.tuples.is_empty());
    }

    fn transitive_document_dsl() -> String {
        r#"
schema {
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

query first_cut {
  as_of e1
  goal depends_transitive(entity(1), y)
  keep y
}

query current_cut {
  current
  goal depends_transitive(entity(1), y)
  keep y
}

explain current_path {
  tuple depends_transitive(entity(1), entity(3))
}

explain plan_shape {
  plan
}
"#
        .into()
    }

    fn dependency_datom(entity: u64, value: u64, element: u64) -> aether_ast::Datom {
        aether_ast::Datom {
            entity: EntityId::new(entity),
            attribute: aether_ast::AttributeId::new(1),
            value: Value::Entity(EntityId::new(value)),
            op: aether_ast::OperationKind::Add,
            element: ElementId::new(element),
            replica: aether_ast::ReplicaId::new(1),
            causal_context: Default::default(),
            provenance: aether_ast::DatomProvenance::default(),
            policy: None,
        }
    }

    fn status_datom(
        entity: u64,
        status: &str,
        element: u64,
        policy: Option<PolicyEnvelope>,
    ) -> aether_ast::Datom {
        aether_ast::Datom {
            entity: EntityId::new(entity),
            attribute: aether_ast::AttributeId::new(1),
            value: Value::String(status.into()),
            op: aether_ast::OperationKind::Assert,
            element: ElementId::new(element),
            replica: aether_ast::ReplicaId::new(1),
            causal_context: Default::default(),
            provenance: aether_ast::DatomProvenance::default(),
            policy,
        }
    }
}
