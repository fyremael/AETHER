use aether_ast::{
    policy_allows, Datom, DerivationTrace, ElementId, ExplainSpec, ExplainTarget, NamedExplainSpec,
    NamedQuerySpec, PhaseGraph, PlanExplanation, PolicyContext, PolicyScope, QueryResult,
    QuerySpec, RuleProgram, TemporalView, Term, TupleId,
};
use aether_explain::{ExplainError, Explainer, InMemoryExplainer};
use aether_plan::CompiledProgram;
use aether_resolver::{ResolveError, ResolvedState};
use aether_rules::{DefaultDslParser, DefaultRuleCompiler, DslParser, ParseError, RuleCompiler};
use aether_runtime::{
    execute_scoped_query, DerivedSet, EvaluationBundle, RuleRuntime, RuntimeError, RuntimeLimits,
    SemiNaiveRuntime,
};
use aether_schema::Schema;
use aether_storage::{InMemoryJournal, Journal, JournalError, PostgresJournal, SqliteJournal};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

pub(crate) const ENGINE_SEMANTICS_VERSION: &str = "aether-semantic-v1-policy-scope-1";

mod admission;
mod evaluation;
pub mod execution;

use admission::{
    activate_schema, append_receipts as list_append_receipts, commit_append,
    document_schema_compatible, ensure_legacy_schema, extend_legacy_schema_if_needed,
    register_schema, replicate_append_receipt, resolve_idempotent_append, schema_catalog,
    validate_append, AdmissionError,
};
use evaluation::{
    project_history, project_history_at_view, resolve_snapshot, EvaluationKey,
    ScopedEvaluationBuilder,
};
use execution::{
    execution_catalog_path_for_journal, persist_execution, resolve_trace, ExecutionError,
    ExecutionStore, InMemoryExecutionStore, SqliteExecutionStore,
};

pub mod deployment;
pub mod http;
pub mod namespace;
pub mod partitioned;
#[doc(hidden)]
pub mod perf;
pub mod pilot;
pub mod report;
pub mod sidecar;
pub mod status;

pub use admission::{
    ActivateSchemaRequest, AppendAdmissionRequest, AppendDryRunResponse, AppendReceipt, BatchId,
    HistoryCertificationStatus, NamespaceSchemaRevision, RegisterSchemaRequest,
    SchemaBaselineReceipt, SchemaCatalogResponse, SchemaCompatibility, SchemaStatus,
};
pub use aether_storage::JournalCutRef;
pub use aether_storage::{PostgresTlsConfig, PostgresTlsMode};
pub use deployment::{
    default_audit_log_path, serve_pilot_http_service, DeploymentError, PilotAuthConfig,
    PilotConcurrencyConfig, PilotHttpTransportConfig, PilotServiceConfig, PilotStorageConfig,
    PilotTokenConfig, ResolvedPilotHttpTransport, ResolvedPilotServiceConfig, ResolvedPilotStorage,
    ResolvedPilotTokenSummary,
};
pub use execution::{
    ContentDigest, ExecutionId, ExecutionManifest, ExecutionReceipt, FederatedExecutionSource,
    FederationManifest, JournalCut, ResolveTraceHandleRequest, ResolveTraceHandleResponse,
    SchemaRef, TraceHandle, TraceHandleBinding, TraceRecord, DEFAULT_EXECUTION_RETENTION,
};
pub use http::{
    http_router, http_router_with_options, http_router_with_partitioned_options,
    http_router_with_postgres_namespaces, http_router_with_postgres_namespaces_and_tls,
    http_router_with_sqlite_namespaces, AuditContext, AuditEntry, AuditLogResponse, AuthScope,
    HealthResponse, HttpAccessToken, HttpAuthConfig, HttpKernelOptions, HttpKernelState,
    HttpResourceLimits, PageInfo, PageRequest, PagedHistoryResponse, PagedRunDocumentResponse,
    PagedTraceResponse, StructuredErrorResponse, AETHER_NAMESPACE_HEADER, AETHER_REQUEST_ID_HEADER,
};
pub use namespace::NamespaceId;
pub use partitioned::{
    render_federated_explain_report_markdown, AuthorityPartitionConfig, FederatedExplainReport,
    FederatedHistoryRequest, FederatedHistoryResponse, FederatedImportedSourceSummary,
    FederatedNamedQuerySummary, FederatedReportRow, FederatedRunDocumentRequest,
    FederatedRunDocumentResponse, FederatedTraceSummary, FederatedTraceTupleSummary,
    ImportedFactQueryRequest, ImportedFactQueryResponse, LeaderEpoch, PartitionAppendRequest,
    PartitionAppendResponse, PartitionHistoryRequest, PartitionHistoryResponse,
    PartitionStateRequest, PartitionStateResponse, PartitionStatus, PartitionStatusResponse,
    PartitionedInMemoryKernelService, PromoteReplicaRequest, PromoteReplicaResponse, ReplicaConfig,
    ReplicaRole, ReplicaStatus, ReplicatedAuthorityPartitionService,
    SqlitePartitionedKernelService,
};
pub use pilot::{
    coordination_pilot_dsl, coordination_pilot_seed_history,
    COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT, COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT,
};
pub use report::{
    build_coordination_delta_report, build_coordination_pilot_report,
    build_coordination_pilot_report_with_policy, render_coordination_delta_report_markdown,
    render_coordination_pilot_report_markdown, CoordinationCut, CoordinationDeltaReport,
    CoordinationDeltaReportRequest, CoordinationPilotReport, CoordinationTraceHandle, ReportRow,
    ReportRowChange, ReportRowDiff, ReportSectionDelta, TraceSummary, TraceTupleSummary,
};
pub use sidecar::{
    ArtifactReference, GetArtifactReferenceRequest, GetArtifactReferenceResponse,
    InMemorySidecarFederation, JournalCatalog, RegisterArtifactReferenceRequest,
    RegisterArtifactReferenceResponse, RegisterVectorRecordRequest, RegisterVectorRecordResponse,
    SearchVectorsRequest, SearchVectorsResponse, SidecarError, SidecarFederation,
    SqliteSidecarFederation, VectorFactProjection, VectorMetric, VectorRecordMetadata,
    VectorSearchMatch,
};
pub use status::{
    AuthReloadResponse, NamespaceStatusSummary, PrincipalStatusSummary, ReplicaStatusSummary,
    ServiceMode, ServiceResourceControlStatus, ServiceStatusResponse, ServiceStatusStorage,
    ServiceTransportStatus,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DocumentExecutionLimits {
    pub max_document_bytes: usize,
    pub max_rules: usize,
    pub max_iterations: usize,
    pub max_derived_tuples: usize,
}

impl DocumentExecutionLimits {
    pub const UNBOUNDED: Self = Self {
        max_document_bytes: usize::MAX,
        max_rules: usize::MAX,
        max_iterations: usize::MAX,
        max_derived_tuples: usize::MAX,
    };
}

pub trait KernelService {
    fn append(&mut self, request: AppendRequest) -> Result<AppendResponse, ApiError>;
    fn admit_append(&mut self, request: AppendAdmissionRequest) -> Result<AppendReceipt, ApiError>;
    fn dry_run_append(
        &mut self,
        request: AppendAdmissionRequest,
    ) -> Result<AppendDryRunResponse, ApiError>;
    fn append_receipts(&self) -> Result<Vec<AppendReceipt>, ApiError>;
    fn register_schema(
        &mut self,
        request: RegisterSchemaRequest,
    ) -> Result<NamespaceSchemaRevision, ApiError>;
    fn activate_schema(
        &mut self,
        request: ActivateSchemaRequest,
    ) -> Result<NamespaceSchemaRevision, ApiError>;
    fn schema_catalog(&self) -> Result<SchemaCatalogResponse, ApiError>;
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
    fn resolve_trace_handle(
        &mut self,
        request: ResolveTraceHandleRequest,
    ) -> Result<ResolveTraceHandleResponse, ApiError>;
    fn explain_plan(&self, request: ExplainPlanRequest) -> Result<ExplainPlanResponse, ApiError>;
    fn parse_document(
        &self,
        request: ParseDocumentRequest,
    ) -> Result<ParseDocumentResponse, ApiError>;
    fn run_document(
        &mut self,
        request: RunDocumentRequest,
    ) -> Result<RunDocumentResponse, ApiError>;
    fn run_document_with_limits(
        &mut self,
        request: RunDocumentRequest,
        limits: DocumentExecutionLimits,
    ) -> Result<RunDocumentResponse, ApiError> {
        if request.dsl.len() > limits.max_document_bytes {
            return Err(ApiError::ResourceLimit {
                resource: "document_bytes",
                limit: limits.max_document_bytes,
                observed: request.dsl.len(),
            });
        }
        let response = self.run_document(request)?;
        if response.program.rules.len() > limits.max_rules {
            return Err(ApiError::ResourceLimit {
                resource: "document_rules",
                limit: limits.max_rules,
                observed: response.program.rules.len(),
            });
        }
        Ok(response)
    }
    fn coordination_pilot_report(
        &mut self,
        request: CoordinationPilotReportRequest,
    ) -> Result<CoordinationPilotReport, ApiError>;
    fn coordination_delta_report(
        &mut self,
        request: CoordinationDeltaReportRequest,
    ) -> Result<CoordinationDeltaReport, ApiError>;
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
pub type PostgresKernelService = KernelServiceCore<PostgresJournal, SqliteSidecarFederation>;

#[derive(Debug)]
pub struct KernelServiceCore<J: Journal, S: SidecarFederation = InMemorySidecarFederation> {
    journal: J,
    sidecars: S,
    namespace: NamespaceId,
    execution_store: Box<dyn ExecutionStore>,
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
        let execution_store = SqliteExecutionStore::open(execution_catalog_path_for_journal(path))
            .map_err(ExecutionError::from)?;
        Ok(Self::from_parts_with_execution_store(
            SqliteJournal::open(path)?,
            SqliteSidecarFederation::open(sidecar::sidecar_catalog_path_for_journal(path))?,
            NamespaceId::default(),
            Box::new(execution_store),
        ))
    }
}

impl KernelServiceCore<PostgresJournal, SqliteSidecarFederation> {
    pub fn open_postgres(
        database_url: &str,
        schema: &str,
        namespace: &str,
        sidecar_path: impl AsRef<Path>,
    ) -> Result<Self, ApiError> {
        Self::open_postgres_with_tls(
            database_url,
            schema,
            namespace,
            sidecar_path,
            &PostgresTlsConfig::default(),
        )
    }

    pub fn open_postgres_with_tls(
        database_url: &str,
        schema: &str,
        namespace: &str,
        sidecar_path: impl AsRef<Path>,
        tls: &PostgresTlsConfig,
    ) -> Result<Self, ApiError> {
        let sidecar_path = sidecar_path.as_ref();
        let execution_store =
            SqliteExecutionStore::open(execution_catalog_path_for_journal(sidecar_path))
                .map_err(ExecutionError::from)?;
        Ok(Self::from_parts_with_execution_store(
            PostgresJournal::open_with_tls(database_url, schema, namespace, tls)?,
            SqliteSidecarFederation::open(sidecar_path)?,
            NamespaceId::new(namespace).map_err(ApiError::Validation)?,
            Box::new(execution_store),
        ))
    }
}

impl<J: Journal, S: SidecarFederation> KernelServiceCore<J, S> {
    pub fn from_parts(journal: J, sidecars: S) -> Self {
        Self::from_parts_with_execution_store(
            journal,
            sidecars,
            NamespaceId::default(),
            Box::<InMemoryExecutionStore>::default(),
        )
    }

    fn from_parts_with_execution_store(
        journal: J,
        sidecars: S,
        namespace: NamespaceId,
        execution_store: Box<dyn ExecutionStore>,
    ) -> Self {
        Self {
            journal,
            sidecars,
            namespace,
            execution_store,
        }
    }

    pub fn with_namespace(mut self, namespace: NamespaceId) -> Self {
        self.namespace = namespace;
        self
    }

    pub(crate) fn replicate_admitted_append(
        &mut self,
        revision: &NamespaceSchemaRevision,
        receipt: &AppendReceipt,
        datoms: Vec<Datom>,
    ) -> Result<AppendReceipt, ApiError> {
        Ok(replicate_append_receipt(
            &mut self.journal,
            revision,
            receipt,
            datoms,
        )?)
    }

    pub(crate) fn authority_history(&self) -> Result<Vec<Datom>, ApiError> {
        Ok(self.journal.history()?)
    }

    pub(crate) fn authority_append_receipts(&self) -> Result<Vec<AppendReceipt>, ApiError> {
        Ok(list_append_receipts(&self.journal)?)
    }

    pub(crate) fn authority_schema_catalog(&self) -> Result<SchemaCatalogResponse, ApiError> {
        Ok(schema_catalog(&self.journal)?)
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
        scope: &PolicyScope,
    ) -> Result<Vec<Datom>, ApiError> {
        project_history(&self.datoms_or_history(datoms)?, scope.clone())
    }

    fn sidecar_journal_catalog(&self) -> Result<JournalCatalog, ApiError> {
        Ok(JournalCatalog::from_history(&self.journal.history()?))
    }

    fn scoped_sidecar_journal_catalog(
        &self,
        scope: PolicyScope,
    ) -> Result<JournalCatalog, ApiError> {
        let history = self.journal.history()?;
        Ok(JournalCatalog::from_history(&project_history(
            &history, scope,
        )?))
    }

    fn document_evaluation<'a>(
        &self,
        cache: &'a mut Vec<DocumentEvaluation>,
        builder: &ScopedEvaluationBuilder<'_>,
        view: &TemporalView,
        limits: DocumentExecutionLimits,
    ) -> Result<&'a DocumentEvaluation, ApiError> {
        if let Some(index) = cache.iter().position(|evaluation| &evaluation.view == view) {
            return Ok(&cache[index]);
        }

        let (key, evaluation) = builder.evaluate_with_key_and_limits(
            view.clone(),
            RuntimeLimits {
                max_iterations: limits.max_iterations,
                max_derived_tuples: limits.max_derived_tuples,
            },
        )?;
        cache.push(DocumentEvaluation {
            view: view.clone(),
            key,
            evaluation,
        });
        Ok(cache
            .last()
            .expect("evaluation cache contains the inserted view"))
    }
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

fn assert_trace_visible(trace: &DerivationTrace, scope: &PolicyScope) -> Result<(), ApiError> {
    if trace
        .tuples
        .iter()
        .all(|tuple| scope.allows(tuple.policy.as_ref()))
    {
        Ok(())
    } else {
        Err(ApiError::Validation(
            "trace contains data outside the evaluation policy scope".into(),
        ))
    }
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
    key: EvaluationKey,
    evaluation: EvaluationBundle,
}

impl<J: Journal, S: SidecarFederation> KernelService for KernelServiceCore<J, S> {
    fn append(&mut self, request: AppendRequest) -> Result<AppendResponse, ApiError> {
        let receipt = self.admit_append(AppendAdmissionRequest {
            schema_ref: None,
            expected_cut: None,
            idempotency_key: None,
            datoms: request.datoms,
            principal: None,
        })?;
        Ok(AppendResponse {
            appended: receipt.appended,
            receipt: Some(receipt),
        })
    }

    fn admit_append(&mut self, request: AppendAdmissionRequest) -> Result<AppendReceipt, ApiError> {
        if let Some(receipt) = resolve_idempotent_append(&self.journal, &request)? {
            return Ok(receipt);
        }
        let current_cut = self.journal.cut()?;
        if request.idempotency_key.is_none() {
            if let Some(expected) = &request.expected_cut {
                if expected != &current_cut {
                    return Err(
                        AdmissionError::Storage(aether_storage::JournalError::StaleCut {
                            expected: expected.clone(),
                            actual: current_cut,
                        })
                        .into(),
                    );
                }
            }
        }
        let active = match schema_catalog(&self.journal)?.active {
            Some(active) => active,
            None if request.schema_ref.is_none() => {
                ensure_legacy_schema(&mut self.journal, &request)?
            }
            None => return Err(AdmissionError::NoActiveSchema.into()),
        };
        let active = if request.schema_ref.is_none() {
            extend_legacy_schema_if_needed(&mut self.journal, active, &request)?
        } else {
            active
        };
        let history = self.journal.history()?;
        let expected = request
            .expected_cut
            .as_ref()
            .unwrap_or(&current_cut)
            .clone();
        let batch = validate_append(&history, &active, &request)?;
        Ok(commit_append(
            &mut self.journal,
            &expected,
            &request,
            batch,
            request.principal.as_deref().unwrap_or("service"),
        )?)
    }

    fn dry_run_append(
        &mut self,
        request: AppendAdmissionRequest,
    ) -> Result<AppendDryRunResponse, ApiError> {
        let active = match schema_catalog(&self.journal)?.active {
            Some(active) => active,
            None => {
                return Ok(AppendDryRunResponse {
                    valid: false,
                    schema_ref: None,
                    current_cut: Some(self.journal.cut()?),
                    batch_digest: None,
                    diagnostics: vec![AdmissionError::NoActiveSchema.to_string()],
                })
            }
        };
        let current_cut = self.journal.cut()?;
        match validate_append(&self.journal.history()?, &active, &request) {
            Ok(batch) => Ok(AppendDryRunResponse {
                valid: request
                    .expected_cut
                    .as_ref()
                    .is_none_or(|expected| expected == &current_cut),
                schema_ref: Some(batch.schema_ref().clone()),
                current_cut: Some(current_cut),
                batch_digest: Some(batch.batch_digest().clone()),
                diagnostics: Vec::new(),
            }),
            Err(error) => Ok(AppendDryRunResponse {
                valid: false,
                schema_ref: Some(active.schema_ref),
                current_cut: Some(current_cut),
                batch_digest: None,
                diagnostics: vec![error.to_string()],
            }),
        }
    }

    fn append_receipts(&self) -> Result<Vec<AppendReceipt>, ApiError> {
        Ok(list_append_receipts(&self.journal)?)
    }

    fn register_schema(
        &mut self,
        request: RegisterSchemaRequest,
    ) -> Result<NamespaceSchemaRevision, ApiError> {
        Ok(register_schema(&mut self.journal, request)?)
    }

    fn activate_schema(
        &mut self,
        request: ActivateSchemaRequest,
    ) -> Result<NamespaceSchemaRevision, ApiError> {
        Ok(activate_schema(&mut self.journal, request)?)
    }

    fn schema_catalog(&self) -> Result<SchemaCatalogResponse, ApiError> {
        Ok(schema_catalog(&self.journal)?)
    }

    fn history(&self, request: HistoryRequest) -> Result<HistoryResponse, ApiError> {
        let scope = PolicyScope::from_optional(request.policy_context);
        Ok(HistoryResponse {
            datoms: self.visible_history(&[], &scope)?,
        })
    }

    fn current_state(
        &self,
        request: CurrentStateRequest,
    ) -> Result<CurrentStateResponse, ApiError> {
        let CurrentStateRequest {
            schema,
            datoms,
            policy_context,
        } = request;
        let datoms = self.datoms_or_history(&datoms)?;
        let scope = PolicyScope::from_optional(policy_context);
        let snapshot = resolve_snapshot(&schema, &datoms, TemporalView::Current, scope)?;
        Ok(CurrentStateResponse {
            state: snapshot.into_state(),
        })
    }

    fn as_of(&self, request: AsOfRequest) -> Result<AsOfResponse, ApiError> {
        let AsOfRequest {
            schema,
            datoms,
            at,
            policy_context,
        } = request;
        let datoms = self.datoms_or_history(&datoms)?;
        let scope = PolicyScope::from_optional(policy_context);
        let snapshot = resolve_snapshot(&schema, &datoms, TemporalView::AsOf(at), scope)?;
        Ok(AsOfResponse {
            state: snapshot.into_state(),
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
            derived: filter_derived_set(&derived, request.policy_context.as_ref()),
        })
    }

    fn explain_tuple(
        &self,
        _request: ExplainTupleRequest,
    ) -> Result<ExplainTupleResponse, ApiError> {
        Err(ApiError::AmbiguousTupleReference)
    }

    fn resolve_trace_handle(
        &mut self,
        request: ResolveTraceHandleRequest,
    ) -> Result<ResolveTraceHandleResponse, ApiError> {
        Ok(resolve_trace(
            self.execution_store.as_mut(),
            &self.namespace,
            request,
        )?)
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
        self.run_document_with_limits(request, DocumentExecutionLimits::UNBOUNDED)
    }

    fn run_document_with_limits(
        &mut self,
        request: RunDocumentRequest,
        limits: DocumentExecutionLimits,
    ) -> Result<RunDocumentResponse, ApiError> {
        let RunDocumentRequest {
            dsl,
            policy_context,
        } = request;
        if dsl.len() > limits.max_document_bytes {
            return Err(ApiError::ResourceLimit {
                resource: "document_bytes",
                limit: limits.max_document_bytes,
                observed: dsl.len(),
            });
        }
        let document = DefaultDslParser.parse_document(&dsl)?;
        if document.program.rules.len() > limits.max_rules {
            return Err(ApiError::ResourceLimit {
                resource: "document_rules",
                limit: limits.max_rules,
                observed: document.program.rules.len(),
            });
        }
        if let Some(active) = schema_catalog(&self.journal)?.active {
            document_schema_compatible(&active, &document.schema)?;
        }
        let datoms = self.datoms_or_history(&[])?;
        let scope = PolicyScope::from_optional(policy_context);
        let builder = ScopedEvaluationBuilder::new_in_namespace(
            self.namespace.as_str(),
            &document.schema,
            &datoms,
            &document.program,
            scope.clone(),
        )?;
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
        let primary =
            self.document_evaluation(&mut evaluations, &builder, &primary_view, limits)?;
        let primary_key = primary.key.clone();
        let primary_state = primary.evaluation.snapshot().state().clone();
        let primary_derived = primary.evaluation.derived().clone();
        let query = match &document.query {
            Some(query) => Some(execute_scoped_query(&primary.evaluation, &query.query)?),
            None => None,
        };
        let queries = document
            .queries
            .iter()
            .map(|named_query| {
                let evaluation = self.document_evaluation(
                    &mut evaluations,
                    &builder,
                    &named_query.spec.view,
                    limits,
                )?;
                Ok(NamedQueryResult {
                    name: named_query.name.clone(),
                    spec: named_query.spec.clone(),
                    result: execute_scoped_query(&evaluation.evaluation, &named_query.spec.query)?,
                    execution_id: Some(ExecutionId(evaluation.key.to_hex())),
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        let explains = document
            .explains
            .iter()
            .map(|named_explain| {
                let evaluation = self.document_evaluation(
                    &mut evaluations,
                    &builder,
                    &named_explain.spec.view,
                    limits,
                )?;
                Ok(NamedExplainResult {
                    name: named_explain.name.clone(),
                    spec: named_explain.spec.clone(),
                    result: execute_explain_spec(evaluation, &named_explain.spec)?,
                    execution_id: Some(ExecutionId(evaluation.key.to_hex())),
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        let mut executions = Vec::with_capacity(evaluations.len());
        for evaluation in &evaluations {
            let visible_history =
                project_history_at_view(&datoms, evaluation.view.clone(), scope.clone())?;
            executions.push(persist_execution(
                self.execution_store.as_mut(),
                &self.namespace,
                &evaluation.key,
                &document.schema,
                visible_history,
                builder.program().compiled(),
                &scope,
                evaluation.view.clone(),
                evaluation.evaluation.derived(),
                None,
            )?);
        }
        let primary_execution_id = ExecutionId(primary_key.to_hex());
        let execution = executions
            .iter()
            .find(|execution| execution.manifest.execution_id == primary_execution_id)
            .cloned()
            .ok_or_else(|| {
                ApiError::Validation("primary execution receipt was not persisted".into())
            })?;
        Ok(RunDocumentResponse {
            state: primary_state,
            program: builder.program().compiled().clone(),
            derived: primary_derived,
            query,
            queries,
            explains,
            execution: Some(execution.clone()),
            executions,
        })
    }

    fn coordination_pilot_report(
        &mut self,
        request: CoordinationPilotReportRequest,
    ) -> Result<CoordinationPilotReport, ApiError> {
        build_coordination_pilot_report_with_policy(self, request.policy_context)
    }

    fn coordination_delta_report(
        &mut self,
        request: CoordinationDeltaReportRequest,
    ) -> Result<CoordinationDeltaReport, ApiError> {
        report::build_coordination_delta_report(self, request)
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
        let scope = PolicyScope::from_optional(request.policy_context.clone());
        let journal = self.scoped_sidecar_journal_catalog(scope)?;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<AppendReceipt>,
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoordinationPilotReportRequest {
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ExecutionReceipt>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub executions: Vec<ExecutionReceipt>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NamedQueryResult {
    pub name: Option<String>,
    pub spec: QuerySpec,
    pub result: QueryResult,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
}

fn execute_explain_spec(
    evaluation: &DocumentEvaluation,
    spec: &ExplainSpec,
) -> Result<ExplainArtifact, ApiError> {
    match &spec.target {
        ExplainTarget::Plan => Ok(ExplainArtifact::Plan(
            InMemoryExplainer::default()
                .explain_plan(&evaluation.evaluation.program().compiled().phase_graph)?,
        )),
        ExplainTarget::Tuple(atom) => {
            let tuple_id = find_matching_derived_tuple(evaluation.evaluation.derived(), atom)
                .ok_or_else(|| {
                    ApiError::Validation(format!(
                        "no derived tuple matched explain target {}",
                        atom.predicate.name
                    ))
                })?;
            let trace = InMemoryExplainer::from_derived_set(evaluation.evaluation.derived())
                .explain_tuple(&tuple_id)?;
            assert_trace_visible(&trace, evaluation.evaluation.scope())?;
            Ok(ExplainArtifact::Tuple(trace))
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
    #[error("bare tuple ids are execution-local; resolve an opaque trace handle instead")]
    AmbiguousTupleReference,
    #[error(transparent)]
    Admission(#[from] AdmissionError),
    #[error("validation error: {0}")]
    Validation(String),
    #[error("resource limit exceeded for {resource}: observed {observed}, limit {limit}")]
    ResourceLimit {
        resource: &'static str,
        limit: usize,
        observed: usize,
    },
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
    #[error(transparent)]
    Execution(#[from] execution::ExecutionError),
}

#[cfg(test)]
mod tests {
    use super::{
        coordination_pilot_dsl, coordination_pilot_seed_history, ApiError, AppendRequest,
        AsOfRequest, CurrentStateRequest, ExplainArtifact, InMemoryKernelService, KernelService,
        ParseDocumentRequest, ResolveTraceHandleRequest, RunDocumentRequest,
        COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT, COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT,
    };
    use crate::execution::ExecutionError;
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
        let authorized_tuple = authorized_rows[0]
            .tuple_id
            .expect("execution_authorized tuple id");
        let handle = current_authorized
            .execution
            .as_ref()
            .expect("execution receipt")
            .trace_handles
            .iter()
            .find(|binding| binding.local_tuple_id == authorized_tuple)
            .expect("authorization trace handle")
            .handle
            .clone();
        let trace = service
            .resolve_trace_handle(ResolveTraceHandleRequest {
                handle,
                policy_context: None,
                verify_replay: true,
            })
            .expect("explain authorization tuple")
            .record
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
        let protected_handle = executor_result
            .execution
            .as_ref()
            .expect("executor execution receipt")
            .trace_handles
            .iter()
            .find(|binding| binding.local_tuple_id == protected_tuple)
            .expect("protected trace handle")
            .handle
            .clone();
        let mismatch = service
            .resolve_trace_handle(ResolveTraceHandleRequest {
                handle: protected_handle.clone(),
                policy_context: None,
                verify_replay: false,
            })
            .expect_err("explain should reject mismatched policy context");
        assert!(matches!(
            mismatch,
            ApiError::Execution(ExecutionError::InsufficientPolicy)
        ));
        let executor_trace = service
            .resolve_trace_handle(ResolveTraceHandleRequest {
                handle: protected_handle,
                policy_context: Some(PolicyContext {
                    capabilities: vec!["executor".into()],
                    visibilities: Vec::new(),
                }),
                verify_replay: true,
            })
            .expect("explain protected tuple with matching policy")
            .record
            .trace;
        assert!(!executor_trace.tuples.is_empty());
    }

    #[test]
    fn service_rejects_hidden_as_of_cuts_under_policy() {
        let mut service = InMemoryKernelService::new();
        let parsed = service
            .parse_document(ParseDocumentRequest {
                dsl: transitive_document_dsl(),
            })
            .expect("parse transitive document");
        service
            .append(AppendRequest {
                datoms: vec![dependency_datom(1, 2, 1), {
                    let mut datom = dependency_datom(2, 3, 2);
                    datom.policy = Some(PolicyEnvelope {
                        capabilities: vec!["executor".into()],
                        visibilities: Vec::new(),
                    });
                    datom
                }],
            })
            .expect("append mixed-visibility chain");

        let hidden_as_of = service.as_of(AsOfRequest {
            schema: parsed.schema.clone(),
            datoms: Vec::new(),
            at: ElementId::new(2),
            policy_context: None,
        });
        assert!(matches!(
            hidden_as_of,
            Err(ApiError::Validation(message)) if message == "unknown element 2"
        ));

        let visible_as_of = service
            .as_of(AsOfRequest {
                schema: parsed.schema,
                datoms: Vec::new(),
                at: ElementId::new(2),
                policy_context: Some(PolicyContext {
                    capabilities: vec!["executor".into()],
                    visibilities: Vec::new(),
                }),
            })
            .expect("authorized as_of should succeed");
        assert_eq!(visible_as_of.state.as_of, Some(ElementId::new(2)));
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
