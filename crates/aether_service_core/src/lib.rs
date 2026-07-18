use aether_ast::{
    Datom, DerivationTrace, ElementId, ExplainSpec, ExplainTarget, NamedExplainSpec,
    NamedQuerySpec, PhaseGraph, PlanExplanation, PolicyContext, PolicyScope, QueryResult,
    QuerySpec, RuleProgram, TemporalView, Term, TupleId,
};
use aether_explain::{ExplainError, Explainer, InMemoryExplainer};
use aether_plan::CompiledProgram;
use aether_resolver::{ResolveError, ResolvedState};
use aether_rules::{DefaultDslParser, DefaultRuleCompiler, DslParser, ParseError, RuleCompiler};
use aether_runtime::{
    execute_scoped_query, DerivedSet, EvaluationBundle, RuntimeError, RuntimeLimits,
};
use aether_schema::Schema;
use aether_storage::{InMemoryJournal, Journal, JournalError, PostgresJournal, SqliteJournal};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Instant;
use thiserror::Error;

pub const ENGINE_SEMANTICS_VERSION: &str = "aether-semantic-v1-policy-scope-1";

pub mod admission;
#[doc(hidden)]
pub mod evaluation;
pub mod execution;
pub mod namespace;
pub mod sidecar {
    pub use aether_sidecar::*;
}

#[doc(hidden)]
pub mod diagnostics {
    use serde::{Deserialize, Serialize};
    use std::time::Instant;

    #[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
    pub struct PhaseTiming {
        pub phase: String,
        pub duration_ns: u64,
    }

    #[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
    pub struct ServiceOperationTiming {
        pub total_duration_ns: u64,
        pub phases: Vec<PhaseTiming>,
    }

    pub(crate) fn elapsed_ns(started: Instant) -> u64 {
        u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    pub(crate) fn record_phase(
        phases: &mut Vec<PhaseTiming>,
        phase: impl Into<String>,
        started: Instant,
    ) {
        phases.push(PhaseTiming {
            phase: phase.into(),
            duration_ns: elapsed_ns(started),
        });
    }
}

fn record_optional_phase(
    phases: &mut Option<&mut Vec<diagnostics::PhaseTiming>>,
    phase: &'static str,
    started: Instant,
) {
    if let Some(phases) = phases.as_deref_mut() {
        diagnostics::record_phase(phases, phase, started);
    }
}

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

pub use admission::{
    ActivateSchemaRequest, AppendAdmissionRequest, AppendDryRunResponse, AppendReceipt, BatchId,
    HistoryCertificationStatus, NamespaceSchemaRevision, RegisterSchemaRequest,
    SchemaBaselineReceipt, SchemaCatalogResponse, SchemaCompatibility, SchemaStatus,
};
pub use aether_sidecar::{
    ArtifactReference, GetArtifactReferenceRequest, GetArtifactReferenceResponse,
    InMemorySidecarFederation, JournalCatalog, RegisterArtifactReferenceRequest,
    RegisterArtifactReferenceResponse, RegisterVectorRecordRequest, RegisterVectorRecordResponse,
    SearchVectorsRequest, SearchVectorsResponse, SidecarError, SidecarFederation,
    SqliteSidecarFederation, VectorFactProjection, VectorMetric, VectorRecordMetadata,
    VectorSearchMatch,
};
pub use aether_storage::{JournalCutRef, PostgresTlsConfig, PostgresTlsMode};
pub use execution::{
    ContentDigest, ExecutionId, ExecutionManifest, ExecutionReceipt, FederatedExecutionSource,
    FederationManifest, JournalCut, ResolveTraceHandleRequest, ResolveTraceHandleResponse,
    SchemaRef, TraceHandle, TraceHandleBinding, TraceRecord, DEFAULT_EXECUTION_RETENTION,
    DEFAULT_TRACE_TOMBSTONE_RETENTION,
};
pub use namespace::NamespaceId;

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

    #[doc(hidden)]
    pub fn open_with_diagnostics(
        path: impl AsRef<Path>,
    ) -> Result<(Self, diagnostics::ServiceOperationTiming), ApiError> {
        let total_started = Instant::now();
        let path = path.as_ref();
        let mut phases = Vec::with_capacity(3);

        let started = Instant::now();
        let execution_store = SqliteExecutionStore::open(execution_catalog_path_for_journal(path))
            .map_err(ExecutionError::from)?;
        diagnostics::record_phase(&mut phases, "execution_store_open_schema", started);

        let started = Instant::now();
        let journal = SqliteJournal::open(path)?;
        diagnostics::record_phase(&mut phases, "journal_open_configure_schema", started);

        let started = Instant::now();
        let sidecars =
            SqliteSidecarFederation::open(sidecar::sidecar_catalog_path_for_journal(path))?;
        diagnostics::record_phase(&mut phases, "sidecar_open_schema", started);

        let service = Self::from_parts_with_execution_store(
            journal,
            sidecars,
            NamespaceId::default(),
            Box::new(execution_store),
        );
        Ok((
            service,
            diagnostics::ServiceOperationTiming {
                total_duration_ns: diagnostics::elapsed_ns(total_started),
                phases,
            },
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

    #[doc(hidden)]
    pub fn replicate_admitted_append(
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

    #[doc(hidden)]
    pub fn authority_history(&self) -> Result<Vec<Datom>, ApiError> {
        Ok(self.journal.history()?)
    }

    #[doc(hidden)]
    pub fn authority_append_receipts(&self) -> Result<Vec<AppendReceipt>, ApiError> {
        Ok(list_append_receipts(&self.journal)?)
    }

    #[doc(hidden)]
    pub fn authority_schema_catalog(&self) -> Result<SchemaCatalogResponse, ApiError> {
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
        phases: Option<&mut Vec<diagnostics::PhaseTiming>>,
    ) -> Result<&'a DocumentEvaluation, ApiError> {
        if let Some(index) = cache.iter().position(|evaluation| &evaluation.view == view) {
            return Ok(&cache[index]);
        }

        let runtime_limits = RuntimeLimits {
            max_iterations: limits.max_iterations,
            max_derived_tuples: limits.max_derived_tuples,
        };
        let (key, evaluation) = match phases {
            Some(phases) => builder.evaluate_with_key_and_limits_diagnostics(
                view.clone(),
                runtime_limits,
                phases,
            )?,
            None => builder.evaluate_with_key_and_limits(view.clone(), runtime_limits)?,
        };
        cache.push(DocumentEvaluation {
            view: view.clone(),
            key,
            evaluation,
        });
        Ok(cache
            .last()
            .expect("evaluation cache contains the inserted view"))
    }

    #[doc(hidden)]
    pub fn run_document_with_diagnostics(
        &mut self,
        request: RunDocumentRequest,
    ) -> Result<(RunDocumentResponse, diagnostics::ServiceOperationTiming), ApiError> {
        let total_started = Instant::now();
        let mut phases = Vec::with_capacity(12);
        let response = self.run_document_with_limits_observed(
            request,
            DocumentExecutionLimits::UNBOUNDED,
            Some(&mut phases),
        )?;
        Ok((
            response,
            diagnostics::ServiceOperationTiming {
                total_duration_ns: diagnostics::elapsed_ns(total_started),
                phases,
            },
        ))
    }

    fn run_document_with_limits_observed(
        &mut self,
        request: RunDocumentRequest,
        limits: DocumentExecutionLimits,
        mut phases: Option<&mut Vec<diagnostics::PhaseTiming>>,
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

        let started = Instant::now();
        let document = DefaultDslParser.parse_document(&dsl)?;
        record_optional_phase(&mut phases, "document_parse", started);
        if document.program.rules.len() > limits.max_rules {
            return Err(ApiError::ResourceLimit {
                resource: "document_rules",
                limit: limits.max_rules,
                observed: document.program.rules.len(),
            });
        }

        let started = Instant::now();
        if let Some(active) = schema_catalog(&self.journal)?.active {
            document_schema_compatible(&active, &document.schema)?;
        }
        record_optional_phase(&mut phases, "schema_catalog_validation", started);

        let started = Instant::now();
        let datoms = self.datoms_or_history(&[])?;
        record_optional_phase(&mut phases, "journal_history_read", started);

        let scope = PolicyScope::from_optional(policy_context);
        let started = Instant::now();
        let builder = ScopedEvaluationBuilder::new_in_namespace(
            self.namespace.as_str(),
            &document.schema,
            &datoms,
            &document.program,
            scope.clone(),
        )?;
        record_optional_phase(&mut phases, "program_compile", started);

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
            &builder,
            &primary_view,
            limits,
            phases.as_deref_mut(),
        )?;
        let primary_key = primary.key.clone();
        let primary_state = primary.evaluation.snapshot().state().clone();
        let primary_derived = primary.evaluation.derived().clone();

        let started = Instant::now();
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
                    None,
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
                    None,
                )?;
                Ok(NamedExplainResult {
                    name: named_explain.name.clone(),
                    spec: named_explain.spec.clone(),
                    result: execute_explain_spec(evaluation, &named_explain.spec)?,
                    execution_id: Some(ExecutionId(evaluation.key.to_hex())),
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        record_optional_phase(&mut phases, "query_and_explain", started);

        let started = Instant::now();
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
        record_optional_phase(&mut phases, "execution_persistence", started);

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
        let EvaluateProgramRequest {
            schema,
            datoms,
            view,
            program,
            policy_context,
        } = request;
        let datoms = match datoms {
            Some(datoms) => datoms,
            None => self.journal.history()?,
        };
        let scope = PolicyScope::from_optional(policy_context);
        let builder = ScopedEvaluationBuilder::new_in_namespace(
            self.namespace.as_str(),
            &schema,
            &datoms,
            &program,
            scope,
        )?;
        let (_, evaluation) = builder.evaluate_with_key(view)?;
        Ok(EvaluateProgramResponse {
            derived: evaluation.derived().clone(),
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
        self.run_document_with_limits_observed(request, limits, None)
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
    pub schema: Schema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datoms: Option<Vec<Datom>>,
    #[serde(default)]
    pub view: TemporalView,
    pub program: RuleProgram,
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
