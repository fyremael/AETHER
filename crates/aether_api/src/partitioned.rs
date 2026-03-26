use crate::{
    ApiError, AppendRequest, CurrentStateRequest, ExplainArtifact, HistoryRequest,
    InMemoryKernelService, KernelService, NamedExplainResult, NamedQueryResult,
    ParseDocumentRequest, RunDocumentRequest, RunDocumentResponse,
};
use aether_ast::{
    merge_partition_cuts, policy_allows, Atom, Datom, ExplainSpec, ExplainTarget, ExtensionalFact,
    FactProvenance, FederatedCut, PartitionCut, PartitionId, PolicyContext, PredicateRef,
    QueryResult, QueryRow, SourceRef, TemporalView, TupleId, Value,
};
use aether_explain::{Explainer, InMemoryExplainer};
use aether_plan::CompiledProgram;
use aether_resolver::ResolvedState;
use aether_runtime::{execute_query, DerivedSet};
use aether_schema::{PredicateSignature, Schema, ValueType};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Default)]
pub struct PartitionedInMemoryKernelService {
    partitions: IndexMap<PartitionId, InMemoryKernelService>,
}

#[derive(Debug)]
pub struct SqlitePartitionedKernelService {
    root: PathBuf,
    partitions: RefCell<IndexMap<PartitionId, crate::SqliteKernelService>>,
}

impl PartitionedInMemoryKernelService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn append_partition(
        &mut self,
        request: PartitionAppendRequest,
    ) -> Result<PartitionAppendResponse, ApiError> {
        let PartitionAppendRequest { partition, datoms } = request;
        let response = self
            .partitions
            .entry(partition.clone())
            .or_default()
            .append(AppendRequest { datoms })?;
        Ok(PartitionAppendResponse {
            partition,
            appended: response.appended,
        })
    }

    pub fn partition_history(
        &self,
        request: PartitionHistoryRequest,
    ) -> Result<PartitionHistoryResponse, ApiError> {
        partition_history_for(self.partition_service(&request.cut.partition)?, request)
    }

    pub fn partition_state(
        &self,
        request: PartitionStateRequest,
    ) -> Result<PartitionStateResponse, ApiError> {
        partition_state_for(self.partition_service(&request.cut.partition)?, request)
    }

    pub fn federated_history(
        &self,
        request: FederatedHistoryRequest,
    ) -> Result<FederatedHistoryResponse, ApiError> {
        let cut = validate_federated_cut(request.cut)?;
        let mut partitions = Vec::with_capacity(cut.cuts.len());
        for partition_cut in cut.cuts {
            partitions.push(self.partition_history(PartitionHistoryRequest {
                cut: partition_cut,
                policy_context: request.policy_context.clone(),
            })?);
        }
        Ok(FederatedHistoryResponse { partitions })
    }

    pub fn import_partition_facts(
        &mut self,
        request: ImportedFactQueryRequest,
        policy_context: Option<PolicyContext>,
    ) -> Result<ImportedFactQueryResponse, ApiError> {
        import_partition_facts_from_service(
            self.partition_service_mut(&request.cut.partition)?,
            request,
            policy_context,
        )
    }

    pub fn federated_run_document(
        &mut self,
        request: FederatedRunDocumentRequest,
    ) -> Result<FederatedRunDocumentResponse, ApiError> {
        let imports = request
            .imports
            .iter()
            .cloned()
            .map(|import| self.import_partition_facts(import, request.policy_context.clone()))
            .collect::<Result<Vec<_>, ApiError>>()?;
        execute_federated_document_request(request, imports)
    }

    pub fn build_federated_explain_report(
        &mut self,
        request: FederatedRunDocumentRequest,
    ) -> Result<FederatedExplainReport, ApiError> {
        let policy_context = request.policy_context.clone();
        let response = self.federated_run_document(request)?;
        Ok(build_federated_explain_report_from_response(
            response,
            policy_context,
        ))
    }

    fn partition_service(
        &self,
        partition: &PartitionId,
    ) -> Result<&InMemoryKernelService, ApiError> {
        self.partitions
            .get(partition)
            .ok_or_else(|| ApiError::Validation(format!("unknown partition {}", partition)))
    }

    fn partition_service_mut(
        &mut self,
        partition: &PartitionId,
    ) -> Result<&mut InMemoryKernelService, ApiError> {
        self.partitions
            .get_mut(partition)
            .ok_or_else(|| ApiError::Validation(format!("unknown partition {}", partition)))
    }
}

impl SqlitePartitionedKernelService {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, ApiError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|error| {
            ApiError::Validation(format!(
                "failed to create partition root {}: {}",
                root.display(),
                error
            ))
        })?;
        Ok(Self {
            root,
            partitions: RefCell::new(IndexMap::new()),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn append_partition(
        &mut self,
        request: PartitionAppendRequest,
    ) -> Result<PartitionAppendResponse, ApiError> {
        let PartitionAppendRequest { partition, datoms } = request;
        self.ensure_partition_open(&partition, true)?;
        let mut partitions = self.partitions.borrow_mut();
        let response = partitions
            .get_mut(&partition)
            .expect("partition should be open")
            .append(AppendRequest { datoms })?;
        Ok(PartitionAppendResponse {
            partition,
            appended: response.appended,
        })
    }

    pub fn partition_history(
        &self,
        request: PartitionHistoryRequest,
    ) -> Result<PartitionHistoryResponse, ApiError> {
        self.ensure_partition_open(&request.cut.partition, false)?;
        let mut partitions = self.partitions.borrow_mut();
        let service = partitions
            .get_mut(&request.cut.partition)
            .expect("partition should be open");
        partition_history_for(service, request)
    }

    pub fn partition_state(
        &self,
        request: PartitionStateRequest,
    ) -> Result<PartitionStateResponse, ApiError> {
        self.ensure_partition_open(&request.cut.partition, false)?;
        let mut partitions = self.partitions.borrow_mut();
        let service = partitions
            .get_mut(&request.cut.partition)
            .expect("partition should be open");
        partition_state_for(service, request)
    }

    pub fn federated_history(
        &self,
        request: FederatedHistoryRequest,
    ) -> Result<FederatedHistoryResponse, ApiError> {
        let cut = validate_federated_cut(request.cut)?;
        let mut partitions = Vec::with_capacity(cut.cuts.len());
        for partition_cut in cut.cuts {
            partitions.push(self.partition_history(PartitionHistoryRequest {
                cut: partition_cut,
                policy_context: request.policy_context.clone(),
            })?);
        }
        Ok(FederatedHistoryResponse { partitions })
    }

    pub fn import_partition_facts(
        &mut self,
        request: ImportedFactQueryRequest,
        policy_context: Option<PolicyContext>,
    ) -> Result<ImportedFactQueryResponse, ApiError> {
        self.ensure_partition_open(&request.cut.partition, false)?;
        let mut partitions = self.partitions.borrow_mut();
        let service = partitions
            .get_mut(&request.cut.partition)
            .expect("partition should be open");
        import_partition_facts_from_service(service, request, policy_context)
    }

    pub fn federated_run_document(
        &mut self,
        request: FederatedRunDocumentRequest,
    ) -> Result<FederatedRunDocumentResponse, ApiError> {
        let imports = request
            .imports
            .iter()
            .cloned()
            .map(|import| self.import_partition_facts(import, request.policy_context.clone()))
            .collect::<Result<Vec<_>, ApiError>>()?;
        execute_federated_document_request(request, imports)
    }

    pub fn build_federated_explain_report(
        &mut self,
        request: FederatedRunDocumentRequest,
    ) -> Result<FederatedExplainReport, ApiError> {
        let policy_context = request.policy_context.clone();
        let response = self.federated_run_document(request)?;
        Ok(build_federated_explain_report_from_response(
            response,
            policy_context,
        ))
    }

    fn ensure_partition_open(
        &self,
        partition: &PartitionId,
        create_if_missing: bool,
    ) -> Result<(), ApiError> {
        if self.partitions.borrow().contains_key(partition) {
            return Ok(());
        }

        let path = self.partition_path(partition);
        if !create_if_missing && !path.exists() {
            return Err(ApiError::Validation(format!(
                "unknown partition {}",
                partition
            )));
        }

        let service = crate::SqliteKernelService::open(&path)?;
        self.partitions
            .borrow_mut()
            .insert(partition.clone(), service);
        Ok(())
    }

    fn partition_path(&self, partition: &PartitionId) -> PathBuf {
        self.root.join(format!(
            "partition-{}.sqlite",
            encode_partition_id(partition)
        ))
    }
}

fn validate_federated_cut(cut: FederatedCut) -> Result<FederatedCut, ApiError> {
    let normalized = cut.normalized();
    for pair in normalized.cuts.windows(2) {
        if pair[0].partition == pair[1].partition {
            return Err(ApiError::Validation(format!(
                "federated cut contains duplicate partition {}",
                pair[0].partition
            )));
        }
    }
    Ok(normalized)
}

fn filter_partition_datoms(
    datoms: Vec<Datom>,
    policy_context: Option<&PolicyContext>,
) -> Vec<Datom> {
    datoms
        .into_iter()
        .filter(|datom| aether_ast::policy_allows(policy_context, datom.policy.as_ref()))
        .collect()
}

fn partition_history_for(
    service: &dyn KernelService,
    request: PartitionHistoryRequest,
) -> Result<PartitionHistoryResponse, ApiError> {
    let datoms = match request.cut.as_of {
        Some(element) => {
            let full_history = service
                .history(HistoryRequest {
                    policy_context: None,
                })?
                .datoms;
            let end = full_history
                .iter()
                .position(|datom| datom.element == element)
                .ok_or_else(|| {
                    ApiError::Validation(format!(
                        "unknown element {} for partition {}",
                        element, request.cut.partition
                    ))
                })?;
            filter_partition_datoms(
                full_history[..=end].to_vec(),
                request.policy_context.as_ref(),
            )
        }
        None => {
            service
                .history(HistoryRequest {
                    policy_context: request.policy_context.clone(),
                })?
                .datoms
        }
    };

    Ok(PartitionHistoryResponse {
        cut: request.cut,
        datoms,
    })
}

fn partition_state_for(
    service: &dyn KernelService,
    request: PartitionStateRequest,
) -> Result<PartitionStateResponse, ApiError> {
    let state = match request.cut.as_of {
        Some(element) => {
            service
                .as_of(crate::AsOfRequest {
                    schema: request.schema,
                    datoms: Vec::new(),
                    at: element,
                    policy_context: request.policy_context,
                })?
                .state
        }
        None => {
            service
                .current_state(CurrentStateRequest {
                    schema: request.schema,
                    datoms: Vec::new(),
                    policy_context: request.policy_context,
                })?
                .state
        }
    };

    Ok(PartitionStateResponse {
        cut: request.cut,
        state,
    })
}

fn import_partition_facts_from_service(
    service: &mut dyn KernelService,
    request: ImportedFactQueryRequest,
    policy_context: Option<PolicyContext>,
) -> Result<ImportedFactQueryResponse, ApiError> {
    let response = service.run_document(RunDocumentRequest {
        dsl: request.dsl.clone(),
        policy_context,
    })?;
    let result = select_query_result(&response, request.query_name.as_deref())?;
    let tuple_index = response
        .derived
        .tuples
        .iter()
        .map(|tuple| (tuple.tuple.id, tuple))
        .collect::<IndexMap<_, _>>();

    let facts = result
        .rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            build_imported_fact(&request, index, row, &tuple_index, &response.derived)
        })
        .collect::<Result<Vec<_>, ApiError>>()?;

    Ok(ImportedFactQueryResponse {
        cut: request.cut,
        predicate: request.predicate,
        query_name: request.query_name,
        row_count: result.rows.len(),
        facts,
    })
}

fn execute_federated_document_request(
    request: FederatedRunDocumentRequest,
    imports: Vec<ImportedFactQueryResponse>,
) -> Result<FederatedRunDocumentResponse, ApiError> {
    let cut = federated_cut_from_imports(&imports)?;

    let mut local = InMemoryKernelService::new();
    let parsed = local.parse_document(ParseDocumentRequest {
        dsl: request.dsl.clone(),
    })?;
    ensure_federated_document_uses_current_views(&parsed)?;

    let mut schema = parsed.schema.clone();
    let mut program = parsed.program.clone();
    let predicate_lookup = parsed
        .program
        .predicates
        .iter()
        .map(|predicate| (predicate.name.clone(), predicate.clone()))
        .collect::<IndexMap<_, _>>();
    for import in &imports {
        for fact in &import.facts {
            let mut fact = fact.clone();
            let predicate = predicate_lookup
                .get(&fact.predicate.name)
                .ok_or_else(|| {
                    ApiError::Validation(format!(
                        "federated document does not declare imported predicate {}",
                        fact.predicate.name
                    ))
                })?
                .clone();
            if predicate.arity != fact.values.len() {
                return Err(ApiError::Validation(format!(
                    "federated document predicate {} expects arity {}, but imported fact supplied {} value(s)",
                    predicate.name,
                    predicate.arity,
                    fact.values.len()
                )));
            }
            fact.predicate = predicate;
            program.facts.push(fact);
        }
    }
    ensure_schema_covers_fact_predicates(&mut schema, &program.facts)?;

    let compiled = local.compile_program(crate::CompileProgramRequest {
        schema: schema.clone(),
        program,
    })?;
    let derived = local
        .evaluate_program(crate::EvaluateProgramRequest {
            state: ResolvedState::default(),
            program: compiled.program.clone(),
            policy_context: request.policy_context.clone(),
        })?
        .derived;
    let visible_program =
        filter_compiled_program_facts(&compiled.program, request.policy_context.as_ref());
    let visible_derived = filter_derived_set(&derived, request.policy_context.as_ref());

    let query = match &parsed.query {
        Some(query) => Some(execute_query(
            &ResolvedState::default(),
            &visible_program,
            &visible_derived,
            &query.query,
            request.policy_context.as_ref(),
        )?),
        None => None,
    };
    let queries = parsed
        .queries
        .iter()
        .map(|named_query| {
            Ok(NamedQueryResult {
                name: named_query.name.clone(),
                spec: named_query.spec.clone(),
                result: execute_query(
                    &ResolvedState::default(),
                    &visible_program,
                    &visible_derived,
                    &named_query.spec.query,
                    request.policy_context.as_ref(),
                )?,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let explains = parsed
        .explains
        .iter()
        .map(|named_explain| {
            Ok(NamedExplainResult {
                name: named_explain.name.clone(),
                spec: named_explain.spec.clone(),
                result: execute_federated_explain_spec(
                    &visible_program,
                    &derived,
                    &visible_derived,
                    &named_explain.spec,
                    request.policy_context.as_ref(),
                )?,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;

    Ok(FederatedRunDocumentResponse {
        cut,
        imports,
        run: RunDocumentResponse {
            state: ResolvedState::default(),
            program: visible_program,
            derived: visible_derived,
            query,
            queries,
            explains,
        },
    })
}

fn build_federated_explain_report_from_response(
    response: FederatedRunDocumentResponse,
    policy_context: Option<PolicyContext>,
) -> FederatedExplainReport {
    let primary_query = response
        .run
        .query
        .as_ref()
        .map(report_rows)
        .unwrap_or_default();
    let named_queries = response
        .run
        .queries
        .iter()
        .map(|query| FederatedNamedQuerySummary {
            name: query.name.clone(),
            rows: report_rows(&query.result),
        })
        .collect::<Vec<_>>();
    let traces = response
        .run
        .explains
        .iter()
        .filter_map(|explain| match &explain.result {
            ExplainArtifact::Tuple(trace) => Some(build_trace_summary(explain.name.clone(), trace)),
            ExplainArtifact::Plan(_) => None,
        })
        .collect::<Vec<_>>();

    FederatedExplainReport {
        generated_at_ms: now_millis(),
        cut: response.cut,
        policy_context,
        imports: response
            .imports
            .iter()
            .map(|import| FederatedImportedSourceSummary {
                cut: import.cut.clone(),
                predicate: import.predicate.clone(),
                query_name: import.query_name.clone(),
                fact_count: import.facts.len(),
            })
            .collect(),
        primary_query,
        named_queries,
        traces,
    }
}

fn encode_partition_id(partition: &PartitionId) -> String {
    partition
        .as_str()
        .as_bytes()
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<String>()
}

fn select_query_result<'a>(
    response: &'a RunDocumentResponse,
    query_name: Option<&str>,
) -> Result<&'a QueryResult, ApiError> {
    match query_name {
        Some(name) => response
            .queries
            .iter()
            .find(|query| query.name.as_deref() == Some(name))
            .map(|query| &query.result)
            .ok_or_else(|| ApiError::Validation(format!("unknown named query {}", name))),
        None => response
            .query
            .as_ref()
            .ok_or_else(|| ApiError::Validation("document did not produce a primary query".into())),
    }
}

fn build_imported_fact(
    request: &ImportedFactQueryRequest,
    index: usize,
    row: &QueryRow,
    tuples: &IndexMap<TupleId, &aether_ast::DerivedTuple>,
    derived: &DerivedSet,
) -> Result<ExtensionalFact, ApiError> {
    if row.values.len() != request.predicate.arity {
        return Err(ApiError::Validation(format!(
            "imported fact row {} from {} produced {} value(s), but predicate {} expects arity {}",
            index,
            request.cut,
            row.values.len(),
            request.predicate.name,
            request.predicate.arity
        )));
    }
    let tuple_id = row.tuple_id.ok_or_else(|| {
        ApiError::Validation(format!(
            "imported fact row {} from {} was not backed by a derived tuple; import a tuple-producing query instead",
            index, request.cut
        ))
    })?;
    let tuple = tuples.get(&tuple_id).copied().ok_or_else(|| {
        ApiError::Validation(format!(
            "imported fact row {} from {} referenced missing tuple t{}",
            index, request.cut, tuple_id.0
        ))
    })?;
    let imported_cuts = merge_partition_cuts(
        std::iter::once(&request.cut).chain(tuple.metadata.imported_cuts.iter()),
    );
    let policy = tuple.policy.clone();

    if !derived
        .tuples
        .iter()
        .any(|candidate| candidate.tuple.id == tuple_id)
    {
        return Err(ApiError::Validation(format!(
            "imported fact row {} from {} referenced tuple t{} outside the derived set",
            index, request.cut, tuple_id.0
        )));
    }

    Ok(ExtensionalFact {
        predicate: request.predicate.clone(),
        values: row.values.clone(),
        policy,
        provenance: Some(FactProvenance {
            source_datom_ids: tuple.metadata.source_datom_ids.clone(),
            imported_cuts,
            sidecar_origin: None,
            source_ref: Some(SourceRef {
                uri: format!(
                    "aether://partition/{}/tuple/t{}",
                    request.cut.partition, tuple_id.0
                ),
                digest: None,
            }),
        }),
    })
}

fn federated_cut_from_imports(
    imports: &[ImportedFactQueryResponse],
) -> Result<FederatedCut, ApiError> {
    let mut by_partition = IndexMap::<PartitionId, PartitionCut>::new();
    for import in imports {
        match by_partition.get(&import.cut.partition) {
            Some(existing) if existing != &import.cut => {
                return Err(ApiError::Validation(format!(
                    "federated imports contain conflicting cuts for partition {}",
                    import.cut.partition
                )));
            }
            Some(_) => {}
            None => {
                by_partition.insert(import.cut.partition.clone(), import.cut.clone());
            }
        }
    }
    validate_federated_cut(FederatedCut {
        cuts: by_partition.into_values().collect(),
    })
}

fn ensure_federated_document_uses_current_views(
    parsed: &crate::ParseDocumentResponse,
) -> Result<(), ApiError> {
    if let Some(query) = &parsed.query {
        ensure_current_view("primary query", &query.view)?;
    }
    for named_query in &parsed.queries {
        ensure_current_view(
            named_query
                .name
                .as_deref()
                .map(|name| format!("query {}", name))
                .unwrap_or_else(|| "query".into())
                .as_str(),
            &named_query.spec.view,
        )?;
    }
    for named_explain in &parsed.explains {
        ensure_current_view(
            named_explain
                .name
                .as_deref()
                .map(|name| format!("explain {}", name))
                .unwrap_or_else(|| "explain".into())
                .as_str(),
            &named_explain.spec.view,
        )?;
    }
    Ok(())
}

fn ensure_current_view(label: &str, view: &TemporalView) -> Result<(), ApiError> {
    match view {
        TemporalView::Current => Ok(()),
        TemporalView::AsOf(element) => Err(ApiError::Validation(format!(
            "{label} cannot use AsOf(e{}); federated time must be expressed through explicit partition cuts",
            element.0
        ))),
    }
}

fn ensure_schema_covers_fact_predicates(
    schema: &mut Schema,
    facts: &[ExtensionalFact],
) -> Result<(), ApiError> {
    let mut signatures = IndexMap::<aether_ast::PredicateId, PredicateSignature>::new();
    for fact in facts {
        let fields = fact.values.iter().map(value_type_for).collect::<Vec<_>>();
        match signatures.get(&fact.predicate.id) {
            Some(existing) if existing.fields != fields => {
                return Err(ApiError::Validation(format!(
                    "imported predicate {} has inconsistent field types across federated facts",
                    fact.predicate.name
                )));
            }
            Some(_) => {}
            None => {
                signatures.insert(
                    fact.predicate.id,
                    PredicateSignature {
                        id: fact.predicate.id,
                        name: fact.predicate.name.clone(),
                        fields,
                    },
                );
            }
        }
    }

    for signature in signatures.into_values() {
        if schema.predicate(&signature.id).is_none() {
            schema
                .register_predicate(signature)
                .map_err(|error| ApiError::Validation(error.to_string()))?;
        }
    }
    Ok(())
}

fn value_type_for(value: &Value) -> ValueType {
    match value {
        Value::Null => ValueType::String,
        Value::Bool(_) => ValueType::Bool,
        Value::I64(_) => ValueType::I64,
        Value::U64(_) => ValueType::U64,
        Value::F64(_) => ValueType::F64,
        Value::String(_) => ValueType::String,
        Value::Bytes(_) => ValueType::Bytes,
        Value::Entity(_) => ValueType::Entity,
        Value::List(values) => ValueType::List(Box::new(
            values
                .first()
                .map(value_type_for)
                .unwrap_or(ValueType::String),
        )),
    }
}

fn filter_compiled_program_facts(
    program: &CompiledProgram,
    policy_context: Option<&PolicyContext>,
) -> CompiledProgram {
    let mut filtered = program.clone();
    filtered
        .facts
        .retain(|fact| policy_allows(policy_context, fact.policy.as_ref()));
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
        .collect::<Vec<_>>();
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
        .collect::<IndexMap<_, _>>();

    DerivedSet {
        tuples,
        iterations: derived.iterations.clone(),
        predicate_index,
    }
}

fn execute_federated_explain_spec(
    program: &CompiledProgram,
    derived: &DerivedSet,
    visible_derived: &DerivedSet,
    spec: &ExplainSpec,
    policy_context: Option<&PolicyContext>,
) -> Result<ExplainArtifact, ApiError> {
    match &spec.target {
        ExplainTarget::Plan => Ok(ExplainArtifact::Plan(
            InMemoryExplainer::default().explain_plan(&program.phase_graph)?,
        )),
        ExplainTarget::Tuple(atom) => {
            let tuple_id = find_matching_derived_tuple(visible_derived, atom).ok_or_else(|| {
                ApiError::Validation(format!(
                    "no derived tuple matched explain target {}",
                    atom.predicate.name
                ))
            })?;
            let trace = InMemoryExplainer::from_derived_set(derived).explain_tuple(&tuple_id)?;
            Ok(ExplainArtifact::Tuple(filter_trace(trace, policy_context)?))
        }
    }
}

fn find_matching_derived_tuple(derived: &DerivedSet, atom: &Atom) -> Option<TupleId> {
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
            .all(|(term, value)| matches_term(term, value));
        matches.then_some(tuple.tuple.id)
    })
}

fn matches_term(term: &aether_ast::Term, value: &Value) -> bool {
    match term {
        aether_ast::Term::Value(expected) => expected == value,
        aether_ast::Term::Variable(_) => true,
        aether_ast::Term::Aggregate(_) => false,
    }
}

fn filter_trace(
    trace: aether_ast::DerivationTrace,
    policy_context: Option<&PolicyContext>,
) -> Result<aether_ast::DerivationTrace, ApiError> {
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
    Ok(aether_ast::DerivationTrace {
        root: trace.root,
        tuples,
    })
}

fn report_rows(result: &QueryResult) -> Vec<FederatedReportRow> {
    result
        .rows
        .iter()
        .map(|row| FederatedReportRow {
            tuple_id: row.tuple_id,
            values: row.values.clone(),
        })
        .collect()
}

fn build_trace_summary(
    name: Option<String>,
    trace: &aether_ast::DerivationTrace,
) -> FederatedTraceSummary {
    let imported_cuts = merge_partition_cuts(
        trace
            .tuples
            .iter()
            .flat_map(|tuple| tuple.metadata.imported_cuts.iter()),
    );
    FederatedTraceSummary {
        name,
        root: trace.root,
        tuple_count: trace.tuples.len(),
        imported_cuts,
        tuples: trace
            .tuples
            .iter()
            .map(|tuple| FederatedTraceTupleSummary {
                tuple_id: tuple.tuple.id,
                values: tuple.tuple.values.clone(),
                iteration: tuple.metadata.iteration,
                source_datom_ids: tuple.metadata.source_datom_ids.clone(),
                imported_cuts: tuple.metadata.imported_cuts.clone(),
                parent_tuple_ids: tuple.metadata.parent_tuple_ids.clone(),
            })
            .collect(),
    }
}

pub fn render_federated_explain_report_markdown(report: &FederatedExplainReport) -> String {
    let mut markdown = String::new();
    markdown.push_str("# Federated Explain Report\n\n");
    markdown.push_str(&format!(
        "- Generated at (ms): `{}`\n- Federated cut: `{}`\n- Effective policy: `{}`\n\n",
        report.generated_at_ms,
        format_federated_cut(&report.cut),
        format_policy_context(report.policy_context.as_ref())
    ));

    markdown.push_str("## Imported Sources\n\n");
    if report.imports.is_empty() {
        markdown.push_str("_None._\n\n");
    } else {
        for import in &report.imports {
            markdown.push_str(&format!(
                "- `{}` -> `{}` | query `{}` | facts `{}`\n",
                import.cut,
                import.predicate.name,
                import.query_name.as_deref().unwrap_or("<primary>"),
                import.fact_count
            ));
        }
        markdown.push('\n');
    }

    markdown.push_str("## Primary Query\n\n");
    if report.primary_query.is_empty() {
        markdown.push_str("_No rows._\n\n");
    } else {
        for row in &report.primary_query {
            markdown.push_str(&format!(
                "- `{}`{}\n",
                format_values(&row.values),
                row.tuple_id
                    .map(|tuple_id| format!(" | tuple `t{}`", tuple_id.0))
                    .unwrap_or_default()
            ));
        }
        markdown.push('\n');
    }

    if !report.named_queries.is_empty() {
        markdown.push_str("## Named Queries\n\n");
        for query in &report.named_queries {
            markdown.push_str(&format!(
                "### {}\n\n",
                query.name.as_deref().unwrap_or("<unnamed>")
            ));
            if query.rows.is_empty() {
                markdown.push_str("_No rows._\n\n");
            } else {
                for row in &query.rows {
                    markdown.push_str(&format!(
                        "- `{}`{}\n",
                        format_values(&row.values),
                        row.tuple_id
                            .map(|tuple_id| format!(" | tuple `t{}`", tuple_id.0))
                            .unwrap_or_default()
                    ));
                }
                markdown.push('\n');
            }
        }
    }

    if !report.traces.is_empty() {
        markdown.push_str("## Federated Traces\n\n");
        for trace in &report.traces {
            markdown.push_str(&format!(
                "### {}\n\n- Root: `t{}`\n- Tuple count: `{}`\n- Imported cuts: `{}`\n\n",
                trace.name.as_deref().unwrap_or("<unnamed trace>"),
                trace.root.0,
                trace.tuple_count,
                format_partition_cuts(&trace.imported_cuts)
            ));
            for tuple in &trace.tuples {
                markdown.push_str(&format!(
                    "- `t{}` via iteration `{}` -> `{}` | sources `{}` | imported `{}` | parents `{}`\n",
                    tuple.tuple_id.0,
                    tuple.iteration,
                    format_values(&tuple.values),
                    format_element_ids(&tuple.source_datom_ids),
                    format_partition_cuts(&tuple.imported_cuts),
                    format_tuple_ids(&tuple.parent_tuple_ids)
                ));
            }
            markdown.push('\n');
        }
    }

    markdown
}

fn format_federated_cut(cut: &FederatedCut) -> String {
    if cut.cuts.is_empty() {
        "<none>".into()
    } else {
        cut.cuts
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn format_partition_cuts(cuts: &[PartitionCut]) -> String {
    if cuts.is_empty() {
        "<none>".into()
    } else {
        cuts.iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn format_values(values: &[Value]) -> String {
    values
        .iter()
        .map(format_value)
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(value) => value.to_string(),
        Value::I64(value) => value.to_string(),
        Value::U64(value) => value.to_string(),
        Value::F64(value) => format!("{value:.4}"),
        Value::String(value) => value.clone(),
        Value::Bytes(value) => format!("<{} bytes>", value.len()),
        Value::Entity(value) => format!("entity({})", value.0),
        Value::List(values) => format!("[{}]", format_values(values)),
    }
}

fn format_element_ids(elements: &[aether_ast::ElementId]) -> String {
    if elements.is_empty() {
        "<none>".into()
    } else {
        elements
            .iter()
            .map(|element| format!("e{}", element.0))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn format_tuple_ids(tuple_ids: &[TupleId]) -> String {
    if tuple_ids.is_empty() {
        "<none>".into()
    } else {
        tuple_ids
            .iter()
            .map(|tuple_id| format!("t{}", tuple_id.0))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn format_policy_context(policy_context: Option<&PolicyContext>) -> String {
    match policy_context {
        None => "public".into(),
        Some(policy) if policy.is_empty() => "public".into(),
        Some(policy) => {
            let capabilities = if policy.capabilities.is_empty() {
                "capabilities=<none>".into()
            } else {
                format!("capabilities={}", policy.capabilities.join(","))
            };
            let visibilities = if policy.visibilities.is_empty() {
                "visibilities=<none>".into()
            } else {
                format!("visibilities={}", policy.visibilities.join(","))
            };
            format!("{capabilities}; {visibilities}")
        }
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PartitionAppendRequest {
    pub partition: PartitionId,
    pub datoms: Vec<Datom>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionAppendResponse {
    pub partition: PartitionId,
    pub appended: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionHistoryRequest {
    pub cut: PartitionCut,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PartitionHistoryResponse {
    pub cut: PartitionCut,
    pub datoms: Vec<Datom>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PartitionStateRequest {
    pub cut: PartitionCut,
    pub schema: Schema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PartitionStateResponse {
    pub cut: PartitionCut,
    pub state: ResolvedState,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedHistoryRequest {
    pub cut: FederatedCut,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedHistoryResponse {
    pub partitions: Vec<PartitionHistoryResponse>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ImportedFactQueryRequest {
    pub cut: PartitionCut,
    pub dsl: String,
    pub predicate: PredicateRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_name: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ImportedFactQueryResponse {
    pub cut: PartitionCut,
    pub predicate: PredicateRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_name: Option<String>,
    pub row_count: usize,
    pub facts: Vec<ExtensionalFact>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedRunDocumentRequest {
    pub dsl: String,
    pub imports: Vec<ImportedFactQueryRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedRunDocumentResponse {
    pub cut: FederatedCut,
    pub imports: Vec<ImportedFactQueryResponse>,
    pub run: RunDocumentResponse,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedExplainReport {
    pub generated_at_ms: u64,
    pub cut: FederatedCut,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
    pub imports: Vec<FederatedImportedSourceSummary>,
    pub primary_query: Vec<FederatedReportRow>,
    pub named_queries: Vec<FederatedNamedQuerySummary>,
    pub traces: Vec<FederatedTraceSummary>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedImportedSourceSummary {
    pub cut: PartitionCut,
    pub predicate: PredicateRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_name: Option<String>,
    pub fact_count: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedReportRow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tuple_id: Option<TupleId>,
    pub values: Vec<Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedNamedQuerySummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub rows: Vec<FederatedReportRow>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedTraceSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub root: TupleId,
    pub tuple_count: usize,
    pub imported_cuts: Vec<PartitionCut>,
    pub tuples: Vec<FederatedTraceTupleSummary>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedTraceTupleSummary {
    pub tuple_id: TupleId,
    pub values: Vec<Value>,
    pub iteration: usize,
    pub source_datom_ids: Vec<aether_ast::ElementId>,
    pub imported_cuts: Vec<PartitionCut>,
    pub parent_tuple_ids: Vec<TupleId>,
}

#[cfg(test)]
mod tests {
    use super::{
        render_federated_explain_report_markdown, FederatedHistoryRequest,
        FederatedRunDocumentRequest, ImportedFactQueryRequest, PartitionAppendRequest,
        PartitionHistoryRequest, PartitionStateRequest, PartitionedInMemoryKernelService,
        SqlitePartitionedKernelService,
    };
    use aether_ast::{
        AttributeId, Datom, DatomProvenance, ElementId, EntityId, FederatedCut, OperationKind,
        PartitionCut, PartitionId, PolicyEnvelope, PredicateId, PredicateRef, ReplicaId, Value,
    };
    use aether_resolver::ResolvedValue;
    use aether_schema::{AttributeClass, AttributeSchema, Schema, ValueType};
    use std::{
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn partitioned_service_keeps_local_truth_exact_per_partition() {
        let mut service = PartitionedInMemoryKernelService::new();
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("tenant-a"),
                datoms: vec![
                    sample_datom(1, 1, "tenant-a-open", 1, None),
                    sample_datom(1, 1, "tenant-a-running", 2, None),
                ],
            })
            .expect("append tenant-a");
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("tenant-b"),
                datoms: vec![sample_datom(1, 1, "tenant-b-ready", 1, None)],
            })
            .expect("append tenant-b");

        let tenant_a_current = service
            .partition_state(PartitionStateRequest {
                cut: PartitionCut::current("tenant-a"),
                schema: schema(),
                policy_context: None,
            })
            .expect("tenant-a current state");
        let tenant_a_as_of = service
            .partition_state(PartitionStateRequest {
                cut: PartitionCut::as_of("tenant-a", ElementId::new(1)),
                schema: schema(),
                policy_context: None,
            })
            .expect("tenant-a as-of state");
        let tenant_b_history = service
            .partition_history(PartitionHistoryRequest {
                cut: PartitionCut::current("tenant-b"),
                policy_context: None,
            })
            .expect("tenant-b history");

        assert_eq!(
            tenant_a_current
                .state
                .entity(&EntityId::new(1))
                .and_then(|entity| entity.attribute(&AttributeId::new(1))),
            Some(&ResolvedValue::Scalar(Some(Value::String(
                "tenant-a-running".into()
            ))))
        );
        assert_eq!(
            tenant_a_as_of
                .state
                .entity(&EntityId::new(1))
                .and_then(|entity| entity.attribute(&AttributeId::new(1))),
            Some(&ResolvedValue::Scalar(Some(Value::String(
                "tenant-a-open".into()
            ))))
        );
        assert_eq!(tenant_b_history.datoms.len(), 1);
        assert_eq!(
            tenant_b_history.datoms[0].value,
            Value::String("tenant-b-ready".into())
        );
    }

    #[test]
    fn federated_history_requires_explicit_unique_partition_cuts() {
        let mut service = PartitionedInMemoryKernelService::new();
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("tenant-a"),
                datoms: vec![sample_datom(1, 1, "alpha", 1, None)],
            })
            .expect("append tenant-a");
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("tenant-b"),
                datoms: vec![sample_datom(
                    1,
                    1,
                    "beta",
                    1,
                    Some(PolicyEnvelope {
                        capabilities: vec!["ops".into()],
                        visibilities: Vec::new(),
                    }),
                )],
            })
            .expect("append tenant-b");

        let federated = service
            .federated_history(FederatedHistoryRequest {
                cut: FederatedCut {
                    cuts: vec![
                        PartitionCut::current("tenant-b"),
                        PartitionCut::current("tenant-a"),
                    ],
                },
                policy_context: None,
            })
            .expect("federated history");
        assert_eq!(
            federated
                .partitions
                .iter()
                .map(|partition| partition.cut.to_string())
                .collect::<Vec<_>>(),
            vec![
                "tenant-a@current".to_string(),
                "tenant-b@current".to_string()
            ]
        );
        assert_eq!(federated.partitions[0].datoms.len(), 1);
        assert!(federated.partitions[1].datoms.is_empty());

        let duplicate_partition = service.federated_history(FederatedHistoryRequest {
            cut: FederatedCut {
                cuts: vec![
                    PartitionCut::current("tenant-a"),
                    PartitionCut::as_of("tenant-a", ElementId::new(1)),
                ],
            },
            policy_context: None,
        });
        assert!(matches!(
            duplicate_partition,
            Err(crate::ApiError::Validation(message))
                if message == "federated cut contains duplicate partition tenant-a"
        ));
    }

    #[test]
    fn unknown_partition_is_rejected_cleanly() {
        let service = PartitionedInMemoryKernelService::new();
        let error = service.partition_history(PartitionHistoryRequest {
            cut: PartitionCut::current("missing"),
            policy_context: None,
        });
        assert!(matches!(
            error,
            Err(crate::ApiError::Validation(message))
                if message == "unknown partition missing"
        ));
    }

    #[test]
    fn imported_facts_and_federated_explain_reports_preserve_partition_cuts() {
        let mut service = PartitionedInMemoryKernelService::new();
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("readiness"),
                datoms: vec![sample_datom(1, 1, "ready", 1, None)],
            })
            .expect("append readiness datoms");
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("authority"),
                datoms: vec![sample_datom(1, 1, "worker-a", 3, None)],
            })
            .expect("append authority datoms");

        let response = service
            .federated_run_document(FederatedRunDocumentRequest {
                dsl: federated_assignment_document(),
                imports: vec![
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("readiness", ElementId::new(1)),
                        dsl: readiness_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(11),
                            name: "imported_ready_task".into(),
                            arity: 1,
                        },
                        query_name: Some("ready_now".into()),
                    },
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("authority", ElementId::new(3)),
                        dsl: authority_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(12),
                            name: "imported_authorized_worker".into(),
                            arity: 2,
                        },
                        query_name: Some("authorized_now".into()),
                    },
                ],
                policy_context: None,
            })
            .expect("run federated document");

        assert_eq!(
            response
                .cut
                .cuts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["authority@e3".to_string(), "readiness@e1".to_string()]
        );
        assert_eq!(response.imports.len(), 2);
        assert_eq!(
            response
                .run
                .query
                .as_ref()
                .expect("primary query result")
                .rows[0]
                .values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-a".into())
            ]
        );

        let actionable_tuple_id = response
            .run
            .query
            .as_ref()
            .expect("primary query result")
            .rows[0]
            .tuple_id
            .expect("actionable tuple id");
        let actionable_tuple = response
            .run
            .derived
            .tuples
            .iter()
            .find(|tuple| tuple.tuple.id == actionable_tuple_id)
            .expect("actionable tuple");
        assert_eq!(
            actionable_tuple
                .metadata
                .imported_cuts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["authority@e3".to_string(), "readiness@e1".to_string()]
        );

        let trace = response
            .run
            .explains
            .iter()
            .find(|explain| explain.name.as_deref() == Some("actionable_trace"))
            .expect("actionable trace");
        let crate::ExplainArtifact::Tuple(trace) = &trace.result else {
            panic!("expected tuple trace");
        };
        assert_eq!(
            trace.tuples[0]
                .metadata
                .imported_cuts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["authority@e3".to_string(), "readiness@e1".to_string()]
        );

        let report = service
            .build_federated_explain_report(FederatedRunDocumentRequest {
                dsl: federated_assignment_document(),
                imports: vec![
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("readiness", ElementId::new(1)),
                        dsl: readiness_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(11),
                            name: "imported_ready_task".into(),
                            arity: 1,
                        },
                        query_name: Some("ready_now".into()),
                    },
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("authority", ElementId::new(3)),
                        dsl: authority_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(12),
                            name: "imported_authorized_worker".into(),
                            arity: 2,
                        },
                        query_name: Some("authorized_now".into()),
                    },
                ],
                policy_context: None,
            })
            .expect("build federated report");
        assert_eq!(report.traces.len(), 1);
        let markdown = render_federated_explain_report_markdown(&report);
        assert!(markdown.contains("authority@e3, readiness@e1"));
        assert!(markdown.contains("imported_authorized_worker"));
        assert!(markdown.contains("imported_ready_task"));
    }

    #[test]
    fn sqlite_partitioned_service_replays_federated_imports_after_restart() {
        let temp = TestPartitionDir::new("partitioned-sqlite");
        {
            let mut service =
                SqlitePartitionedKernelService::open(temp.path()).expect("open sqlite partitions");
            service
                .append_partition(PartitionAppendRequest {
                    partition: PartitionId::new("readiness"),
                    datoms: vec![sample_datom(1, 1, "ready", 1, None)],
                })
                .expect("append readiness datoms");
            service
                .append_partition(PartitionAppendRequest {
                    partition: PartitionId::new("authority"),
                    datoms: vec![sample_datom(1, 1, "worker-a", 3, None)],
                })
                .expect("append authority datoms");
        }

        let mut service =
            SqlitePartitionedKernelService::open(temp.path()).expect("reopen sqlite partitions");
        let response = service
            .federated_run_document(FederatedRunDocumentRequest {
                dsl: federated_assignment_document(),
                imports: vec![
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("readiness", ElementId::new(1)),
                        dsl: readiness_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(11),
                            name: "imported_ready_task".into(),
                            arity: 1,
                        },
                        query_name: Some("ready_now".into()),
                    },
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("authority", ElementId::new(3)),
                        dsl: authority_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(12),
                            name: "imported_authorized_worker".into(),
                            arity: 2,
                        },
                        query_name: Some("authorized_now".into()),
                    },
                ],
                policy_context: None,
            })
            .expect("run federated document after restart");

        assert_eq!(
            response.run.query.as_ref().expect("query result").rows[0].values,
            vec![
                Value::Entity(EntityId::new(1)),
                Value::String("worker-a".into())
            ]
        );
        let report = service
            .build_federated_explain_report(FederatedRunDocumentRequest {
                dsl: federated_assignment_document(),
                imports: vec![
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("readiness", ElementId::new(1)),
                        dsl: readiness_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(11),
                            name: "imported_ready_task".into(),
                            arity: 1,
                        },
                        query_name: Some("ready_now".into()),
                    },
                    ImportedFactQueryRequest {
                        cut: PartitionCut::as_of("authority", ElementId::new(3)),
                        dsl: authority_document(),
                        predicate: PredicateRef {
                            id: PredicateId::new(12),
                            name: "imported_authorized_worker".into(),
                            arity: 2,
                        },
                        query_name: Some("authorized_now".into()),
                    },
                ],
                policy_context: None,
            })
            .expect("build report after restart");
        assert!(render_federated_explain_report_markdown(&report)
            .contains("authority@e3, readiness@e1"));
    }

    fn schema() -> Schema {
        let mut schema = Schema::new("partitioned-v1");
        schema
            .register_attribute(AttributeSchema {
                id: AttributeId::new(1),
                name: "task.status".into(),
                class: AttributeClass::ScalarLww,
                value_type: ValueType::String,
            })
            .expect("register status attribute");
        schema
    }

    fn readiness_document() -> String {
        r#"
schema {
  attr task.status: ScalarLWW<String>
}

predicates {
  task_status(Entity, String)
  ready_task(Entity)
}

rules {
  ready_task(t) <- task_status(t, "ready")
}

materialize {
  ready_task
}

query ready_now {
  current
  goal ready_task(t)
  keep t
}
"#
        .into()
    }

    fn authority_document() -> String {
        r#"
schema {
  attr task.owner: ScalarLWW<String>
}

predicates {
  task_owner(Entity, String)
  authorized_worker(Entity, String)
}

rules {
  authorized_worker(t, worker) <- task_owner(t, worker)
}

materialize {
  authorized_worker
}

query authorized_now {
  current
  goal authorized_worker(t, worker)
  keep t, worker
}
"#
        .into()
    }

    fn federated_assignment_document() -> String {
        r#"
schema {
}

predicates {
  imported_ready_task(Entity)
  imported_authorized_worker(Entity, String)
  actionable_assignment(Entity, String)
}

rules {
  actionable_assignment(t, worker) <- imported_ready_task(t), imported_authorized_worker(t, worker)
}

materialize {
  actionable_assignment
}

query actionable_now {
  current
  goal actionable_assignment(t, worker)
  keep t, worker
}

explain actionable_trace {
  tuple actionable_assignment(entity(1), "worker-a")
}
"#
        .into()
    }

    fn sample_datom(
        entity: u64,
        attribute: u64,
        value: &str,
        element: u64,
        policy: Option<PolicyEnvelope>,
    ) -> Datom {
        Datom {
            entity: EntityId::new(entity),
            attribute: AttributeId::new(attribute),
            value: Value::String(value.into()),
            op: OperationKind::Assert,
            element: ElementId::new(element),
            replica: ReplicaId::new(1),
            causal_context: Default::default(),
            provenance: DatomProvenance::default(),
            policy,
        }
    }

    struct TestPartitionDir {
        path: PathBuf,
    }

    impl TestPartitionDir {
        fn new(name: &str) -> Self {
            let unique = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos();
            let mut path = std::env::temp_dir();
            path.push(format!("aether-partitions-{name}-{nanos}-{unique}"));
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestPartitionDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
