use crate::{
    deployment::PilotServiceConfig, ActivateSchemaRequest, ApiError, AppendAdmissionRequest,
    AsOfRequest, AuthReloadResponse, CoordinationCut, CoordinationDeltaReportRequest,
    CoordinationPilotReportRequest, CurrentStateRequest, ExplainTupleRequest,
    FederatedExplainReport, FederatedHistoryRequest, FederatedRunDocumentRequest,
    GetArtifactReferenceRequest, HistoryRequest, KernelService, NamespaceId, ParseDocumentRequest,
    PartitionAppendRequest, PartitionHistoryRequest, PartitionStateRequest,
    PartitionStatusResponse, PostgresKernelService, PromoteReplicaRequest,
    RegisterArtifactReferenceRequest, RegisterSchemaRequest, RegisterVectorRecordRequest,
    ReplicatedAuthorityPartitionService, ResolveTraceHandleRequest, RunDocumentRequest,
    SearchVectorsRequest, ServiceMode, ServiceStatusResponse, ServiceStatusStorage,
    SqliteKernelService,
};
use aether_ast::PolicyContext;
use aether_storage::PostgresTlsConfig;
use axum::{
    body::{to_bytes, Body},
    extract::State,
    http::{
        header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER},
        HeaderMap, HeaderValue, StatusCode,
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{mpsc, Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;

pub const AETHER_NAMESPACE_HEADER: &str = "x-aether-namespace";
pub const AETHER_REQUEST_ID_HEADER: &str = "x-aether-request-id";

tokio::task_local! {
    static REQUEST_ID: String;
}

fn new_request_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

fn current_request_id() -> String {
    REQUEST_ID
        .try_with(Clone::clone)
        .unwrap_or_else(|_| new_request_id())
}

async fn request_id_middleware(request: axum::extract::Request, next: Next) -> Response {
    let request_id = new_request_id();
    let response_request_id = request_id.clone();
    REQUEST_ID
        .scope(request_id, async move {
            let response = next.run(request).await;
            let mut response = ensure_structured_http_error(response, &response_request_id).await;
            if let Ok(value) = HeaderValue::from_str(&response_request_id) {
                response.headers_mut().insert(
                    axum::http::HeaderName::from_static(AETHER_REQUEST_ID_HEADER),
                    value,
                );
            }
            response
        })
        .await
}

async fn ensure_structured_http_error(response: Response, request_id: &str) -> Response {
    if !response.status().is_client_error() && !response.status().is_server_error() {
        return response;
    }

    let status = response.status();
    let (parts, body) = response.into_parts();
    let bytes = to_bytes(body, 64 * 1024).await.unwrap_or_default();
    if serde_json::from_slice::<StructuredErrorResponse>(&bytes)
        .is_ok_and(|body| body.request_id == request_id)
    {
        return Response::from_parts(parts, Body::from(bytes));
    }

    let message = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .or_else(|| value.get("message"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            String::from_utf8(bytes.to_vec())
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| {
            status
                .canonical_reason()
                .unwrap_or("HTTP request failed")
                .to_owned()
        });
    let code = match status {
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => "bad_request",
        StatusCode::PAYLOAD_TOO_LARGE => "request_body_too_large",
        StatusCode::NOT_FOUND => "route_not_found",
        StatusCode::METHOD_NOT_ALLOWED => "method_not_allowed",
        _ => "http_error",
    };
    let mut structured = (
        status,
        Json(StructuredErrorResponse {
            error: message,
            code: code.into(),
            request_id: request_id.into(),
            details: serde_json::json!({}),
        }),
    )
        .into_response();
    for (name, value) in &parts.headers {
        if name != CONTENT_LENGTH && name != CONTENT_TYPE {
            structured.headers_mut().insert(name.clone(), value.clone());
        }
    }
    structured
}

#[derive(Clone)]
struct BoundedBlockingExecutor {
    admitted: Arc<Semaphore>,
    workers: Arc<Semaphore>,
}

impl BoundedBlockingExecutor {
    fn new(concurrency: usize, queue: usize) -> Self {
        let concurrency = concurrency.max(1);
        Self {
            admitted: Arc::new(Semaphore::new(concurrency.saturating_add(queue))),
            workers: Arc::new(Semaphore::new(concurrency)),
        }
    }

    async fn run<T, F>(&self, operation: F) -> Result<T, HttpError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, HttpError> + Send + 'static,
    {
        let admitted = Arc::clone(&self.admitted)
            .try_acquire_owned()
            .map_err(|_| HttpError::NamespaceBusy {
                retry_after_seconds: 1,
            })?;
        let worker = Arc::clone(&self.workers)
            .acquire_owned()
            .await
            .map_err(|_| HttpError::WorkerUnavailable)?;
        tokio::task::spawn_blocking(move || {
            let _admitted = admitted;
            let _worker = worker;
            operation()
        })
        .await
        .map_err(|_| HttpError::WorkerFailed)?
    }
}

#[derive(Clone)]
pub struct HttpKernelState {
    services: Arc<NamespaceServiceDirectory>,
    partitioned: Option<Arc<ReplicatedAuthorityPartitionService>>,
    blocking: BoundedBlockingExecutor,
    auth: Arc<Mutex<HttpAuth>>,
    audit: AuditLog,
    status: Arc<Mutex<ServiceStatusResponse>>,
    auth_reload_config_path: Option<PathBuf>,
    configured_work_limits: (usize, usize, usize),
}

impl HttpKernelState {
    pub fn new(service: impl KernelService + Send + 'static) -> Self {
        Self::with_options(service, HttpKernelOptions::default())
    }

    pub fn with_partitioned_options(
        service: impl KernelService + Send + 'static,
        partitioned: ReplicatedAuthorityPartitionService,
        options: HttpKernelOptions,
    ) -> Self {
        Self::with_optional_partitioned(service, Some(partitioned), options)
    }

    pub fn with_options(
        service: impl KernelService + Send + 'static,
        options: HttpKernelOptions,
    ) -> Self {
        Self::with_optional_partitioned(service, None, options)
    }

    fn with_optional_partitioned(
        service: impl KernelService + Send + 'static,
        partitioned: Option<ReplicatedAuthorityPartitionService>,
        options: HttpKernelOptions,
    ) -> Self {
        Self::with_service_store(
            NamespaceServiceDirectory::single(service),
            partitioned,
            options,
        )
    }

    pub fn with_sqlite_namespaces(
        data_root: impl Into<PathBuf>,
        options: HttpKernelOptions,
    ) -> Self {
        Self::with_service_store(NamespaceServiceDirectory::sqlite(data_root), None, options)
    }

    pub fn with_postgres_namespaces(
        database_url: impl Into<String>,
        schema: impl Into<String>,
        sidecar_path: impl Into<PathBuf>,
        options: HttpKernelOptions,
    ) -> Self {
        Self::with_postgres_namespaces_and_tls(
            database_url,
            schema,
            sidecar_path,
            PostgresTlsConfig::default(),
            options,
        )
    }

    pub fn with_postgres_namespaces_and_tls(
        database_url: impl Into<String>,
        schema: impl Into<String>,
        sidecar_path: impl Into<PathBuf>,
        tls: PostgresTlsConfig,
        options: HttpKernelOptions,
    ) -> Self {
        Self::with_service_store(
            NamespaceServiceDirectory::postgres(database_url, schema, sidecar_path, tls),
            None,
            options,
        )
    }

    fn with_service_store(
        services: NamespaceServiceDirectory,
        partitioned: Option<ReplicatedAuthorityPartitionService>,
        options: HttpKernelOptions,
    ) -> Self {
        let HttpKernelOptions {
            auth,
            audit_log_path,
            service_status,
            auth_reload_config_path,
            namespace_concurrency_limit,
            namespace_queue_limit,
            audit_queue_limit,
        } = options;
        let status =
            service_status.unwrap_or_else(|| services.default_status(audit_log_path.clone()));
        Self {
            services: Arc::new(services),
            partitioned: partitioned.map(Arc::new),
            blocking: BoundedBlockingExecutor::new(
                namespace_concurrency_limit,
                namespace_queue_limit,
            ),
            auth: Arc::new(Mutex::new(HttpAuth::from_config(auth))),
            audit: AuditLog::new(audit_log_path, audit_queue_limit),
            status: Arc::new(Mutex::new(status)),
            auth_reload_config_path,
            configured_work_limits: (
                namespace_concurrency_limit,
                namespace_queue_limit,
                audit_queue_limit,
            ),
        }
    }

    fn authorize(
        &self,
        headers: &HeaderMap,
        required_scope: AuthScope,
        namespace: &NamespaceId,
    ) -> Result<AuthenticatedPrincipal, HttpError> {
        self.auth
            .lock()
            .map_err(|_| HttpError::LockPoisoned)?
            .authorize(headers, required_scope, namespace)
    }

    fn status_snapshot(&self) -> Result<ServiceStatusResponse, HttpError> {
        let mut status = self
            .status
            .lock()
            .map(|status| status.clone())
            .map_err(|_| HttpError::LockPoisoned)?;
        if let Some(partitioned) = &self.partitioned {
            let partition_status = partitioned.partition_status().map_err(HttpError::Api)?;
            status.service_mode = ServiceMode::Partitioned;
            status.replicas = flatten_replica_status(&partition_status);
        }
        let active_namespaces = self.services.active_namespaces()?;
        status.active_namespace_count = active_namespaces.len();
        status.namespaces = self
            .auth
            .lock()
            .map_err(|_| HttpError::LockPoisoned)?
            .namespace_status(&active_namespaces);
        Ok(status)
    }

    fn reload_auth_from_config(&self) -> Result<AuthReloadResponse, HttpError> {
        let Some(config_path) = &self.auth_reload_config_path else {
            return Err(HttpError::Api(ApiError::Validation(
                "auth reload is not configured for this service".into(),
            )));
        };
        let resolved = PilotServiceConfig::load(config_path)
            .and_then(|config| config.resolve(config_path))
            .map_err(|error| HttpError::Api(ApiError::Validation(error.to_string())))?;
        let resolved_status = resolved.service_status();
        let mut status = self.status.lock().map_err(|_| HttpError::LockPoisoned)?;
        if status.bind_addr.as_deref() != Some(resolved.bind_addr.as_str())
            || status.storage != resolved_status.storage
            || status.transport != resolved_status.transport
            || self.configured_work_limits
                != (
                    resolved.concurrency.namespace_workers,
                    resolved.concurrency.namespace_queue,
                    resolved.concurrency.audit_queue,
                )
        {
            return Err(HttpError::Api(ApiError::Validation(
                "auth reload cannot change bind, transport, storage, or concurrency configuration"
                    .into(),
            )));
        }
        {
            let mut auth = self.auth.lock().map_err(|_| HttpError::LockPoisoned)?;
            *auth = HttpAuth::from_config(resolved.auth.clone());
        }
        status.config_version.clone_from(&resolved.config_version);
        status.schema_version.clone_from(&resolved.schema_version);
        status.principals = resolved_status.principals;
        Ok(AuthReloadResponse {
            reloaded_at_ms: now_millis(),
            principal_count: status.principals.len(),
            revoked_count: status
                .principals
                .iter()
                .filter(|principal| principal.revoked)
                .count(),
        })
    }

    async fn execute<T, F>(
        &self,
        headers: &HeaderMap,
        method: &'static str,
        path: &'static str,
        required_scope: AuthScope,
        mut context: AuditContext,
        operation: F,
    ) -> Result<T, HttpError>
    where
        T: Send + 'static,
        F: FnOnce(
                &mut dyn KernelService,
                &AuthenticatedPrincipal,
                &mut AuditContext,
            ) -> Result<T, HttpError>
            + Send
            + 'static,
    {
        let namespace = namespace_from_headers(headers)?;
        context.namespace = Some(namespace.to_string());
        let principal = match self.authorize(headers, required_scope, &namespace) {
            Ok(principal) => principal,
            Err(error) => {
                self.audit.record(AuditEntry::for_denied(
                    method,
                    path,
                    error.status_code(),
                    error.audit_principal(),
                    None,
                    None,
                    required_scope,
                    error.audit_message(),
                    context,
                ));
                return Err(error);
            }
        };

        let principal_for_operation = principal.clone();
        let services = Arc::clone(&self.services);
        let namespace_for_operation = namespace.clone();
        let (result, context) = self
            .blocking
            .run(move || {
                let mut context = context;
                let result = services.execute(&namespace_for_operation, |service| {
                    operation(service, &principal_for_operation, &mut context)
                });
                Ok((result, context))
            })
            .await?;

        let status = match &result {
            Ok(_) => StatusCode::OK,
            Err(error) => error.status_code(),
        };
        self.audit.record(AuditEntry::for_request(
            method,
            path,
            status,
            &principal,
            required_scope,
            context,
        ));

        result
    }

    async fn execute_partitioned<T, F>(
        &self,
        headers: &HeaderMap,
        method: &'static str,
        path: &'static str,
        required_scope: AuthScope,
        context: AuditContext,
        operation: F,
    ) -> Result<T, HttpError>
    where
        T: Send + 'static,
        F: FnOnce(
                &ReplicatedAuthorityPartitionService,
                &AuthenticatedPrincipal,
                &mut AuditContext,
            ) -> Result<T, HttpError>
            + Send
            + 'static,
    {
        let namespace = namespace_from_headers(headers)?;
        let mut context = context;
        context.namespace = Some(namespace.to_string());
        let principal = match self.authorize(headers, required_scope, &namespace) {
            Ok(principal) => principal,
            Err(error) => {
                self.audit.record(AuditEntry::for_denied(
                    method,
                    path,
                    error.status_code(),
                    error.audit_principal(),
                    None,
                    None,
                    required_scope,
                    error.audit_message(),
                    context,
                ));
                return Err(error);
            }
        };

        let partitioned = self.partitioned.clone().ok_or_else(|| {
            HttpError::Api(ApiError::Validation(
                "partitioned prototype is not configured for this service".into(),
            ))
        })?;
        let principal_for_operation = principal.clone();
        let (result, context) = self
            .blocking
            .run(move || {
                let mut context = context;
                let result = operation(&partitioned, &principal, &mut context);
                Ok((result, context))
            })
            .await?;
        let status = match &result {
            Ok(_) => StatusCode::OK,
            Err(error) => error.status_code(),
        };
        self.audit.record(AuditEntry::for_request(
            method,
            path,
            status,
            &principal_for_operation,
            required_scope,
            context,
        ));
        result
    }

    async fn resolve_execution_trace(
        &self,
        headers: &HeaderMap,
        mut request: ResolveTraceHandleRequest,
        mut context: AuditContext,
    ) -> Result<crate::ResolveTraceHandleResponse, HttpError> {
        let namespace = namespace_from_headers(headers)?;
        context.namespace = Some(namespace.to_string());
        let principal = match self.authorize(headers, AuthScope::Explain, &namespace) {
            Ok(principal) => principal,
            Err(error) => {
                self.audit.record(AuditEntry::for_denied(
                    "POST",
                    "/v1/explanations/resolve",
                    error.status_code(),
                    error.audit_principal(),
                    None,
                    None,
                    AuthScope::Explain,
                    error.audit_message(),
                    context,
                ));
                return Err(error);
            }
        };
        request.policy_context =
            match apply_policy_binding(&principal, request.policy_context, &mut context) {
                Ok(policy) => policy,
                Err(error) => {
                    self.audit.record(AuditEntry::for_denied(
                        "POST",
                        "/v1/explanations/resolve",
                        error.status_code(),
                        error.audit_principal(),
                        principal.principal_id.clone(),
                        principal.token_id.clone(),
                        AuthScope::Explain,
                        error.audit_message(),
                        context,
                    ));
                    return Err(error);
                }
            };

        let services = Arc::clone(&self.services);
        let partitioned = self.partitioned.clone();
        let namespace_for_operation = namespace.clone();
        let result = self
            .blocking
            .run(move || {
                let central = services.execute(&namespace_for_operation, |service| {
                    service
                        .resolve_trace_handle(request.clone())
                        .map_err(HttpError::Api)
                });
                let partition = if namespace_for_operation == NamespaceId::default() {
                    partitioned.map(|service| {
                        service
                            .resolve_trace_handle(request)
                            .map_err(HttpError::Api)
                    })
                } else {
                    None
                };
                Ok(match (central, partition) {
                    (central, None) => central,
                    (Ok(_), Some(Ok(_))) => Err(HttpError::Api(ApiError::Execution(
                        crate::execution::ExecutionError::Store(
                            "trace handle collision across execution stores".into(),
                        ),
                    ))),
                    (Ok(response), Some(Err(error))) if is_unknown_trace_error(&error) => {
                        Ok(response)
                    }
                    (Ok(_), Some(Err(error))) => Err(error),
                    (Err(error), Some(Ok(response))) if is_unknown_trace_error(&error) => {
                        Ok(response)
                    }
                    (Err(error), Some(Ok(_))) => Err(error),
                    (Err(central_error), Some(Err(partition_error))) => {
                        if is_unknown_trace_error(&central_error) {
                            Err(partition_error)
                        } else {
                            Err(central_error)
                        }
                    }
                })
            })
            .await?;

        if let Ok(response) = &result {
            context.tuple_id = Some(response.record.local_tuple_id.0);
            context.trace_tuple_count = Some(response.record.trace.tuples.len());
        }
        let status = result
            .as_ref()
            .map(|_| StatusCode::OK)
            .unwrap_or_else(|error| error.status_code());
        self.audit.record(AuditEntry::for_request(
            "POST",
            "/v1/explanations/resolve",
            status,
            &principal,
            AuthScope::Explain,
            context,
        ));
        result
    }

    fn audit_entries(&self, headers: &HeaderMap) -> Result<AuditLogResponse, HttpError> {
        let namespace = namespace_from_headers(headers)?;
        let context = AuditContext {
            namespace: Some(namespace.to_string()),
            ..Default::default()
        };
        let principal = match self.authorize(headers, AuthScope::Ops, &namespace) {
            Ok(principal) => principal,
            Err(error) => {
                self.audit.record(AuditEntry::for_denied(
                    "GET",
                    "/v1/audit",
                    error.status_code(),
                    error.audit_principal(),
                    None,
                    None,
                    AuthScope::Ops,
                    error.audit_message(),
                    context,
                ));
                return Err(error);
            }
        };

        let response = AuditLogResponse {
            entries: self
                .audit
                .snapshot()?
                .into_iter()
                .filter(|entry| {
                    entry.context.namespace.as_deref().unwrap_or("default") == namespace.as_str()
                })
                .collect(),
        };
        self.audit.record(AuditEntry::for_request(
            "GET",
            "/v1/audit",
            StatusCode::OK,
            &principal,
            AuthScope::Ops,
            AuditContext {
                namespace: Some(namespace.to_string()),
                ..Default::default()
            },
        ));
        Ok(response)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthScope {
    Append,
    Query,
    Explain,
    Ops,
}

impl AuthScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Query => "query",
            Self::Explain => "explain",
            Self::Ops => "ops",
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpAuthConfig {
    pub tokens: Vec<HttpAccessToken>,
}

impl HttpAuthConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_token(
        mut self,
        token: impl Into<String>,
        principal: impl Into<String>,
        scopes: impl IntoIterator<Item = AuthScope>,
    ) -> Self {
        self.tokens.push(HttpAccessToken {
            token: token.into(),
            token_id: String::new(),
            principal: principal.into(),
            principal_id: String::new(),
            scopes: scopes.into_iter().collect(),
            namespaces: Vec::new(),
            policy_context: None,
            source: "inline".into(),
            revoked: false,
        });
        self
    }

    pub fn with_token_context(
        mut self,
        token: impl Into<String>,
        principal: impl Into<String>,
        scopes: impl IntoIterator<Item = AuthScope>,
        policy_context: PolicyContext,
    ) -> Self {
        self.tokens.push(HttpAccessToken {
            token: token.into(),
            token_id: String::new(),
            principal: principal.into(),
            principal_id: String::new(),
            scopes: scopes.into_iter().collect(),
            namespaces: Vec::new(),
            policy_context: normalize_policy_context(Some(policy_context)),
            source: "inline".into(),
            revoked: false,
        });
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpAccessToken {
    pub token: String,
    #[serde(default)]
    pub token_id: String,
    pub principal: String,
    #[serde(default)]
    pub principal_id: String,
    pub scopes: Vec<AuthScope>,
    #[serde(default)]
    pub namespaces: Vec<NamespaceId>,
    pub policy_context: Option<PolicyContext>,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub revoked: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpKernelOptions {
    pub auth: HttpAuthConfig,
    pub audit_log_path: Option<PathBuf>,
    pub service_status: Option<ServiceStatusResponse>,
    pub auth_reload_config_path: Option<PathBuf>,
    #[serde(default = "default_namespace_concurrency_limit")]
    pub namespace_concurrency_limit: usize,
    #[serde(default = "default_namespace_queue_limit")]
    pub namespace_queue_limit: usize,
    #[serde(default = "default_audit_queue_limit")]
    pub audit_queue_limit: usize,
}

impl Default for HttpKernelOptions {
    fn default() -> Self {
        Self {
            auth: HttpAuthConfig::default(),
            audit_log_path: None,
            service_status: None,
            auth_reload_config_path: None,
            namespace_concurrency_limit: default_namespace_concurrency_limit(),
            namespace_queue_limit: default_namespace_queue_limit(),
            audit_queue_limit: default_audit_queue_limit(),
        }
    }
}

impl HttpKernelOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_auth(mut self, auth: HttpAuthConfig) -> Self {
        self.auth = auth;
        self
    }

    pub fn with_audit_log_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.audit_log_path = Some(path.into());
        self
    }

    pub fn with_service_status(mut self, status: ServiceStatusResponse) -> Self {
        self.service_status = Some(status);
        self
    }

    pub fn with_auth_reload_config_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.auth_reload_config_path = Some(path.into());
        self
    }

    pub fn with_namespace_work_limits(mut self, concurrency: usize, queue: usize) -> Self {
        self.namespace_concurrency_limit = concurrency.max(1);
        self.namespace_queue_limit = queue;
        self
    }

    pub fn with_audit_queue_limit(mut self, limit: usize) -> Self {
        self.audit_queue_limit = limit.max(1);
        self
    }
}

fn default_namespace_concurrency_limit() -> usize {
    8
}

fn default_namespace_queue_limit() -> usize {
    64
}

fn default_audit_queue_limit() -> usize {
    1_024
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub timestamp_ms: u64,
    pub principal: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_id: Option<String>,
    pub method: String,
    pub path: String,
    pub status: u16,
    pub scope: AuthScope,
    pub outcome: String,
    pub detail: Option<String>,
    pub context: AuditContext,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuditContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_report: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_cut: Option<String>,
    pub temporal_view: Option<String>,
    pub query_goal: Option<String>,
    pub tuple_id: Option<u64>,
    pub requested_element: Option<u64>,
    pub datom_count: Option<usize>,
    pub entity_count: Option<usize>,
    pub row_count: Option<usize>,
    pub derived_tuple_count: Option<usize>,
    pub trace_tuple_count: Option<usize>,
    pub last_element: Option<u64>,
    pub requested_capabilities: Vec<String>,
    pub requested_visibilities: Vec<String>,
    pub granted_capabilities: Vec<String>,
    pub granted_visibilities: Vec<String>,
    pub effective_capabilities: Vec<String>,
    pub effective_visibilities: Vec<String>,
    pub policy_decision: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub legacy_endpoint: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub schema_ref_omitted: bool,
}

impl AuditEntry {
    fn for_request(
        method: impl Into<String>,
        path: impl Into<String>,
        status: StatusCode,
        principal: &AuthenticatedPrincipal,
        scope: AuthScope,
        context: AuditContext,
    ) -> Self {
        Self {
            timestamp_ms: now_millis(),
            principal: principal.id.clone(),
            principal_id: principal.principal_id.clone(),
            token_id: principal.token_id.clone(),
            method: method.into(),
            path: path.into(),
            status: status.as_u16(),
            scope,
            outcome: if status.is_success() {
                "ok".into()
            } else {
                "error".into()
            },
            detail: None,
            context,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn for_denied(
        method: impl Into<String>,
        path: impl Into<String>,
        status: StatusCode,
        principal: impl Into<String>,
        principal_id: Option<String>,
        token_id: Option<String>,
        scope: AuthScope,
        detail: impl Into<String>,
        context: AuditContext,
    ) -> Self {
        Self {
            timestamp_ms: now_millis(),
            principal: principal.into(),
            principal_id,
            token_id,
            method: method.into(),
            path: path.into(),
            status: status.as_u16(),
            scope,
            outcome: if status == StatusCode::UNAUTHORIZED {
                "unauthorized".into()
            } else {
                "forbidden".into()
            },
            detail: Some(detail.into()),
            context,
        }
    }

    fn audit_failure(path: &Path, error: &std::io::Error) -> Self {
        Self {
            timestamp_ms: now_millis(),
            principal: "aether".into(),
            principal_id: None,
            token_id: None,
            method: "AUDIT".into(),
            path: path.display().to_string(),
            status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            scope: AuthScope::Ops,
            outcome: "audit_write_failed".into(),
            detail: Some(error.to_string()),
            context: AuditContext::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuditLogResponse {
    pub entries: Vec<AuditEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
}

impl Default for HealthResponse {
    fn default() -> Self {
        Self {
            status: "ok".into(),
        }
    }
}

pub fn http_router(service: impl KernelService + Send + 'static) -> Router {
    http_router_with_options(service, HttpKernelOptions::default())
}

pub fn http_router_with_partitioned_options(
    service: impl KernelService + Send + 'static,
    partitioned: ReplicatedAuthorityPartitionService,
    options: HttpKernelOptions,
) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/status", get(service_status))
        .route("/v1/history", get(history))
        .route("/v1/audit", get(audit_log))
        .route("/v1/admin/auth/reload", post(reload_auth))
        .route("/v1/append", post(append))
        .route("/v1/append/dry-run", post(append_dry_run))
        .route("/v1/append/receipts", get(append_receipts_endpoint))
        .route("/v1/schema", get(schema_catalog_endpoint))
        .route("/v1/schema/register", post(register_schema_endpoint))
        .route("/v1/schema/activate", post(activate_schema_endpoint))
        .route("/v1/state/current", post(current_state))
        .route("/v1/state/as-of", post(as_of))
        .route("/v1/documents/parse", post(parse_document))
        .route("/v1/documents/run", post(run_document))
        .route(
            "/v1/reports/pilot/coordination",
            post(coordination_pilot_report),
        )
        .route(
            "/v1/reports/pilot/coordination-delta",
            post(coordination_delta_report),
        )
        .route("/v1/explain/tuple", post(explain_tuple))
        .route("/v1/explanations/resolve", post(resolve_trace_handle))
        .route("/v1/partitions/status", get(partition_status))
        .route("/v1/partitions/promote", post(promote_replica))
        .route("/v1/partitions/append", post(partition_append))
        .route("/v1/partitions/history", post(partition_history))
        .route("/v1/partitions/state", post(partition_state))
        .route("/v1/federated/history", post(federated_history))
        .route("/v1/federated/run", post(federated_run_document))
        .route("/v1/federated/report", post(federated_report))
        .route(
            "/v1/sidecars/artifacts/register",
            post(register_artifact_reference),
        )
        .route("/v1/sidecars/artifacts/get", post(get_artifact_reference))
        .route(
            "/v1/sidecars/vectors/register",
            post(register_vector_record),
        )
        .route("/v1/sidecars/vectors/search", post(search_vectors))
        .with_state(HttpKernelState::with_partitioned_options(
            service,
            partitioned,
            options,
        ))
        .layer(middleware::from_fn(request_id_middleware))
}

pub fn http_router_with_options(
    service: impl KernelService + Send + 'static,
    options: HttpKernelOptions,
) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/status", get(service_status))
        .route("/v1/history", get(history))
        .route("/v1/audit", get(audit_log))
        .route("/v1/admin/auth/reload", post(reload_auth))
        .route("/v1/append", post(append))
        .route("/v1/append/dry-run", post(append_dry_run))
        .route("/v1/append/receipts", get(append_receipts_endpoint))
        .route("/v1/schema", get(schema_catalog_endpoint))
        .route("/v1/schema/register", post(register_schema_endpoint))
        .route("/v1/schema/activate", post(activate_schema_endpoint))
        .route("/v1/state/current", post(current_state))
        .route("/v1/state/as-of", post(as_of))
        .route("/v1/documents/parse", post(parse_document))
        .route("/v1/documents/run", post(run_document))
        .route(
            "/v1/reports/pilot/coordination",
            post(coordination_pilot_report),
        )
        .route(
            "/v1/reports/pilot/coordination-delta",
            post(coordination_delta_report),
        )
        .route("/v1/explain/tuple", post(explain_tuple))
        .route("/v1/explanations/resolve", post(resolve_trace_handle))
        .route(
            "/v1/sidecars/artifacts/register",
            post(register_artifact_reference),
        )
        .route("/v1/sidecars/artifacts/get", post(get_artifact_reference))
        .route(
            "/v1/sidecars/vectors/register",
            post(register_vector_record),
        )
        .route("/v1/sidecars/vectors/search", post(search_vectors))
        .with_state(HttpKernelState::with_options(service, options))
        .layer(middleware::from_fn(request_id_middleware))
}

pub fn http_router_with_sqlite_namespaces(
    data_root: impl Into<PathBuf>,
    options: HttpKernelOptions,
) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/status", get(service_status))
        .route("/v1/history", get(history))
        .route("/v1/audit", get(audit_log))
        .route("/v1/admin/auth/reload", post(reload_auth))
        .route("/v1/append", post(append))
        .route("/v1/append/dry-run", post(append_dry_run))
        .route("/v1/append/receipts", get(append_receipts_endpoint))
        .route("/v1/schema", get(schema_catalog_endpoint))
        .route("/v1/schema/register", post(register_schema_endpoint))
        .route("/v1/schema/activate", post(activate_schema_endpoint))
        .route("/v1/state/current", post(current_state))
        .route("/v1/state/as-of", post(as_of))
        .route("/v1/documents/parse", post(parse_document))
        .route("/v1/documents/run", post(run_document))
        .route(
            "/v1/reports/pilot/coordination",
            post(coordination_pilot_report),
        )
        .route(
            "/v1/reports/pilot/coordination-delta",
            post(coordination_delta_report),
        )
        .route("/v1/explain/tuple", post(explain_tuple))
        .route("/v1/explanations/resolve", post(resolve_trace_handle))
        .route(
            "/v1/sidecars/artifacts/register",
            post(register_artifact_reference),
        )
        .route("/v1/sidecars/artifacts/get", post(get_artifact_reference))
        .route(
            "/v1/sidecars/vectors/register",
            post(register_vector_record),
        )
        .route("/v1/sidecars/vectors/search", post(search_vectors))
        .with_state(HttpKernelState::with_sqlite_namespaces(data_root, options))
        .layer(middleware::from_fn(request_id_middleware))
}

pub fn http_router_with_postgres_namespaces(
    database_url: impl Into<String>,
    schema: impl Into<String>,
    sidecar_path: impl Into<PathBuf>,
    options: HttpKernelOptions,
) -> Router {
    http_router_with_postgres_namespaces_and_tls(
        database_url,
        schema,
        sidecar_path,
        PostgresTlsConfig::default(),
        options,
    )
}

pub fn http_router_with_postgres_namespaces_and_tls(
    database_url: impl Into<String>,
    schema: impl Into<String>,
    sidecar_path: impl Into<PathBuf>,
    tls: PostgresTlsConfig,
    options: HttpKernelOptions,
) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/status", get(service_status))
        .route("/v1/history", get(history))
        .route("/v1/audit", get(audit_log))
        .route("/v1/admin/auth/reload", post(reload_auth))
        .route("/v1/append", post(append))
        .route("/v1/append/dry-run", post(append_dry_run))
        .route("/v1/append/receipts", get(append_receipts_endpoint))
        .route("/v1/schema", get(schema_catalog_endpoint))
        .route("/v1/schema/register", post(register_schema_endpoint))
        .route("/v1/schema/activate", post(activate_schema_endpoint))
        .route("/v1/state/current", post(current_state))
        .route("/v1/state/as-of", post(as_of))
        .route("/v1/documents/parse", post(parse_document))
        .route("/v1/documents/run", post(run_document))
        .route(
            "/v1/reports/pilot/coordination",
            post(coordination_pilot_report),
        )
        .route(
            "/v1/reports/pilot/coordination-delta",
            post(coordination_delta_report),
        )
        .route("/v1/explain/tuple", post(explain_tuple))
        .route("/v1/explanations/resolve", post(resolve_trace_handle))
        .route(
            "/v1/sidecars/artifacts/register",
            post(register_artifact_reference),
        )
        .route("/v1/sidecars/artifacts/get", post(get_artifact_reference))
        .route(
            "/v1/sidecars/vectors/register",
            post(register_vector_record),
        )
        .route("/v1/sidecars/vectors/search", post(search_vectors))
        .with_state(HttpKernelState::with_postgres_namespaces_and_tls(
            database_url,
            schema,
            sidecar_path,
            tls,
            options,
        ))
        .layer(middleware::from_fn(request_id_middleware))
}

#[derive(Clone)]
enum NamespaceServiceMode {
    Static,
    Sqlite {
        data_root: PathBuf,
    },
    Postgres {
        database_url: String,
        schema: String,
        sidecar_path: PathBuf,
        tls: PostgresTlsConfig,
    },
}

struct NamespaceServiceDirectory {
    mode: NamespaceServiceMode,
    services: Mutex<HashMap<NamespaceId, Arc<NamespaceServiceHandle>>>,
}

struct NamespaceServiceHandle {
    state: Mutex<NamespaceServiceState>,
}

enum NamespaceServiceState {
    Uninitialized,
    Ready(Box<dyn KernelService + Send>),
    Failed(String),
}

impl NamespaceServiceHandle {
    fn uninitialized() -> Self {
        Self {
            state: Mutex::new(NamespaceServiceState::Uninitialized),
        }
    }

    fn ready(service: impl KernelService + Send + 'static) -> Self {
        Self {
            state: Mutex::new(NamespaceServiceState::Ready(Box::new(service))),
        }
    }
}

impl NamespaceServiceDirectory {
    fn single(service: impl KernelService + Send + 'static) -> Self {
        let mut services = HashMap::new();
        services.insert(
            NamespaceId::default(),
            Arc::new(NamespaceServiceHandle::ready(service)),
        );
        Self {
            mode: NamespaceServiceMode::Static,
            services: Mutex::new(services),
        }
    }

    fn sqlite(data_root: impl Into<PathBuf>) -> Self {
        Self {
            mode: NamespaceServiceMode::Sqlite {
                data_root: data_root.into(),
            },
            services: Mutex::new(HashMap::new()),
        }
    }

    fn postgres(
        database_url: impl Into<String>,
        schema: impl Into<String>,
        sidecar_path: impl Into<PathBuf>,
        tls: PostgresTlsConfig,
    ) -> Self {
        Self {
            mode: NamespaceServiceMode::Postgres {
                database_url: database_url.into(),
                schema: schema.into(),
                sidecar_path: sidecar_path.into(),
                tls,
            },
            services: Mutex::new(HashMap::new()),
        }
    }

    fn handle(&self, namespace: &NamespaceId) -> Result<Arc<NamespaceServiceHandle>, HttpError> {
        let mut services = self.services.lock().map_err(|_| HttpError::LockPoisoned)?;
        Ok(Arc::clone(
            services
                .entry(namespace.clone())
                .or_insert_with(|| Arc::new(NamespaceServiceHandle::uninitialized())),
        ))
    }

    fn execute<T, F>(&self, namespace: &NamespaceId, operation: F) -> Result<T, HttpError>
    where
        F: FnOnce(&mut dyn KernelService) -> Result<T, HttpError>,
    {
        let handle = self.handle(namespace)?;
        let mut state = handle.state.lock().map_err(|_| HttpError::LockPoisoned)?;
        if matches!(*state, NamespaceServiceState::Uninitialized) {
            match self.open_namespace_service(namespace) {
                Ok(service) => *state = NamespaceServiceState::Ready(service),
                Err(error) => {
                    let message = error.audit_message();
                    *state = NamespaceServiceState::Failed(message.clone());
                    return Err(HttpError::NamespaceInitializationFailed(message));
                }
            }
        }
        match &mut *state {
            NamespaceServiceState::Ready(service) => operation(service.as_mut()),
            NamespaceServiceState::Failed(message) => {
                Err(HttpError::NamespaceInitializationFailed(message.clone()))
            }
            NamespaceServiceState::Uninitialized => unreachable!("namespace initialized above"),
        }
    }

    fn active_namespaces(&self) -> Result<Vec<NamespaceId>, HttpError> {
        let services = self.services.lock().map_err(|_| HttpError::LockPoisoned)?;
        let mut namespaces = services.keys().cloned().collect::<Vec<_>>();
        namespaces.sort();
        Ok(namespaces)
    }

    fn default_status(&self, audit_log_path: Option<PathBuf>) -> ServiceStatusResponse {
        let mut status =
            ServiceStatusResponse::single_node(env!("CARGO_PKG_VERSION"), "pilot-v1", "v1");
        status.storage = match &self.mode {
            NamespaceServiceMode::Static => ServiceStatusStorage {
                audit_log_path,
                ..ServiceStatusStorage::default()
            },
            NamespaceServiceMode::Sqlite { data_root } => ServiceStatusStorage {
                backend: "sqlite".into(),
                database_path: None,
                data_root: Some(data_root.clone()),
                postgres_schema: None,
                postgres_url_configured: false,
                postgres_tls_mode: None,
                postgres_ca_certificate_count: None,
                postgres_client_certificate_configured: None,
                postgres_system_roots_enabled: None,
                sidecar_mode: "sqlite_local_per_namespace".into(),
                sidecar_path: None,
                audit_log_path,
                partition_root: None,
            },
            NamespaceServiceMode::Postgres {
                schema,
                sidecar_path,
                tls,
                ..
            } => ServiceStatusStorage {
                backend: "postgres".into(),
                database_path: None,
                data_root: None,
                postgres_schema: Some(schema.clone()),
                postgres_url_configured: true,
                postgres_tls_mode: Some(
                    match tls.mode {
                        aether_storage::PostgresTlsMode::VerifyFull => "verify_full",
                        aether_storage::PostgresTlsMode::VerifyCa => "verify_ca",
                        aether_storage::PostgresTlsMode::DevelopmentPlaintext => {
                            "development_plaintext"
                        }
                    }
                    .into(),
                ),
                postgres_ca_certificate_count: Some(tls.ca_certificate_paths.len()),
                postgres_client_certificate_configured: Some(tls.client_certificate_path.is_some()),
                postgres_system_roots_enabled: Some(!tls.disable_system_roots),
                sidecar_mode: "sqlite_local".into(),
                sidecar_path: Some(sidecar_path.clone()),
                audit_log_path,
                partition_root: None,
            },
        };
        status
    }

    fn open_namespace_service(
        &self,
        namespace: &NamespaceId,
    ) -> Result<Box<dyn KernelService + Send>, HttpError> {
        match &self.mode {
            NamespaceServiceMode::Static => {
                if namespace == &NamespaceId::default() {
                    Err(HttpError::Api(ApiError::Validation(
                        "default namespace service is not initialized".into(),
                    )))
                } else {
                    Err(HttpError::Api(ApiError::Validation(format!(
                        "namespace {} is not configured for this single-node service",
                        namespace
                    ))))
                }
            }
            NamespaceServiceMode::Sqlite { data_root } => Ok(Box::new(
                SqliteKernelService::open(namespace_sqlite_path(data_root, namespace))
                    .map_err(HttpError::Api)?
                    .with_namespace(namespace.clone()),
            )),
            NamespaceServiceMode::Postgres {
                database_url,
                schema,
                sidecar_path,
                tls,
            } => Ok(Box::new(
                PostgresKernelService::open_postgres_with_tls(
                    database_url,
                    schema,
                    namespace.as_str(),
                    namespace_sidecar_path(sidecar_path, namespace),
                    tls,
                )
                .map_err(HttpError::Api)?,
            )),
        }
    }
}

fn namespace_sqlite_path(data_root: &Path, namespace: &NamespaceId) -> PathBuf {
    if namespace == &NamespaceId::default() {
        data_root.join("default.sqlite")
    } else {
        data_root.join(format!(
            "namespace-{}.sqlite",
            namespace_file_token(namespace)
        ))
    }
}

fn namespace_sidecar_path(sidecar_path: &Path, namespace: &NamespaceId) -> PathBuf {
    if namespace == &NamespaceId::default() {
        return sidecar_path.to_path_buf();
    }
    let token = namespace_file_token(namespace);
    let stem = sidecar_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("sidecars");
    let file_name = match sidecar_path.extension().and_then(|value| value.to_str()) {
        Some(extension) if !extension.is_empty() => format!("{stem}-{token}.{extension}"),
        _ => format!("{stem}-{token}"),
    };
    match sidecar_path.parent() {
        Some(parent) => parent.join(file_name),
        None => PathBuf::from(file_name),
    }
}

fn namespace_file_token(namespace: &NamespaceId) -> String {
    use std::fmt::Write as _;

    let mut token = String::with_capacity(namespace.as_str().len() * 2);
    for byte in namespace.as_str().as_bytes() {
        write!(&mut token, "{byte:02x}").expect("writing to String cannot fail");
    }
    token
}

#[derive(Clone, Debug)]
struct AuthenticatedPrincipal {
    id: String,
    principal_id: Option<String>,
    token_id: Option<String>,
    policy_context: Option<PolicyContext>,
    policy_bound: bool,
}

#[derive(Clone)]
struct AuditLog {
    entries: Arc<Mutex<Vec<AuditEntry>>>,
    path: Option<PathBuf>,
    writer: Option<mpsc::SyncSender<AuditEntry>>,
}

impl AuditLog {
    fn new(path: Option<PathBuf>, queue_limit: usize) -> Self {
        let entries = Arc::new(Mutex::new(Vec::new()));
        let writer = path.as_ref().map(|path| {
            let (sender, receiver) = mpsc::sync_channel::<AuditEntry>(queue_limit.max(1));
            let writer_path = path.clone();
            let writer_entries = Arc::clone(&entries);
            let spawn = std::thread::Builder::new()
                .name("aether-audit-writer".into())
                .spawn(move || {
                    while let Ok(entry) = receiver.recv() {
                        if let Err(error) = append_audit_entry(&writer_path, &entry) {
                            if let Ok(mut entries) = writer_entries.lock() {
                                entries.push(AuditEntry::audit_failure(&writer_path, &error));
                            }
                        }
                    }
                });
            if let Err(error) = spawn {
                if let Ok(mut entries) = entries.lock() {
                    entries.push(AuditEntry::audit_failure(path, &error));
                }
            }
            sender
        });
        Self {
            entries,
            path,
            writer,
        }
    }

    fn record(&self, entry: AuditEntry) {
        let mut entries = match self.entries.lock() {
            Ok(entries) => entries,
            Err(_) => return,
        };
        entries.push(entry.clone());
        drop(entries);

        if let (Some(path), Some(writer)) = (&self.path, &self.writer) {
            if let Err(error) = writer.try_send(entry) {
                let kind = match error {
                    mpsc::TrySendError::Full(_) => "bounded audit queue is saturated",
                    mpsc::TrySendError::Disconnected(_) => "audit writer is unavailable",
                };
                if let Ok(mut entries) = self.entries.lock() {
                    entries.push(AuditEntry::audit_failure(
                        path,
                        &std::io::Error::other(kind),
                    ));
                }
            }
        }
    }

    fn snapshot(&self) -> Result<Vec<AuditEntry>, HttpError> {
        self.entries
            .lock()
            .map(|entries| entries.clone())
            .map_err(|_| HttpError::LockPoisoned)
    }
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new(None, default_audit_queue_limit())
    }
}

#[derive(Clone, Default)]
struct HttpAuth {
    tokens: HashMap<String, AuthenticatedToken>,
}

impl HttpAuth {
    fn from_config(config: HttpAuthConfig) -> Self {
        let mut tokens = HashMap::new();
        for access in config.tokens {
            let principal_id = if access.principal_id.trim().is_empty() {
                Some(format!("principal:{}", access.principal))
            } else {
                Some(access.principal_id.clone())
            };
            let token_id = if access.token_id.trim().is_empty() {
                Some(format!("token:{}", access.principal))
            } else {
                Some(access.token_id.clone())
            };
            let namespaces = normalized_namespaces(access.namespaces);
            tokens.insert(
                access.token,
                AuthenticatedToken {
                    principal: access.principal,
                    principal_id,
                    token_id,
                    scopes: access.scopes.into_iter().collect(),
                    namespaces,
                    policy_context: access.policy_context,
                    revoked: access.revoked,
                },
            );
        }
        Self { tokens }
    }

    fn authorize(
        &self,
        headers: &HeaderMap,
        required_scope: AuthScope,
        namespace: &NamespaceId,
    ) -> Result<AuthenticatedPrincipal, HttpError> {
        if self.tokens.is_empty() {
            return Ok(AuthenticatedPrincipal {
                id: "anonymous".into(),
                principal_id: None,
                token_id: None,
                policy_context: None,
                policy_bound: false,
            });
        }

        let header = headers.get(AUTHORIZATION).ok_or(HttpError::Unauthorized {
            principal: "anonymous".into(),
            message: "missing bearer token".into(),
        })?;
        let header = header.to_str().map_err(|_| HttpError::Unauthorized {
            principal: "anonymous".into(),
            message: "authorization header is not valid UTF-8".into(),
        })?;
        let token = header
            .strip_prefix("Bearer ")
            .ok_or(HttpError::Unauthorized {
                principal: "anonymous".into(),
                message: "authorization header must use Bearer auth".into(),
            })?;

        let Some(access) = self.tokens.get(token) else {
            return Err(HttpError::Unauthorized {
                principal: "anonymous".into(),
                message: "unknown bearer token".into(),
            });
        };

        if !access.scopes.contains(&required_scope) {
            return Err(HttpError::Forbidden {
                principal: access.principal.clone(),
                message: format!("token lacks {} scope", required_scope.as_str()),
            });
        }
        if !access.namespaces.contains(namespace) {
            return Err(HttpError::Forbidden {
                principal: access.principal.clone(),
                message: format!("token is not allowed for namespace {}", namespace),
            });
        }
        if access.revoked {
            return Err(HttpError::Forbidden {
                principal: access.principal.clone(),
                message: "token is revoked".into(),
            });
        }

        Ok(AuthenticatedPrincipal {
            id: access.principal.clone(),
            principal_id: access.principal_id.clone(),
            token_id: access.token_id.clone(),
            policy_context: access.policy_context.clone(),
            policy_bound: true,
        })
    }

    fn namespace_status(
        &self,
        active_namespaces: &[NamespaceId],
    ) -> Vec<crate::NamespaceStatusSummary> {
        let mut namespaces: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for namespace in active_namespaces {
            namespaces.entry(namespace.to_string()).or_default();
        }
        for token in self.tokens.values() {
            for namespace in &token.namespaces {
                namespaces
                    .entry(namespace.to_string())
                    .or_default()
                    .insert(token.principal.clone());
            }
        }
        if self.tokens.is_empty() {
            for namespace in active_namespaces {
                namespaces
                    .entry(namespace.to_string())
                    .or_default()
                    .insert("anonymous".into());
            }
        }
        namespaces
            .into_iter()
            .map(|(namespace, principals)| crate::NamespaceStatusSummary {
                namespace,
                principals: principals.into_iter().collect(),
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
struct AuthenticatedToken {
    principal: String,
    principal_id: Option<String>,
    token_id: Option<String>,
    scopes: BTreeSet<AuthScope>,
    namespaces: BTreeSet<NamespaceId>,
    policy_context: Option<PolicyContext>,
    revoked: bool,
}

fn normalized_namespaces(namespaces: Vec<NamespaceId>) -> BTreeSet<NamespaceId> {
    if namespaces.is_empty() {
        BTreeSet::from([NamespaceId::default()])
    } else {
        namespaces.into_iter().collect()
    }
}

fn namespace_from_headers(headers: &HeaderMap) -> Result<NamespaceId, HttpError> {
    let Some(value) = headers.get(AETHER_NAMESPACE_HEADER) else {
        return Ok(NamespaceId::default());
    };
    let value = value.to_str().map_err(|_| {
        HttpError::Api(ApiError::Validation(
            "X-Aether-Namespace header is not valid UTF-8".into(),
        ))
    })?;
    NamespaceId::new(value).map_err(|message| {
        HttpError::Api(ApiError::Validation(format!(
            "invalid X-Aether-Namespace header: {message}"
        )))
    })
}

fn normalize_policy_context(policy_context: Option<PolicyContext>) -> Option<PolicyContext> {
    match policy_context {
        Some(policy_context) if policy_context.is_empty() => None,
        other => other,
    }
}

fn flatten_replica_status(status: &PartitionStatusResponse) -> Vec<crate::ReplicaStatusSummary> {
    let mut replicas = Vec::new();
    for partition in &status.partitions {
        for replica in &partition.replicas {
            replicas.push(crate::ReplicaStatusSummary {
                partition: partition.partition.to_string(),
                replica_id: replica.replica_id.0,
                leader_replica: partition.leader_replica.0,
                role: match replica.role {
                    crate::ReplicaRole::Leader => "leader".into(),
                    crate::ReplicaRole::Follower => "follower".into(),
                },
                leader_epoch: replica.leader_epoch.0,
                applied_element: replica.applied_element.map(|element| element.0),
                replication_lag: replica.replication_lag,
                healthy: replica.healthy,
                detail: replica.detail.clone(),
            });
        }
    }
    replicas
}

fn bound_policy_context(
    principal: &AuthenticatedPrincipal,
    requested: Option<PolicyContext>,
) -> Result<Option<PolicyContext>, HttpError> {
    let requested = normalize_policy_context(requested);
    if !principal.policy_bound {
        return Ok(requested);
    }

    let granted = normalize_policy_context(principal.policy_context.clone());
    match (granted, requested) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(HttpError::Forbidden {
            principal: principal.id.clone(),
            message: "requested policy context exceeds token policy".into(),
        }),
        (Some(granted), None) => Ok(Some(granted)),
        (Some(granted), Some(requested)) => {
            if requested.subset_of(&granted) {
                Ok(Some(requested))
            } else {
                Err(HttpError::Forbidden {
                    principal: principal.id.clone(),
                    message: "requested policy context exceeds token policy".into(),
                })
            }
        }
    }
}

fn write_policy_context_fields(
    target_capabilities: &mut Vec<String>,
    target_visibilities: &mut Vec<String>,
    policy_context: Option<&PolicyContext>,
) {
    target_capabilities.clear();
    target_visibilities.clear();
    if let Some(policy_context) = policy_context {
        target_capabilities.extend(policy_context.capabilities.iter().cloned());
        target_visibilities.extend(policy_context.visibilities.iter().cloned());
    }
}

fn apply_policy_binding(
    principal: &AuthenticatedPrincipal,
    requested: Option<PolicyContext>,
    context: &mut AuditContext,
) -> Result<Option<PolicyContext>, HttpError> {
    let requested = normalize_policy_context(requested);
    write_policy_context_fields(
        &mut context.requested_capabilities,
        &mut context.requested_visibilities,
        requested.as_ref(),
    );
    write_policy_context_fields(
        &mut context.granted_capabilities,
        &mut context.granted_visibilities,
        principal.policy_context.as_ref(),
    );

    match bound_policy_context(principal, requested.clone()) {
        Ok(effective) => {
            write_policy_context_fields(
                &mut context.effective_capabilities,
                &mut context.effective_visibilities,
                effective.as_ref(),
            );
            context.policy_decision = Some(
                match (
                    normalize_policy_context(principal.policy_context.clone()),
                    requested,
                    effective.clone(),
                ) {
                    (None, None, None) => "public".into(),
                    (None, Some(_), Some(_)) => "request_supplied".into(),
                    (Some(_), None, Some(_)) => "token_default".into(),
                    (Some(granted), Some(requested), Some(_)) if requested == granted => {
                        "request_exact".into()
                    }
                    (Some(_), Some(_), Some(_)) => "request_narrowed".into(),
                    _ => "public".into(),
                },
            );
            Ok(effective)
        }
        Err(error) => {
            context.policy_decision = Some("denied_escalation".into());
            Err(error)
        }
    }
}

#[derive(Debug)]
enum HttpError {
    Api(ApiError),
    Unauthorized { principal: String, message: String },
    Forbidden { principal: String, message: String },
    NamespaceBusy { retry_after_seconds: u64 },
    NamespaceInitializationFailed(String),
    WorkerUnavailable,
    WorkerFailed,
    LockPoisoned,
}

impl HttpError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Api(error) => status_for_api_error(error),
            Self::Unauthorized { .. } => StatusCode::UNAUTHORIZED,
            Self::Forbidden { .. } => StatusCode::FORBIDDEN,
            Self::NamespaceBusy { .. } | Self::WorkerUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::NamespaceInitializationFailed(_) | Self::WorkerFailed => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::LockPoisoned => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn audit_principal(&self) -> String {
        match self {
            Self::Unauthorized { principal, .. } | Self::Forbidden { principal, .. } => {
                principal.clone()
            }
            Self::Api(_)
            | Self::NamespaceBusy { .. }
            | Self::NamespaceInitializationFailed(_)
            | Self::WorkerUnavailable
            | Self::WorkerFailed
            | Self::LockPoisoned => "aether".into(),
        }
    }

    fn audit_message(&self) -> String {
        match self {
            Self::Api(error) => error.to_string(),
            Self::Unauthorized { message, .. } | Self::Forbidden { message, .. } => message.clone(),
            Self::NamespaceBusy { .. } => "namespace work queue is saturated".into(),
            Self::NamespaceInitializationFailed(message) => message.clone(),
            Self::WorkerUnavailable => "namespace worker executor is unavailable".into(),
            Self::WorkerFailed => "namespace worker failed".into(),
            Self::LockPoisoned => "internal service state is unavailable".into(),
        }
    }
}

impl From<ApiError> for HttpError {
    fn from(value: ApiError) -> Self {
        Self::Api(value)
    }
}

fn is_unknown_trace_error(error: &HttpError) -> bool {
    matches!(
        error,
        HttpError::Api(ApiError::Execution(
            crate::execution::ExecutionError::UnknownTraceHandle
        ))
    )
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StructuredErrorResponse {
    pub error: String,
    pub code: String,
    pub request_id: String,
    pub details: serde_json::Value,
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let request_id = current_request_id();
        let details = http_error_details(&self);
        let retry_after = match &self {
            Self::NamespaceBusy {
                retry_after_seconds,
            } => Some(*retry_after_seconds),
            _ => None,
        };
        let (error, code) = match self {
            Self::Api(error) => {
                let code = api_error_code(&error);
                (error.to_string(), code)
            }
            Self::Unauthorized { message, .. } => (message, "unauthorized"),
            Self::Forbidden { message, .. } => (message, "forbidden"),
            Self::NamespaceBusy { .. } => {
                ("namespace work queue is saturated".into(), "namespace_busy")
            }
            Self::NamespaceInitializationFailed(message) => {
                (message, "namespace_initialization_failed")
            }
            Self::WorkerUnavailable => (
                "namespace worker executor is unavailable".into(),
                "namespace_worker_unavailable",
            ),
            Self::WorkerFailed => ("namespace worker failed".into(), "namespace_worker_failed"),
            Self::LockPoisoned => (
                "internal service state is unavailable".into(),
                "service_state_unavailable",
            ),
        };

        let mut response = (
            status,
            Json(StructuredErrorResponse {
                error,
                code: code.into(),
                request_id,
                details,
            }),
        )
            .into_response();
        if let Some(seconds) = retry_after {
            if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
                response.headers_mut().insert(RETRY_AFTER, value);
            }
        }
        response
    }
}

fn http_error_details(error: &HttpError) -> serde_json::Value {
    match error {
        HttpError::Api(ApiError::Admission(crate::admission::AdmissionError::SchemaMismatch {
            expected,
            provided,
        })) => serde_json::json!({
            "expected_schema_ref": expected,
            "provided_schema_ref": provided,
        }),
        HttpError::Api(ApiError::Admission(crate::admission::AdmissionError::UnknownSchema(
            schema_ref,
        ))) => serde_json::json!({ "schema_ref": schema_ref }),
        HttpError::Api(ApiError::Admission(crate::admission::AdmissionError::Storage(
            aether_storage::JournalError::StaleCut { expected, actual },
        )))
        | HttpError::Api(ApiError::Journal(aether_storage::JournalError::StaleCut {
            expected,
            actual,
        })) => serde_json::json!({
            "expected_cut": expected,
            "actual_cut": actual,
        }),
        HttpError::NamespaceBusy {
            retry_after_seconds,
        } => serde_json::json!({ "retry_after_seconds": retry_after_seconds }),
        _ => serde_json::json!({}),
    }
}

fn api_error_code(error: &ApiError) -> &'static str {
    match error {
        ApiError::AmbiguousTupleReference => "ambiguous_tuple_reference",
        ApiError::Execution(crate::execution::ExecutionError::MalformedTraceHandle) => {
            "malformed_trace_handle"
        }
        ApiError::Execution(crate::execution::ExecutionError::UnknownTraceHandle) => {
            "unknown_trace_handle"
        }
        ApiError::Execution(crate::execution::ExecutionError::ExpiredTraceHandle) => {
            "expired_trace_handle"
        }
        ApiError::Execution(crate::execution::ExecutionError::InsufficientPolicy) => {
            "insufficient_policy"
        }
        ApiError::Execution(_) => "execution_integrity_failure",
        ApiError::Admission(error) => match error {
            crate::admission::AdmissionError::SchemaMismatch { .. }
            | crate::admission::AdmissionError::NoActiveSchema
            | crate::admission::AdmissionError::UnknownSchema(_)
            | crate::admission::AdmissionError::SchemaActivationPrecondition
            | crate::admission::AdmissionError::Storage(
                aether_storage::JournalError::StaleSchemaActivation
                | aether_storage::JournalError::UnknownSchemaDigest(_)
                | aether_storage::JournalError::ActiveSchemaChanged { .. },
            ) => "schema_mismatch",
            crate::admission::AdmissionError::ExistingHistoryQuarantined(_) => {
                "history_quarantined"
            }
            crate::admission::AdmissionError::Storage(aether_storage::JournalError::StaleCut {
                ..
            }) => "stale_cut",
            crate::admission::AdmissionError::Storage(
                aether_storage::JournalError::IdempotencyConflict(_),
            ) => "idempotency_conflict",
            _ => "append_validation_failed",
        },
        ApiError::Journal(_) => "journal_conflict",
        ApiError::Validation(_) => "validation_error",
        ApiError::Sidecar(_) => "sidecar_error",
        ApiError::Resolve(_) => "resolve_error",
        ApiError::Parse(_) => "parse_error",
        ApiError::Compile(_) => "compile_error",
        ApiError::Runtime(_) => "runtime_error",
        ApiError::Explain(_) => "explain_error",
    }
}

fn status_for_api_error(error: &ApiError) -> StatusCode {
    match error {
        ApiError::AmbiguousTupleReference => StatusCode::CONFLICT,
        ApiError::Validation(_)
        | ApiError::Sidecar(_)
        | ApiError::Resolve(_)
        | ApiError::Parse(_)
        | ApiError::Compile(_)
        | ApiError::Runtime(_)
        | ApiError::Explain(_) => StatusCode::BAD_REQUEST,
        ApiError::Journal(_) => StatusCode::CONFLICT,
        ApiError::Admission(error) => match error {
            crate::admission::AdmissionError::SchemaMismatch { .. }
            | crate::admission::AdmissionError::NoActiveSchema
            | crate::admission::AdmissionError::UnknownSchema(_)
            | crate::admission::AdmissionError::SchemaActivationPrecondition
            | crate::admission::AdmissionError::ExistingHistoryQuarantined(_)
            | crate::admission::AdmissionError::ReplicationReceiptMismatch
            | crate::admission::AdmissionError::Storage(
                aether_storage::JournalError::StaleCut { .. }
                | aether_storage::JournalError::StaleSchemaActivation
                | aether_storage::JournalError::UnknownSchemaDigest(_)
                | aether_storage::JournalError::IdempotencyConflict(_)
                | aether_storage::JournalError::ActiveSchemaChanged { .. },
            ) => StatusCode::CONFLICT,
            _ => StatusCode::BAD_REQUEST,
        },
        ApiError::Execution(error) => match error {
            crate::execution::ExecutionError::MalformedTraceHandle => StatusCode::BAD_REQUEST,
            crate::execution::ExecutionError::UnknownTraceHandle => StatusCode::NOT_FOUND,
            crate::execution::ExecutionError::ExpiredTraceHandle => StatusCode::GONE,
            crate::execution::ExecutionError::InsufficientPolicy => StatusCode::FORBIDDEN,
            crate::execution::ExecutionError::CorruptedExecutionManifest
            | crate::execution::ExecutionError::CorruptedTraceRecord
            | crate::execution::ExecutionError::IncompatibleEngineSemantics
            | crate::execution::ExecutionError::ReplayMismatch
            | crate::execution::ExecutionError::Resolve(_)
            | crate::execution::ExecutionError::Runtime(_)
            | crate::execution::ExecutionError::Explain(_)
            | crate::execution::ExecutionError::Serde(_)
            | crate::execution::ExecutionError::Store(_) => StatusCode::CONFLICT,
        },
    }
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse::default())
}

async fn service_status(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
) -> Result<Json<ServiceStatusResponse>, HttpError> {
    let namespace = namespace_from_headers(&headers)?;
    let context = AuditContext {
        namespace: Some(namespace.to_string()),
        temporal_view: Some("service_status".into()),
        ..Default::default()
    };
    let principal = match state.authorize(&headers, AuthScope::Ops, &namespace) {
        Ok(principal) => principal,
        Err(error) => {
            state.audit.record(AuditEntry::for_denied(
                "GET",
                "/v1/status",
                error.status_code(),
                error.audit_principal(),
                None,
                None,
                AuthScope::Ops,
                error.audit_message(),
                context,
            ));
            return Err(error);
        }
    };
    let response = state.status_snapshot()?;
    let mut response = response;
    response.effective_namespace = Some(namespace.to_string());
    state.audit.record(AuditEntry::for_request(
        "GET",
        "/v1/status",
        StatusCode::OK,
        &principal,
        AuthScope::Ops,
        AuditContext {
            namespace: Some(namespace.to_string()),
            command_source: Some("http".into()),
            selected_report: Some("service_status".into()),
            temporal_view: Some("service_status".into()),
            ..Default::default()
        },
    ));
    Ok(Json(response))
}

async fn history(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
) -> Result<Json<crate::HistoryResponse>, HttpError> {
    let request_context = AuditContext {
        command_source: Some("http".into()),
        selected_report: Some("history".into()),
        temporal_view: Some("history".into()),
        ..Default::default()
    };
    let response = state
        .execute(
            &headers,
            "GET",
            "/v1/history",
            AuthScope::Ops,
            request_context.clone(),
            move |service, principal, context| {
                let policy_context = apply_policy_binding(principal, None, context)?;
                let response = service
                    .history(HistoryRequest { policy_context })
                    .map_err(HttpError::Api)?;
                context.datom_count = Some(response.datoms.len());
                context.last_element = response.datoms.last().map(|datom| datom.element.0);
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn audit_log(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
) -> Result<Json<AuditLogResponse>, HttpError> {
    Ok(Json(state.audit_entries(&headers)?))
}

async fn reload_auth(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
) -> Result<Json<AuthReloadResponse>, HttpError> {
    let namespace = namespace_from_headers(&headers)?;
    let context = AuditContext {
        namespace: Some(namespace.to_string()),
        temporal_view: Some("auth_reload".into()),
        ..Default::default()
    };
    let principal = match state.authorize(&headers, AuthScope::Ops, &namespace) {
        Ok(principal) => principal,
        Err(error) => {
            state.audit.record(AuditEntry::for_denied(
                "POST",
                "/v1/admin/auth/reload",
                error.status_code(),
                error.audit_principal(),
                None,
                None,
                AuthScope::Ops,
                error.audit_message(),
                context,
            ));
            return Err(error);
        }
    };
    let response = state.reload_auth_from_config()?;
    state.audit.record(AuditEntry::for_request(
        "POST",
        "/v1/admin/auth/reload",
        StatusCode::OK,
        &principal,
        AuthScope::Ops,
        AuditContext {
            namespace: Some(namespace.to_string()),
            temporal_view: Some("auth_reload".into()),
            ..Default::default()
        },
    ));
    Ok(Json(response))
}

async fn append(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<AppendAdmissionRequest>,
) -> Result<Json<crate::AppendReceipt>, HttpError> {
    let request_context = audit_context_for_append(&request);
    let response = state
        .execute(
            &headers,
            "POST",
            "/v1/append",
            AuthScope::Append,
            request_context.clone(),
            move |service, principal, _context| {
                let mut request = request;
                request.principal = Some(principal.id.clone());
                let response = service.admit_append(request).map_err(HttpError::Api)?;
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn append_dry_run(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<AppendAdmissionRequest>,
) -> Result<Json<crate::AppendDryRunResponse>, HttpError> {
    let request_context = audit_context_for_append(&request);
    let response = state
        .execute(
            &headers,
            "POST",
            "/v1/append/dry-run",
            AuthScope::Append,
            request_context,
            move |service, principal, _context| {
                let mut request = request;
                request.principal = Some(principal.id.clone());
                service.dry_run_append(request).map_err(HttpError::Api)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn append_receipts_endpoint(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
) -> Result<Json<Vec<crate::AppendReceipt>>, HttpError> {
    let response = state
        .execute(
            &headers,
            "GET",
            "/v1/append/receipts",
            AuthScope::Ops,
            AuditContext {
                temporal_view: Some("append_receipts".into()),
                ..Default::default()
            },
            move |service, _principal, _context| service.append_receipts().map_err(HttpError::Api),
        )
        .await?;
    Ok(Json(response))
}

async fn schema_catalog_endpoint(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
) -> Result<Json<crate::SchemaCatalogResponse>, HttpError> {
    let response = state
        .execute(
            &headers,
            "GET",
            "/v1/schema",
            AuthScope::Query,
            AuditContext {
                temporal_view: Some("schema_catalog".into()),
                ..Default::default()
            },
            move |service, _principal, _context| service.schema_catalog().map_err(HttpError::Api),
        )
        .await?;
    Ok(Json(response))
}

async fn register_schema_endpoint(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<RegisterSchemaRequest>,
) -> Result<Json<crate::NamespaceSchemaRevision>, HttpError> {
    let response = state
        .execute(
            &headers,
            "POST",
            "/v1/schema/register",
            AuthScope::Ops,
            AuditContext {
                temporal_view: Some("schema_register".into()),
                ..Default::default()
            },
            move |service, _principal, _context| {
                service.register_schema(request).map_err(HttpError::Api)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn activate_schema_endpoint(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<ActivateSchemaRequest>,
) -> Result<Json<crate::NamespaceSchemaRevision>, HttpError> {
    let response = state
        .execute(
            &headers,
            "POST",
            "/v1/schema/activate",
            AuthScope::Ops,
            AuditContext {
                temporal_view: Some("schema_activate".into()),
                ..Default::default()
            },
            move |service, _principal, _context| {
                service.activate_schema(request).map_err(HttpError::Api)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn current_state(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<CurrentStateRequest>,
) -> Result<Json<crate::CurrentStateResponse>, HttpError> {
    let request_context = AuditContext {
        temporal_view: Some("current".into()),
        datom_count: Some(request.datoms.len()),
        ..Default::default()
    };
    let response = state
        .execute(
            &headers,
            "POST",
            "/v1/state/current",
            AuthScope::Query,
            request_context.clone(),
            move |service, principal, context| {
                let mut request = request;
                request.policy_context =
                    apply_policy_binding(principal, request.policy_context, context)?;
                let response = service.current_state(request).map_err(HttpError::Api)?;
                context.entity_count = Some(response.state.entities.len());
                context.last_element = response.state.as_of.map(|element| element.0);
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn as_of(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<AsOfRequest>,
) -> Result<Json<crate::AsOfResponse>, HttpError> {
    let request_context = AuditContext {
        temporal_view: Some(format!("as_of(e{})", request.at.0)),
        requested_element: Some(request.at.0),
        datom_count: Some(request.datoms.len()),
        ..Default::default()
    };
    let response = state
        .execute(
            &headers,
            "POST",
            "/v1/state/as-of",
            AuthScope::Query,
            request_context.clone(),
            move |service, principal, context| {
                let mut request = request;
                request.policy_context =
                    apply_policy_binding(principal, request.policy_context, context)?;
                let response = service.as_of(request).map_err(HttpError::Api)?;
                context.entity_count = Some(response.state.entities.len());
                context.last_element = response.state.as_of.map(|element| element.0);
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn parse_document(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<ParseDocumentRequest>,
) -> Result<Json<crate::ParseDocumentResponse>, HttpError> {
    let request_context = audit_context_for_document(&request.dsl);
    let response = state
        .execute(
            &headers,
            "POST",
            "/v1/documents/parse",
            AuthScope::Query,
            request_context.clone(),
            move |service, _principal, _context| {
                let response = service.parse_document(request).map_err(HttpError::Api)?;
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn run_document(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<RunDocumentRequest>,
) -> Result<Json<crate::RunDocumentResponse>, HttpError> {
    let request_context = audit_context_for_document(&request.dsl);
    let response = state
        .execute(
            &headers,
            "POST",
            "/v1/documents/run",
            AuthScope::Query,
            request_context.clone(),
            move |service, principal, context| {
                let mut request = request;
                request.policy_context =
                    apply_policy_binding(principal, request.policy_context, context)?;
                let response = service.run_document(request).map_err(HttpError::Api)?;
                context.entity_count = Some(response.state.entities.len());
                context.last_element = response.state.as_of.map(|element| element.0);
                context.derived_tuple_count = Some(response.derived.tuples.len());
                context.row_count = response.query.as_ref().map(|query| query.rows.len());
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn coordination_pilot_report(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<CoordinationPilotReportRequest>,
) -> Result<Json<crate::CoordinationPilotReport>, HttpError> {
    let request_context = AuditContext {
        command_source: Some("http".into()),
        temporal_view: Some("coordination_pilot_report".into()),
        selected_report: Some("coordination_pilot".into()),
        selected_cut: Some("current".into()),
        ..Default::default()
    };
    let response = state
        .execute(
            &headers,
            "POST",
            "/v1/reports/pilot/coordination",
            AuthScope::Query,
            request_context.clone(),
            move |service, principal, context| {
                let mut request = request;
                request.policy_context =
                    apply_policy_binding(principal, request.policy_context, context)?;
                let response = service
                    .coordination_pilot_report(request)
                    .map_err(HttpError::Api)?;
                context.datom_count = Some(response.history_len);
                context.row_count = Some(
                    response.pre_heartbeat_authorized.len()
                        + response.as_of_authorized.len()
                        + response.live_heartbeats.len()
                        + response.current_authorized.len()
                        + response.claimable.len()
                        + response.accepted_outcomes.len()
                        + response.rejected_outcomes.len(),
                );
                context.trace_tuple_count = response.trace.as_ref().map(|trace| trace.tuple_count);
                context.tuple_id = response.trace.as_ref().map(|trace| trace.root.0);
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn coordination_delta_report(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<CoordinationDeltaReportRequest>,
) -> Result<Json<crate::CoordinationDeltaReport>, HttpError> {
    let request_context = AuditContext {
        command_source: Some("http".into()),
        temporal_view: Some("coordination_delta_report".into()),
        selected_report: Some("coordination_delta".into()),
        selected_cut: Some(format!(
            "{} -> {}",
            coordination_cut_label(&request.left),
            coordination_cut_label(&request.right)
        )),
        ..Default::default()
    };
    let response = state
        .execute(
            &headers,
            "POST",
            "/v1/reports/pilot/coordination-delta",
            AuthScope::Query,
            request_context,
            move |service, principal, context| {
                let mut request = request;
                request.policy_context =
                    apply_policy_binding(principal, request.policy_context, context)?;
                let response = service
                    .coordination_delta_report(request)
                    .map_err(HttpError::Api)?;
                context.datom_count = Some(response.right_history_len);
                context.row_count = Some(
                    response.current_authorized.added.len()
                        + response.current_authorized.removed.len()
                        + response.current_authorized.changed.len()
                        + response.claimable.added.len()
                        + response.claimable.removed.len()
                        + response.claimable.changed.len()
                        + response.live_heartbeats.added.len()
                        + response.live_heartbeats.removed.len()
                        + response.live_heartbeats.changed.len()
                        + response.accepted_outcomes.added.len()
                        + response.accepted_outcomes.removed.len()
                        + response.accepted_outcomes.changed.len()
                        + response.rejected_outcomes.added.len()
                        + response.rejected_outcomes.removed.len()
                        + response.rejected_outcomes.changed.len(),
                );
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn partition_status(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
) -> Result<Json<PartitionStatusResponse>, HttpError> {
    let response = state
        .execute_partitioned(
            &headers,
            "GET",
            "/v1/partitions/status",
            AuthScope::Ops,
            AuditContext {
                command_source: Some("http".into()),
                temporal_view: Some("partition_status".into()),
                selected_report: Some("partition_status".into()),
                ..Default::default()
            },
            move |service, _principal, context| {
                let response = service.partition_status().map_err(HttpError::Api)?;
                context.entity_count = Some(response.partitions.len());
                context.row_count = Some(
                    response
                        .partitions
                        .iter()
                        .map(|partition| partition.replicas.len())
                        .sum(),
                );
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn promote_replica(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<PromoteReplicaRequest>,
) -> Result<Json<crate::PromoteReplicaResponse>, HttpError> {
    let request_context = AuditContext {
        temporal_view: Some(format!("partition({})", request.partition)),
        ..Default::default()
    };
    let response = state
        .execute_partitioned(
            &headers,
            "POST",
            "/v1/partitions/promote",
            AuthScope::Ops,
            request_context,
            move |service, _principal, context| {
                let response = service.promote_replica(request).map_err(HttpError::Api)?;
                context.requested_element = Some(response.leader_epoch.0);
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn partition_append(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(mut request): Json<PartitionAppendRequest>,
) -> Result<Json<crate::PartitionAppendResponse>, HttpError> {
    let request_context = AuditContext {
        temporal_view: Some(format!("partition({})", request.partition)),
        datom_count: Some(request.datoms.len()),
        last_element: request.datoms.last().map(|datom| datom.element.0),
        ..Default::default()
    };
    let response = state
        .execute_partitioned(
            &headers,
            "POST",
            "/v1/partitions/append",
            AuthScope::Append,
            request_context,
            move |service, principal, context| {
                request.principal = Some(principal.id.clone());
                let response = service.append_partition(request).map_err(HttpError::Api)?;
                context.requested_element = response.leader_epoch.as_ref().map(|epoch| epoch.0);
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn partition_history(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<PartitionHistoryRequest>,
) -> Result<Json<crate::PartitionHistoryResponse>, HttpError> {
    let request_context = AuditContext {
        temporal_view: Some(request.cut.to_string()),
        requested_element: request.cut.as_of.map(|element| element.0),
        ..Default::default()
    };
    let response = state
        .execute_partitioned(
            &headers,
            "POST",
            "/v1/partitions/history",
            AuthScope::Query,
            request_context,
            move |service, principal, context| {
                let mut request = request;
                request.policy_context =
                    apply_policy_binding(principal, request.policy_context, context)?;
                let response = service.partition_history(request).map_err(HttpError::Api)?;
                context.datom_count = Some(response.datoms.len());
                context.entity_count = Some(
                    response
                        .datoms
                        .iter()
                        .map(|datom| datom.entity)
                        .collect::<BTreeSet<_>>()
                        .len(),
                );
                context.last_element = response.datoms.last().map(|datom| datom.element.0);
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn partition_state(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<PartitionStateRequest>,
) -> Result<Json<crate::PartitionStateResponse>, HttpError> {
    let request_context = AuditContext {
        temporal_view: Some(request.cut.to_string()),
        requested_element: request.cut.as_of.map(|element| element.0),
        ..Default::default()
    };
    let response = state
        .execute_partitioned(
            &headers,
            "POST",
            "/v1/partitions/state",
            AuthScope::Query,
            request_context,
            move |service, principal, context| {
                let mut request = request;
                request.policy_context =
                    apply_policy_binding(principal, request.policy_context, context)?;
                let response = service.partition_state(request).map_err(HttpError::Api)?;
                context.entity_count = Some(response.state.entities.len());
                context.last_element = response.cut.as_of.map(|element| element.0);
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn federated_history(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<FederatedHistoryRequest>,
) -> Result<Json<crate::FederatedHistoryResponse>, HttpError> {
    let request_context = AuditContext {
        temporal_view: Some("federated_history".into()),
        ..Default::default()
    };
    let response = state
        .execute_partitioned(
            &headers,
            "POST",
            "/v1/federated/history",
            AuthScope::Query,
            request_context,
            move |service, principal, context| {
                let mut request = request;
                request.policy_context =
                    apply_policy_binding(principal, request.policy_context, context)?;
                let response = service.federated_history(request).map_err(HttpError::Api)?;
                context.datom_count = Some(
                    response
                        .partitions
                        .iter()
                        .map(|partition| partition.datoms.len())
                        .sum(),
                );
                context.entity_count = Some(response.partitions.len());
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn federated_run_document(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<FederatedRunDocumentRequest>,
) -> Result<Json<crate::FederatedRunDocumentResponse>, HttpError> {
    let request_context = AuditContext {
        command_source: Some("http".into()),
        temporal_view: Some("federated_run_document".into()),
        selected_report: Some("federated_run".into()),
        ..Default::default()
    };
    let response = state
        .execute_partitioned(
            &headers,
            "POST",
            "/v1/federated/run",
            AuthScope::Query,
            request_context,
            move |service, principal, context| {
                let mut request = request;
                request.policy_context =
                    apply_policy_binding(principal, request.policy_context, context)?;
                let response = service
                    .federated_run_document(request)
                    .map_err(HttpError::Api)?;
                context.entity_count = Some(response.cut.cuts.len());
                context.row_count = Some(
                    response
                        .run
                        .query
                        .as_ref()
                        .map(|query| query.rows.len())
                        .unwrap_or(0),
                );
                context.derived_tuple_count = Some(response.run.derived.tuples.len());
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn federated_report(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<FederatedRunDocumentRequest>,
) -> Result<Json<FederatedExplainReport>, HttpError> {
    let request_context = AuditContext {
        command_source: Some("http".into()),
        temporal_view: Some("federated_report".into()),
        selected_report: Some("federated_report".into()),
        ..Default::default()
    };
    let response = state
        .execute_partitioned(
            &headers,
            "POST",
            "/v1/federated/report",
            AuthScope::Explain,
            request_context,
            move |service, principal, context| {
                let mut request = request;
                request.policy_context =
                    apply_policy_binding(principal, request.policy_context, context)?;
                let response = service
                    .build_federated_explain_report(request)
                    .map_err(HttpError::Api)?;
                context.entity_count = Some(response.cut.cuts.len());
                context.row_count = Some(
                    response.primary_query.len()
                        + response
                            .named_queries
                            .iter()
                            .map(|query| query.rows.len())
                            .sum::<usize>(),
                );
                context.trace_tuple_count =
                    Some(response.traces.iter().map(|trace| trace.tuple_count).sum());
                context.tuple_id = response.traces.first().map(|trace| trace.root.0);
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn explain_tuple(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<ExplainTupleRequest>,
) -> Result<Json<crate::ExplainTupleResponse>, HttpError> {
    let request_context = AuditContext {
        tuple_id: Some(request.tuple_id.0),
        command_source: Some("http".into()),
        selected_report: Some("tuple_explain".into()),
        legacy_endpoint: true,
        ..Default::default()
    };
    let response = state
        .execute(
            &headers,
            "POST",
            "/v1/explain/tuple",
            AuthScope::Explain,
            request_context.clone(),
            move |service, principal, context| {
                let mut request = request;
                request.policy_context =
                    apply_policy_binding(principal, request.policy_context, context)?;
                let response = service.explain_tuple(request).map_err(HttpError::Api)?;
                context.trace_tuple_count = Some(response.trace.tuples.len());
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn resolve_trace_handle(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<ResolveTraceHandleRequest>,
) -> Result<Json<crate::ResolveTraceHandleResponse>, HttpError> {
    let request_context = AuditContext {
        command_source: Some("http".into()),
        selected_report: Some("trace_handle_resolve".into()),
        ..Default::default()
    };
    let response = state
        .resolve_execution_trace(&headers, request, request_context)
        .await?;
    Ok(Json(response))
}

async fn register_artifact_reference(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<RegisterArtifactReferenceRequest>,
) -> Result<Json<crate::RegisterArtifactReferenceResponse>, HttpError> {
    let request_context = AuditContext {
        temporal_view: Some("sidecar_artifact_register".into()),
        requested_element: Some(request.reference.registered_at.0),
        ..Default::default()
    };
    let response = state
        .execute(
            &headers,
            "POST",
            "/v1/sidecars/artifacts/register",
            AuthScope::Append,
            request_context.clone(),
            move |service, _principal, _context| {
                let response = service
                    .register_artifact_reference(request)
                    .map_err(HttpError::Api)?;
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn get_artifact_reference(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<GetArtifactReferenceRequest>,
) -> Result<Json<crate::GetArtifactReferenceResponse>, HttpError> {
    let request_context = AuditContext {
        temporal_view: Some("sidecar_artifact_lookup".into()),
        ..Default::default()
    };
    let response = state
        .execute(
            &headers,
            "POST",
            "/v1/sidecars/artifacts/get",
            AuthScope::Query,
            request_context.clone(),
            move |service, principal, context| {
                let mut request = request;
                request.policy_context =
                    apply_policy_binding(principal, request.policy_context, context)?;
                let response = service
                    .get_artifact_reference(request)
                    .map_err(HttpError::Api)?;
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn register_vector_record(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<RegisterVectorRecordRequest>,
) -> Result<Json<crate::RegisterVectorRecordResponse>, HttpError> {
    let request_context = AuditContext {
        temporal_view: Some("sidecar_vector_register".into()),
        requested_element: Some(request.record.registered_at.0),
        ..Default::default()
    };
    let response = state
        .execute(
            &headers,
            "POST",
            "/v1/sidecars/vectors/register",
            AuthScope::Append,
            request_context.clone(),
            move |service, _principal, _context| {
                let response = service
                    .register_vector_record(request)
                    .map_err(HttpError::Api)?;
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

async fn search_vectors(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<SearchVectorsRequest>,
) -> Result<Json<crate::SearchVectorsResponse>, HttpError> {
    let request_context = AuditContext {
        temporal_view: Some("sidecar_vector_search".into()),
        requested_element: request.as_of.map(|element| element.0),
        ..Default::default()
    };
    let response = state
        .execute(
            &headers,
            "POST",
            "/v1/sidecars/vectors/search",
            AuthScope::Query,
            request_context.clone(),
            move |service, principal, context| {
                let mut request = request;
                request.policy_context =
                    apply_policy_binding(principal, request.policy_context, context)?;
                let response = service.search_vectors(request).map_err(HttpError::Api)?;
                context.row_count = Some(response.matches.len());
                Ok(response)
            },
        )
        .await?;
    Ok(Json(response))
}

fn audit_context_for_append(request: &AppendAdmissionRequest) -> AuditContext {
    AuditContext {
        command_source: Some("http".into()),
        datom_count: Some(request.datoms.len()),
        last_element: request.datoms.last().map(|datom| datom.element.0),
        schema_ref_omitted: request.schema_ref.is_none(),
        ..Default::default()
    }
}

fn coordination_cut_label(cut: &CoordinationCut) -> String {
    match cut {
        CoordinationCut::Current => "current".into(),
        CoordinationCut::AsOf { element } => format!("as_of(e{})", element.0),
    }
}

fn audit_context_for_document(dsl: &str) -> AuditContext {
    let summary = summarize_document_dsl(dsl);
    AuditContext {
        command_source: Some("http".into()),
        temporal_view: summary.temporal_view,
        query_goal: summary.query_goal,
        requested_element: summary.requested_element,
        ..Default::default()
    }
}

#[derive(Default)]
struct DocumentAuditSummary {
    temporal_view: Option<String>,
    query_goal: Option<String>,
    requested_element: Option<u64>,
}

fn summarize_document_dsl(dsl: &str) -> DocumentAuditSummary {
    let mut summary = DocumentAuditSummary::default();
    let mut in_query = false;

    for line in dsl.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !in_query {
            if trimmed.starts_with("query") && trimmed.ends_with('{') {
                in_query = true;
            }
            continue;
        }

        if trimmed == "}" {
            break;
        }

        if summary.temporal_view.is_none() {
            if trimmed == "current" {
                summary.temporal_view = Some("current".into());
                continue;
            }
            if let Some(element) = trimmed.strip_prefix("as_of ") {
                summary.temporal_view = Some(format!("as_of({})", element.trim()));
                summary.requested_element = element
                    .trim()
                    .strip_prefix('e')
                    .and_then(|value| value.parse::<u64>().ok());
                continue;
            }
        }

        if summary.query_goal.is_none() {
            if let Some(goal) = trimmed
                .strip_prefix("goal ")
                .or_else(|| trimmed.strip_prefix("find "))
            {
                summary.query_goal = Some(goal.trim().to_string());
            }
        }
    }

    summary
}

fn append_audit_entry(path: &Path, entry: &AuditEntry) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let json =
        serde_json::to_string(entry).map_err(|error| std::io::Error::other(error.to_string()))?;
    file.write_all(json.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

fn now_millis() -> u64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod concurrency_tests {
    use super::{
        health, AuditContext, AuditEntry, AuditLog, AuthScope, BoundedBlockingExecutor, HeaderMap,
        HttpError, HttpKernelOptions, HttpKernelState, NamespaceServiceDirectory,
        NamespaceServiceState, AUTHORIZATION,
    };
    use crate::{
        NamespaceId, PilotAuthConfig, PilotConcurrencyConfig, PilotHttpTransportConfig,
        PilotServiceConfig, PilotStorageConfig, PilotTokenConfig, ServiceMode,
    };
    use axum::{
        http::{header::RETRY_AFTER, StatusCode},
        response::IntoResponse,
    };
    use std::{
        fs,
        path::PathBuf,
        sync::{mpsc, Arc, Mutex},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    fn directory(label: &str) -> Arc<NamespaceServiceDirectory> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("aether-http-{label}-{nonce}"));
        fs::create_dir_all(&root).expect("create namespace test root");
        Arc::new(NamespaceServiceDirectory::sqlite(root))
    }

    fn audit_entry() -> AuditEntry {
        AuditEntry {
            timestamp_ms: 1,
            principal: "test".into(),
            principal_id: None,
            token_id: None,
            method: "GET".into(),
            path: "/test".into(),
            status: 200,
            scope: AuthScope::Ops,
            outcome: "allowed".into(),
            detail: None,
            context: AuditContext::default(),
        }
    }

    #[test]
    fn audit_backpressure_is_bounded_and_visible() {
        let (writer, receiver) = mpsc::sync_channel(1);
        writer.try_send(audit_entry()).expect("fill audit queue");
        let audit = AuditLog {
            entries: Arc::new(Mutex::new(Vec::new())),
            path: Some(PathBuf::from("audit.jsonl")),
            writer: Some(writer),
        };
        audit.record(audit_entry());
        let entries = audit.snapshot().expect("audit snapshot");
        assert!(entries.iter().any(|entry| {
            entry.outcome == "audit_write_failed"
                && entry
                    .detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("saturated"))
        }));
        drop(receiver);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn blocked_namespace_does_not_delay_another_namespace_or_directory_status() {
        let executor = BoundedBlockingExecutor::new(2, 2);
        let services = directory("independent");
        let blocked_namespace = NamespaceId::new("blocked").expect("namespace");
        let free_namespace = NamespaceId::new("free").expect("namespace");
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);

        let blocked = {
            let executor = executor.clone();
            let services = Arc::clone(&services);
            let namespace = blocked_namespace.clone();
            tokio::spawn(async move {
                executor
                    .run(move || {
                        services.execute(&namespace, |_service| {
                            started_tx.send(()).expect("signal blocked operation");
                            release_rx.recv().expect("release blocked operation");
                            Ok(())
                        })
                    })
                    .await
            })
        };
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("blocked namespace started");

        let active = services.active_namespaces().expect("directory status");
        assert!(active.contains(&blocked_namespace));
        tokio::time::timeout(Duration::from_secs(1), {
            let executor = executor.clone();
            let services = Arc::clone(&services);
            async move {
                executor
                    .run(move || services.execute(&free_namespace, |_service| Ok(17_u8)))
                    .await
            }
        })
        .await
        .expect("free namespace must not wait")
        .expect("free namespace result");

        release_tx.send(()).expect("release blocked namespace");
        blocked
            .await
            .expect("blocked task join")
            .expect("blocked task result");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn saturated_namespace_work_does_not_delay_health_status_or_auth_reload() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("aether-http-control-{nonce}"));
        fs::create_dir_all(&root).expect("create control test root");
        let config_path = root.join("pilot-service.json");
        let data_root = root.join("data");
        let config = PilotServiceConfig {
            config_version: "control-test".into(),
            schema_version: "v1".into(),
            service_mode: ServiceMode::SingleNode,
            bind_addr: "127.0.0.1:3000".into(),
            http_transport: PilotHttpTransportConfig::default(),
            concurrency: PilotConcurrencyConfig {
                namespace_workers: 1,
                namespace_queue: 0,
                audit_queue: 1_024,
            },
            database_path: None,
            storage: Some(PilotStorageConfig::Sqlite {
                data_root: data_root.clone(),
            }),
            audit_log_path: Some(root.join("audit.jsonl")),
            auth: PilotAuthConfig {
                tokens: vec![PilotTokenConfig {
                    principal: "operator".into(),
                    principal_id: Some("principal:operator".into()),
                    token_id: Some("token:operator".into()),
                    scopes: vec![AuthScope::Ops],
                    policy_context: None,
                    token: Some("control-token".into()),
                    token_env: None,
                    token_file: None,
                    token_command: None,
                    namespaces: vec![NamespaceId::default()],
                    revoked: false,
                }],
                revoked_token_ids: Vec::new(),
                revoked_principal_ids: Vec::new(),
            },
        };
        fs::write(
            &config_path,
            serde_json::to_vec_pretty(&config).expect("serialize config"),
        )
        .expect("write config");
        let resolved = config.resolve(&config_path).expect("resolve config");
        let options = HttpKernelOptions::new()
            .with_auth(resolved.auth.clone())
            .with_audit_log_path(resolved.audit_log_path.clone())
            .with_service_status(resolved.service_status())
            .with_auth_reload_config_path(config_path)
            .with_namespace_work_limits(1, 0);
        let state = HttpKernelState::with_sqlite_namespaces(data_root, options);

        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let running = {
            let executor = state.blocking.clone();
            tokio::spawn(async move {
                executor
                    .run(move || {
                        started_tx.send(()).expect("work started");
                        release_rx.recv().expect("release work");
                        Ok(())
                    })
                    .await
            })
        };
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("work saturated executor");

        assert_eq!(health().await.status, "ok");
        assert_eq!(state.status_snapshot().expect("status").status, "ok");
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, "Bearer control-token".parse().unwrap());
        state
            .authorize(&headers, AuthScope::Ops, &NamespaceId::default())
            .expect("authorization unaffected");
        state
            .reload_auth_from_config()
            .expect("auth reload unaffected");

        release_tx.send(()).expect("release work");
        running.await.expect("work join").expect("work result");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn same_namespace_is_ordered_and_initializes_once() {
        let executor = BoundedBlockingExecutor::new(2, 2);
        let services = directory("ordered");
        let namespace = NamespaceId::new("ordered").expect("namespace");
        let order = Arc::new(Mutex::new(Vec::new()));
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);

        let first = {
            let executor = executor.clone();
            let services = Arc::clone(&services);
            let namespace = namespace.clone();
            let order = Arc::clone(&order);
            tokio::spawn(async move {
                executor
                    .run(move || {
                        services.execute(&namespace, |_service| {
                            order.lock().expect("order lock").push(1);
                            started_tx.send(()).expect("first started");
                            release_rx.recv().expect("release first");
                            Ok(())
                        })
                    })
                    .await
            })
        };
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first operation started");
        let second = {
            let executor = executor.clone();
            let services = Arc::clone(&services);
            let namespace = namespace.clone();
            let order = Arc::clone(&order);
            tokio::spawn(async move {
                executor
                    .run(move || {
                        services.execute(&namespace, |_service| {
                            order.lock().expect("order lock").push(2);
                            Ok(())
                        })
                    })
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(*order.lock().expect("order lock"), vec![1]);
        release_tx.send(()).expect("release first");
        first.await.expect("first join").expect("first result");
        second.await.expect("second join").expect("second result");
        assert_eq!(*order.lock().expect("order lock"), vec![1, 2]);

        let handle = services.handle(&namespace).expect("namespace handle");
        assert!(matches!(
            *handle.state.lock().expect("namespace state"),
            NamespaceServiceState::Ready(_)
        ));
        assert!(Arc::ptr_eq(
            &handle,
            &services.handle(&namespace).expect("same namespace handle")
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn executor_saturation_and_panics_fail_boundedly() {
        let saturated = BoundedBlockingExecutor::new(1, 0);
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let running = {
            let executor = saturated.clone();
            tokio::spawn(async move {
                executor
                    .run(move || {
                        started_tx.send(()).expect("worker started");
                        release_rx.recv().expect("release worker");
                        Ok(())
                    })
                    .await
            })
        };
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("worker started");
        assert!(matches!(
            saturated.run(|| Ok(())).await,
            Err(HttpError::NamespaceBusy { .. })
        ));
        let response = HttpError::NamespaceBusy {
            retry_after_seconds: 1,
        }
        .into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get(RETRY_AFTER).unwrap(), "1");
        release_tx.send(()).expect("release worker");
        running
            .await
            .expect("running join")
            .expect("running result");

        let executor = BoundedBlockingExecutor::new(1, 0);
        assert!(matches!(
            executor
                .run(|| -> Result<(), HttpError> { panic!("worker panic") })
                .await,
            Err(HttpError::WorkerFailed)
        ));
        executor
            .run(|| Ok(()))
            .await
            .expect("permit must be released after panic");
    }
}
