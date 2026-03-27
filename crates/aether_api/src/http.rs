use crate::{
    ApiError, AppendRequest, AsOfRequest, CoordinationPilotReportRequest, CurrentStateRequest,
    ExplainTupleRequest, GetArtifactReferenceRequest, HistoryRequest, KernelService,
    ParseDocumentRequest, RegisterArtifactReferenceRequest, RegisterVectorRecordRequest,
    RunDocumentRequest, SearchVectorsRequest,
};
use aether_ast::PolicyContext;
use axum::{
    extract::State,
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone)]
pub struct HttpKernelState {
    service: Arc<Mutex<Box<dyn KernelService + Send>>>,
    auth: HttpAuth,
    audit: AuditLog,
}

impl HttpKernelState {
    pub fn new(service: impl KernelService + Send + 'static) -> Self {
        Self::with_options(service, HttpKernelOptions::default())
    }

    pub fn with_options(
        service: impl KernelService + Send + 'static,
        options: HttpKernelOptions,
    ) -> Self {
        Self {
            service: Arc::new(Mutex::new(Box::new(service))),
            auth: HttpAuth::from_config(options.auth),
            audit: AuditLog::new(options.audit_log_path),
        }
    }

    fn service(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Box<dyn KernelService + Send>>, HttpError> {
        self.service.lock().map_err(|_| HttpError::LockPoisoned)
    }

    fn authorize(
        &self,
        headers: &HeaderMap,
        required_scope: AuthScope,
    ) -> Result<AuthenticatedPrincipal, HttpError> {
        self.auth.authorize(headers, required_scope)
    }

    fn execute<T, F>(
        &self,
        headers: &HeaderMap,
        method: &'static str,
        path: &'static str,
        required_scope: AuthScope,
        mut context: AuditContext,
        operation: F,
    ) -> Result<T, HttpError>
    where
        F: FnOnce(
            &mut dyn KernelService,
            &AuthenticatedPrincipal,
            &mut AuditContext,
        ) -> Result<T, HttpError>,
    {
        let principal = match self.authorize(headers, required_scope) {
            Ok(principal) => principal,
            Err(error) => {
                self.audit.record(AuditEntry::for_denied(
                    method,
                    path,
                    error.status_code(),
                    error.audit_principal(),
                    required_scope,
                    error.audit_message(),
                    context,
                ));
                return Err(error);
            }
        };

        let result = {
            let mut service = self.service()?;
            operation(service.as_mut(), &principal, &mut context)
        };

        let status = match &result {
            Ok(_) => StatusCode::OK,
            Err(error) => error.status_code(),
        };
        self.audit.record(AuditEntry::for_request(
            method,
            path,
            status,
            principal.id,
            required_scope,
            context,
        ));

        result
    }

    fn audit_entries(&self, headers: &HeaderMap) -> Result<AuditLogResponse, HttpError> {
        let principal = match self.authorize(headers, AuthScope::Ops) {
            Ok(principal) => principal,
            Err(error) => {
                self.audit.record(AuditEntry::for_denied(
                    "GET",
                    "/v1/audit",
                    error.status_code(),
                    error.audit_principal(),
                    AuthScope::Ops,
                    error.audit_message(),
                    AuditContext::default(),
                ));
                return Err(error);
            }
        };

        let response = AuditLogResponse {
            entries: self.audit.snapshot()?,
        };
        self.audit.record(AuditEntry::for_request(
            "GET",
            "/v1/audit",
            StatusCode::OK,
            principal.id,
            AuthScope::Ops,
            AuditContext::default(),
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
            principal: principal.into(),
            scopes: scopes.into_iter().collect(),
            policy_context: None,
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
            principal: principal.into(),
            scopes: scopes.into_iter().collect(),
            policy_context: normalize_policy_context(Some(policy_context)),
        });
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpAccessToken {
    pub token: String,
    pub principal: String,
    pub scopes: Vec<AuthScope>,
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpKernelOptions {
    pub auth: HttpAuthConfig,
    pub audit_log_path: Option<PathBuf>,
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
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub timestamp_ms: u64,
    pub principal: String,
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
}

impl AuditEntry {
    fn for_request(
        method: impl Into<String>,
        path: impl Into<String>,
        status: StatusCode,
        principal: impl Into<String>,
        scope: AuthScope,
        context: AuditContext,
    ) -> Self {
        Self {
            timestamp_ms: now_millis(),
            principal: principal.into(),
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

    fn for_denied(
        method: impl Into<String>,
        path: impl Into<String>,
        status: StatusCode,
        principal: impl Into<String>,
        scope: AuthScope,
        detail: impl Into<String>,
        context: AuditContext,
    ) -> Self {
        Self {
            timestamp_ms: now_millis(),
            principal: principal.into(),
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

pub fn http_router_with_options(
    service: impl KernelService + Send + 'static,
    options: HttpKernelOptions,
) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/history", get(history))
        .route("/v1/audit", get(audit_log))
        .route("/v1/append", post(append))
        .route("/v1/state/current", post(current_state))
        .route("/v1/state/as-of", post(as_of))
        .route("/v1/documents/parse", post(parse_document))
        .route("/v1/documents/run", post(run_document))
        .route(
            "/v1/reports/pilot/coordination",
            post(coordination_pilot_report),
        )
        .route("/v1/explain/tuple", post(explain_tuple))
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
}

#[derive(Clone, Debug)]
struct AuthenticatedPrincipal {
    id: String,
    policy_context: Option<PolicyContext>,
    policy_bound: bool,
}

#[derive(Clone, Default)]
struct AuditLog {
    entries: Arc<Mutex<Vec<AuditEntry>>>,
    path: Option<PathBuf>,
}

impl AuditLog {
    fn new(path: Option<PathBuf>) -> Self {
        Self {
            entries: Arc::new(Mutex::new(Vec::new())),
            path,
        }
    }

    fn record(&self, entry: AuditEntry) {
        let mut entries = match self.entries.lock() {
            Ok(entries) => entries,
            Err(_) => return,
        };
        entries.push(entry.clone());

        if let Some(path) = &self.path {
            if let Err(error) = append_audit_entry(path, &entry) {
                entries.push(AuditEntry::audit_failure(path, &error));
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

#[derive(Clone, Default)]
struct HttpAuth {
    tokens: HashMap<String, AuthenticatedToken>,
}

impl HttpAuth {
    fn from_config(config: HttpAuthConfig) -> Self {
        let mut tokens = HashMap::new();
        for access in config.tokens {
            tokens.insert(
                access.token,
                AuthenticatedToken {
                    principal: access.principal,
                    scopes: access.scopes.into_iter().collect(),
                    policy_context: access.policy_context,
                },
            );
        }
        Self { tokens }
    }

    fn authorize(
        &self,
        headers: &HeaderMap,
        required_scope: AuthScope,
    ) -> Result<AuthenticatedPrincipal, HttpError> {
        if self.tokens.is_empty() {
            return Ok(AuthenticatedPrincipal {
                id: "anonymous".into(),
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

        Ok(AuthenticatedPrincipal {
            id: access.principal.clone(),
            policy_context: access.policy_context.clone(),
            policy_bound: true,
        })
    }
}

#[derive(Clone, Debug)]
struct AuthenticatedToken {
    principal: String,
    scopes: BTreeSet<AuthScope>,
    policy_context: Option<PolicyContext>,
}

fn normalize_policy_context(policy_context: Option<PolicyContext>) -> Option<PolicyContext> {
    match policy_context {
        Some(policy_context) if policy_context.is_empty() => None,
        other => other,
    }
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
    LockPoisoned,
}

impl HttpError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Api(error) => status_for_api_error(error),
            Self::Unauthorized { .. } => StatusCode::UNAUTHORIZED,
            Self::Forbidden { .. } => StatusCode::FORBIDDEN,
            Self::LockPoisoned => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn audit_principal(&self) -> String {
        match self {
            Self::Unauthorized { principal, .. } | Self::Forbidden { principal, .. } => {
                principal.clone()
            }
            Self::Api(_) | Self::LockPoisoned => "aether".into(),
        }
    }

    fn audit_message(&self) -> String {
        match self {
            Self::Api(error) => error.to_string(),
            Self::Unauthorized { message, .. } | Self::Forbidden { message, .. } => message.clone(),
            Self::LockPoisoned => "internal service state is unavailable".into(),
        }
    }
}

impl From<ApiError> for HttpError {
    fn from(value: ApiError) -> Self {
        Self::Api(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let error = match self {
            Self::Api(error) => error.to_string(),
            Self::Unauthorized { message, .. } | Self::Forbidden { message, .. } => message,
            Self::LockPoisoned => "internal service state is unavailable".into(),
        };

        (status, Json(ErrorBody { error })).into_response()
    }
}

fn status_for_api_error(error: &ApiError) -> StatusCode {
    match error {
        ApiError::Validation(_)
        | ApiError::Sidecar(_)
        | ApiError::Resolve(_)
        | ApiError::Parse(_)
        | ApiError::Compile(_)
        | ApiError::Runtime(_)
        | ApiError::Explain(_) => StatusCode::BAD_REQUEST,
        ApiError::Journal(_) => StatusCode::CONFLICT,
    }
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse::default())
}

async fn history(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
) -> Result<Json<crate::HistoryResponse>, HttpError> {
    let request_context = AuditContext {
        temporal_view: Some("history".into()),
        ..Default::default()
    };
    let response = state.execute(
        &headers,
        "GET",
        "/v1/history",
        AuthScope::Ops,
        request_context.clone(),
        |service, principal, context| {
            let policy_context = apply_policy_binding(principal, None, context)?;
            let response = service
                .history(HistoryRequest { policy_context })
                .map_err(HttpError::Api)?;
            context.datom_count = Some(response.datoms.len());
            context.last_element = response.datoms.last().map(|datom| datom.element.0);
            Ok(response)
        },
    )?;
    Ok(Json(response))
}

async fn audit_log(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
) -> Result<Json<AuditLogResponse>, HttpError> {
    Ok(Json(state.audit_entries(&headers)?))
}

async fn append(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<AppendRequest>,
) -> Result<Json<crate::AppendResponse>, HttpError> {
    let request_context = audit_context_for_append(&request);
    let response = state.execute(
        &headers,
        "POST",
        "/v1/append",
        AuthScope::Append,
        request_context.clone(),
        |service, _principal, _context| {
            let response = service.append(request).map_err(HttpError::Api)?;
            Ok(response)
        },
    )?;
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
    let response = state.execute(
        &headers,
        "POST",
        "/v1/state/current",
        AuthScope::Query,
        request_context.clone(),
        |service, principal, context| {
            let mut request = request;
            request.policy_context =
                apply_policy_binding(principal, request.policy_context, context)?;
            let response = service.current_state(request).map_err(HttpError::Api)?;
            context.entity_count = Some(response.state.entities.len());
            context.last_element = response.state.as_of.map(|element| element.0);
            Ok(response)
        },
    )?;
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
    let response = state.execute(
        &headers,
        "POST",
        "/v1/state/as-of",
        AuthScope::Query,
        request_context.clone(),
        |service, principal, context| {
            let mut request = request;
            request.policy_context =
                apply_policy_binding(principal, request.policy_context, context)?;
            let response = service.as_of(request).map_err(HttpError::Api)?;
            context.entity_count = Some(response.state.entities.len());
            context.last_element = response.state.as_of.map(|element| element.0);
            Ok(response)
        },
    )?;
    Ok(Json(response))
}

async fn parse_document(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<ParseDocumentRequest>,
) -> Result<Json<crate::ParseDocumentResponse>, HttpError> {
    let request_context = audit_context_for_document(&request.dsl);
    let response = state.execute(
        &headers,
        "POST",
        "/v1/documents/parse",
        AuthScope::Query,
        request_context.clone(),
        |service, _principal, _context| {
            let response = service.parse_document(request).map_err(HttpError::Api)?;
            Ok(response)
        },
    )?;
    Ok(Json(response))
}

async fn run_document(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<RunDocumentRequest>,
) -> Result<Json<crate::RunDocumentResponse>, HttpError> {
    let request_context = audit_context_for_document(&request.dsl);
    let response = state.execute(
        &headers,
        "POST",
        "/v1/documents/run",
        AuthScope::Query,
        request_context.clone(),
        |service, principal, context| {
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
    )?;
    Ok(Json(response))
}

async fn coordination_pilot_report(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<CoordinationPilotReportRequest>,
) -> Result<Json<crate::CoordinationPilotReport>, HttpError> {
    let request_context = AuditContext {
        temporal_view: Some("coordination_pilot_report".into()),
        ..Default::default()
    };
    let response = state.execute(
        &headers,
        "POST",
        "/v1/reports/pilot/coordination",
        AuthScope::Query,
        request_context.clone(),
        |service, principal, context| {
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
    )?;
    Ok(Json(response))
}

async fn explain_tuple(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<ExplainTupleRequest>,
) -> Result<Json<crate::ExplainTupleResponse>, HttpError> {
    let request_context = AuditContext {
        tuple_id: Some(request.tuple_id.0),
        ..Default::default()
    };
    let response = state.execute(
        &headers,
        "POST",
        "/v1/explain/tuple",
        AuthScope::Explain,
        request_context.clone(),
        |service, principal, context| {
            let mut request = request;
            request.policy_context =
                apply_policy_binding(principal, request.policy_context, context)?;
            let response = service.explain_tuple(request).map_err(HttpError::Api)?;
            context.trace_tuple_count = Some(response.trace.tuples.len());
            Ok(response)
        },
    )?;
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
    let response = state.execute(
        &headers,
        "POST",
        "/v1/sidecars/artifacts/register",
        AuthScope::Append,
        request_context.clone(),
        |service, _principal, _context| {
            let response = service
                .register_artifact_reference(request)
                .map_err(HttpError::Api)?;
            Ok(response)
        },
    )?;
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
    let response = state.execute(
        &headers,
        "POST",
        "/v1/sidecars/artifacts/get",
        AuthScope::Query,
        request_context.clone(),
        |service, principal, context| {
            let mut request = request;
            request.policy_context =
                apply_policy_binding(principal, request.policy_context, context)?;
            let response = service
                .get_artifact_reference(request)
                .map_err(HttpError::Api)?;
            Ok(response)
        },
    )?;
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
    let response = state.execute(
        &headers,
        "POST",
        "/v1/sidecars/vectors/register",
        AuthScope::Append,
        request_context.clone(),
        |service, _principal, _context| {
            let response = service
                .register_vector_record(request)
                .map_err(HttpError::Api)?;
            Ok(response)
        },
    )?;
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
    let response = state.execute(
        &headers,
        "POST",
        "/v1/sidecars/vectors/search",
        AuthScope::Query,
        request_context.clone(),
        |service, principal, context| {
            let mut request = request;
            request.policy_context =
                apply_policy_binding(principal, request.policy_context, context)?;
            let response = service.search_vectors(request).map_err(HttpError::Api)?;
            context.row_count = Some(response.matches.len());
            Ok(response)
        },
    )?;
    Ok(Json(response))
}

fn audit_context_for_append(request: &AppendRequest) -> AuditContext {
    AuditContext {
        datom_count: Some(request.datoms.len()),
        last_element: request.datoms.last().map(|datom| datom.element.0),
        ..Default::default()
    }
}

fn audit_context_for_document(dsl: &str) -> AuditContext {
    let summary = summarize_document_dsl(dsl);
    AuditContext {
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
