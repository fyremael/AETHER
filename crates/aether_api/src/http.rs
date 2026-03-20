use crate::{
    ApiError, AppendRequest, AsOfRequest, CurrentStateRequest, ExplainTupleRequest, HistoryRequest,
    KernelService, ParseDocumentRequest, RunDocumentRequest,
};
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
        operation: F,
    ) -> Result<T, HttpError>
    where
        F: FnOnce(&mut dyn KernelService) -> Result<T, ApiError>,
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
                ));
                return Err(error);
            }
        };

        let result = {
            let mut service = self.service()?;
            operation(service.as_mut())
        };

        let status = match &result {
            Ok(_) => StatusCode::OK,
            Err(error) => status_for_api_error(error),
        };
        self.audit.record(AuditEntry::for_request(
            method,
            path,
            status,
            principal.id,
            required_scope,
        ));

        result.map_err(HttpError::Api)
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
        });
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HttpAccessToken {
    pub token: String,
    pub principal: String,
    pub scopes: Vec<AuthScope>,
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
}

impl AuditEntry {
    fn for_request(
        method: impl Into<String>,
        path: impl Into<String>,
        status: StatusCode,
        principal: impl Into<String>,
        scope: AuthScope,
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
        }
    }

    fn for_denied(
        method: impl Into<String>,
        path: impl Into<String>,
        status: StatusCode,
        principal: impl Into<String>,
        scope: AuthScope,
        detail: impl Into<String>,
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
        .route("/v1/explain/tuple", post(explain_tuple))
        .with_state(HttpKernelState::with_options(service, options))
}

#[derive(Clone, Debug)]
struct AuthenticatedPrincipal {
    id: String,
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
        })
    }
}

#[derive(Clone, Debug)]
struct AuthenticatedToken {
    principal: String,
    scopes: BTreeSet<AuthScope>,
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
    let response = state.execute(&headers, "GET", "/v1/history", AuthScope::Ops, |service| {
        service.history(HistoryRequest)
    })?;
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
    let response = state.execute(
        &headers,
        "POST",
        "/v1/append",
        AuthScope::Append,
        |service| service.append(request),
    )?;
    Ok(Json(response))
}

async fn current_state(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<CurrentStateRequest>,
) -> Result<Json<crate::CurrentStateResponse>, HttpError> {
    let response = state.execute(
        &headers,
        "POST",
        "/v1/state/current",
        AuthScope::Query,
        |service| service.current_state(request),
    )?;
    Ok(Json(response))
}

async fn as_of(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<AsOfRequest>,
) -> Result<Json<crate::AsOfResponse>, HttpError> {
    let response = state.execute(
        &headers,
        "POST",
        "/v1/state/as-of",
        AuthScope::Query,
        |service| service.as_of(request),
    )?;
    Ok(Json(response))
}

async fn parse_document(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<ParseDocumentRequest>,
) -> Result<Json<crate::ParseDocumentResponse>, HttpError> {
    let response = state.execute(
        &headers,
        "POST",
        "/v1/documents/parse",
        AuthScope::Query,
        |service| service.parse_document(request),
    )?;
    Ok(Json(response))
}

async fn run_document(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<RunDocumentRequest>,
) -> Result<Json<crate::RunDocumentResponse>, HttpError> {
    let response = state.execute(
        &headers,
        "POST",
        "/v1/documents/run",
        AuthScope::Query,
        |service| service.run_document(request),
    )?;
    Ok(Json(response))
}

async fn explain_tuple(
    State(state): State<HttpKernelState>,
    headers: HeaderMap,
    Json(request): Json<ExplainTupleRequest>,
) -> Result<Json<crate::ExplainTupleResponse>, HttpError> {
    let response = state.execute(
        &headers,
        "POST",
        "/v1/explain/tuple",
        AuthScope::Explain,
        |service| service.explain_tuple(request),
    )?;
    Ok(Json(response))
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
