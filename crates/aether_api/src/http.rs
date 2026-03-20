use crate::{
    ApiError, AppendRequest, AsOfRequest, CurrentStateRequest, ExplainTupleRequest, HistoryRequest,
    KernelService, ParseDocumentRequest, RunDocumentRequest,
};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct HttpKernelState {
    service: Arc<Mutex<Box<dyn KernelService + Send>>>,
}

impl HttpKernelState {
    pub fn new(service: impl KernelService + Send + 'static) -> Self {
        Self {
            service: Arc::new(Mutex::new(Box::new(service))),
        }
    }

    fn service(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Box<dyn KernelService + Send>>, HttpError> {
        self.service.lock().map_err(|_| HttpError::LockPoisoned)
    }
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
    Router::new()
        .route("/health", get(health))
        .route("/v1/history", get(history))
        .route("/v1/append", post(append))
        .route("/v1/state/current", post(current_state))
        .route("/v1/state/as-of", post(as_of))
        .route("/v1/documents/parse", post(parse_document))
        .route("/v1/documents/run", post(run_document))
        .route("/v1/explain/tuple", post(explain_tuple))
        .with_state(HttpKernelState::new(service))
}

#[derive(Debug)]
enum HttpError {
    Api(ApiError),
    LockPoisoned,
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
        let (status, error) = match self {
            Self::Api(error) => (status_for_api_error(&error), error.to_string()),
            Self::LockPoisoned => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal service state is unavailable".into(),
            ),
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
) -> Result<Json<crate::HistoryResponse>, HttpError> {
    let response = state.service()?.history(HistoryRequest)?;
    Ok(Json(response))
}

async fn append(
    State(state): State<HttpKernelState>,
    Json(request): Json<AppendRequest>,
) -> Result<Json<crate::AppendResponse>, HttpError> {
    let response = state.service()?.append(request)?;
    Ok(Json(response))
}

async fn current_state(
    State(state): State<HttpKernelState>,
    Json(request): Json<CurrentStateRequest>,
) -> Result<Json<crate::CurrentStateResponse>, HttpError> {
    let response = state.service()?.current_state(request)?;
    Ok(Json(response))
}

async fn as_of(
    State(state): State<HttpKernelState>,
    Json(request): Json<AsOfRequest>,
) -> Result<Json<crate::AsOfResponse>, HttpError> {
    let response = state.service()?.as_of(request)?;
    Ok(Json(response))
}

async fn parse_document(
    State(state): State<HttpKernelState>,
    Json(request): Json<ParseDocumentRequest>,
) -> Result<Json<crate::ParseDocumentResponse>, HttpError> {
    let response = state.service()?.parse_document(request)?;
    Ok(Json(response))
}

async fn run_document(
    State(state): State<HttpKernelState>,
    Json(request): Json<RunDocumentRequest>,
) -> Result<Json<crate::RunDocumentResponse>, HttpError> {
    let response = state.service()?.run_document(request)?;
    Ok(Json(response))
}

async fn explain_tuple(
    State(state): State<HttpKernelState>,
    Json(request): Json<ExplainTupleRequest>,
) -> Result<Json<crate::ExplainTupleResponse>, HttpError> {
    let response = state.service()?.explain_tuple(request)?;
    Ok(Json(response))
}
