use aether_ast::{
    Datom, DerivationTrace, ElementId, PhaseGraph, PlanExplanation, RuleProgram, TupleId,
};
use aether_plan::CompiledProgram;
use aether_resolver::ResolvedState;
use aether_runtime::DerivedSet;
use aether_schema::Schema;
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
        &self,
        request: EvaluateProgramRequest,
    ) -> Result<EvaluateProgramResponse, ApiError>;
    fn explain_tuple(&self, request: ExplainTupleRequest)
        -> Result<ExplainTupleResponse, ApiError>;
    fn explain_plan(&self, request: ExplainPlanRequest) -> Result<ExplainPlanResponse, ApiError>;
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

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("validation error: {0}")]
    Validation(String),
    #[error("unsupported operation: {0}")]
    Unsupported(String),
}
