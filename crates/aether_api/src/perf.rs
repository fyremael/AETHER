use crate::{
    build_coordination_delta_report, build_coordination_pilot_report,
    coordination_pilot_seed_history, http_router_with_options, ApiError, AppendRequest, AuthScope,
    AuthorityPartitionConfig, CoordinationCut, CoordinationDeltaReportRequest,
    CoordinationPilotReportRequest, CurrentStateRequest, ExplainTupleRequest,
    FederatedHistoryRequest, FederatedRunDocumentRequest, HealthResponse, HttpAuthConfig,
    HttpKernelOptions, ImportedFactQueryRequest, InMemoryKernelService, KernelService, LeaderEpoch,
    PartitionAppendRequest, PromoteReplicaRequest, ReplicaConfig, ReplicaRole,
    ReplicatedAuthorityPartitionService, RunDocumentRequest, ServiceMode, ServiceStatusResponse,
    ServiceStatusStorage, SqliteKernelService, COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT,
};
use aether_ast::{
    Atom, AttributeId, Datom, DatomProvenance, DerivationTrace, ElementId, EntityId, FederatedCut,
    Literal, OperationKind, PartitionCut, PartitionId, PolicyContext, PredicateId, PredicateRef,
    ReplicaId, RuleAst, RuleId, RuleProgram, Term, TupleId, Value, Variable,
};
use aether_explain::{Explainer, InMemoryExplainer};
use aether_plan::CompiledProgram;
use aether_resolver::{MaterializedResolver, ResolvedState, Resolver};
use aether_rules::{DefaultRuleCompiler, RuleCompiler};
use aether_runtime::{DerivedSet, RuleRuntime, RuntimeIteration, SemiNaiveRuntime};
use aether_schema::{AttributeClass, AttributeSchema, PredicateSignature, Schema, ValueType};
use aether_storage::{InMemoryJournal, Journal};
use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::hint::black_box;
use std::mem::size_of;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{
    collections::{BTreeMap, HashMap},
    env,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::Command,
};
use sysinfo::System;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};
use tower::util::ServiceExt;

mod capacity;
pub use capacity::{
    build_capacity_input_bundle, build_capacity_report, load_capacity_input_bundle,
    load_capacity_report, load_perturbation_summary, render_markdown_capacity_input_bundle,
    render_markdown_capacity_report, write_capacity_report, CapacityConfidenceLevel,
    CapacityHardwareClass, CapacityLimitingFactor, CapacityNodeClass, PerfCapacityArtifactPaths,
    PerfCapacityClassEnvelope, PerfCapacityConcurrencyPack, PerfCapacityConcurrencyPoint,
    PerfCapacityCurve, PerfCapacityCurvePoint, PerfCapacityInputBundle, PerfCapacityReport,
    PerfCapacityStoragePoint, PerfPerturbationSummary,
};

pub const DEFAULT_REPORT_SAMPLES: usize = 5;
pub const DEFAULT_HOST_MANIFEST_PATH: &str =
    "fixtures/performance/hosts/dev-chad-windows-native.json";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PerfSuiteId {
    CoreKernel,
    ServiceInProcess,
    HttpPilotBoundary,
    ReplicatedPartition,
    FullStack,
    #[default]
    Legacy,
}

impl PerfSuiteId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CoreKernel => "core_kernel",
            Self::ServiceInProcess => "service_in_process",
            Self::HttpPilotBoundary => "http_pilot_boundary",
            Self::ReplicatedPartition => "replicated_partition",
            Self::FullStack => "full_stack",
            Self::Legacy => "legacy",
        }
    }

    pub fn is_release_gated(self) -> bool {
        matches!(
            self,
            Self::CoreKernel | Self::ServiceInProcess | Self::Legacy
        )
    }
}

impl std::fmt::Display for PerfSuiteId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for PerfSuiteId {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "core_kernel" => Ok(Self::CoreKernel),
            "service_in_process" => Ok(Self::ServiceInProcess),
            "http_pilot_boundary" => Ok(Self::HttpPilotBoundary),
            "replicated_partition" => Ok(Self::ReplicatedPartition),
            "full_stack" => Ok(Self::FullStack),
            "legacy" => Ok(Self::Legacy),
            other => Err(ApiError::Validation(format!(
                "unknown performance suite `{other}`; expected one of core_kernel, service_in_process, http_pilot_boundary, replicated_partition, full_stack"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PerfExecutionEnvironment {
    NativeWindows,
    NativeLinux,
    WslUbuntu,
    GithubWindows,
    GithubUbuntu,
    #[default]
    Unknown,
}

impl PerfExecutionEnvironment {
    fn detect() -> Self {
        if env::var("GITHUB_ACTIONS")
            .map(|value| value == "true")
            .unwrap_or(false)
        {
            return match env::consts::OS {
                "windows" => Self::GithubWindows,
                "linux" => Self::GithubUbuntu,
                _ => Self::Unknown,
            };
        }
        if env::var_os("WSL_DISTRO_NAME").is_some() || env::var_os("WSL_INTEROP").is_some() {
            return Self::WslUbuntu;
        }
        match env::consts::OS {
            "windows" => Self::NativeWindows,
            "linux" => Self::NativeLinux,
            _ => Self::Unknown,
        }
    }
}

impl std::fmt::Display for PerfExecutionEnvironment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::NativeWindows => "native_windows",
            Self::NativeLinux => "native_linux",
            Self::WslUbuntu => "wsl_ubuntu",
            Self::GithubWindows => "github_windows",
            Self::GithubUbuntu => "github_ubuntu",
            Self::Unknown => "unknown",
        };
        f.write_str(value)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PerfHostSnapshot {
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub cpu_brand: String,
    pub physical_cores: Option<usize>,
    pub logical_cores: Option<usize>,
    pub total_memory_bytes: Option<u64>,
    pub execution_environment: PerfExecutionEnvironment,
}

impl PerfHostSnapshot {
    pub fn fingerprint(&self) -> String {
        format!(
            "{}|{}|{}|{}|{}|{}|{}",
            self.hostname,
            self.os,
            self.arch,
            self.cpu_brand,
            self.physical_cores
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            self.logical_cores
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            self.execution_environment
        )
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PerfHostManifest {
    pub host_id: String,
    pub display_name: String,
    pub host_class: String,
    #[serde(default)]
    pub execution_environment: Option<PerfExecutionEnvironment>,
    #[serde(default)]
    pub vendor: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub notes: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PerfRunMetadata {
    pub suite_id: PerfSuiteId,
    pub timestamp: String,
    pub build_profile: String,
    pub samples_per_workload: usize,
    pub execution_environment: PerfExecutionEnvironment,
    #[serde(default)]
    pub git_commit: Option<String>,
    #[serde(default)]
    pub git_dirty: Option<bool>,
    #[serde(default)]
    pub host_manifest_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfScalarMetric {
    pub name: String,
    pub value: f64,
    pub unit: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfRunBundle {
    pub label: String,
    pub generated_at: String,
    pub host_snapshot: PerfHostSnapshot,
    #[serde(default)]
    pub host_manifest: Option<PerfHostManifest>,
    pub run: PerfRunMetadata,
    pub report: PerfReport,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PerfMatrixCell {
    pub host_id: String,
    pub host_name: String,
    pub suite_id: PerfSuiteId,
    pub generated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mean_latency_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throughput_per_second: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_bytes: Option<usize>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PerfMatrixRow {
    pub suite_id: PerfSuiteId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<PerfSuiteId>,
    pub workload: String,
    pub scale: String,
    pub kind: String,
    pub unit: String,
    pub cells: Vec<PerfMatrixCell>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PerfMatrixReport {
    pub generated_at: String,
    pub rows: Vec<PerfMatrixRow>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LatencyStats {
    pub samples: usize,
    pub mean: Duration,
    pub min: Duration,
    pub max: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfMeasurement {
    #[serde(default)]
    pub group: Option<PerfSuiteId>,
    pub workload: String,
    pub scale: String,
    pub units: usize,
    pub unit_label: String,
    pub latency: LatencyStats,
    pub throughput_per_second: f64,
    #[serde(default)]
    pub metrics: Vec<PerfScalarMetric>,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FootprintEstimate {
    #[serde(default)]
    pub group: Option<PerfSuiteId>,
    pub workload: String,
    pub scale: String,
    pub estimated_bytes: usize,
    #[serde(default)]
    pub metrics: Vec<PerfScalarMetric>,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfReport {
    pub samples_per_workload: usize,
    pub measurements: Vec<PerfMeasurement>,
    pub footprints: Vec<FootprintEstimate>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfBaseline {
    pub label: String,
    pub generated_at: String,
    #[serde(default)]
    pub suite_id: Option<PerfSuiteId>,
    #[serde(default)]
    pub host_snapshot: Option<PerfHostSnapshot>,
    #[serde(default)]
    pub host_manifest_id: Option<String>,
    #[serde(default)]
    pub run_metadata: Option<PerfRunMetadata>,
    pub report: PerfReport,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfDriftBudget {
    pub warn_throughput_regression_pct: f64,
    pub fail_throughput_regression_pct: f64,
    pub warn_footprint_growth_pct: f64,
    pub fail_footprint_growth_pct: f64,
}

impl Default for PerfDriftBudget {
    fn default() -> Self {
        Self {
            // Local throughput is materially noisier than structural footprint,
            // so the pilot defaults keep warnings sensitive while reserving fail
            // for larger regressions that are more likely to be meaningful.
            warn_throughput_regression_pct: 15.0,
            fail_throughput_regression_pct: 30.0,
            warn_footprint_growth_pct: 10.0,
            fail_footprint_growth_pct: 20.0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftSeverity {
    Ok,
    Warn,
    Fail,
    MissingBaseline,
}

impl DriftSeverity {
    fn merge(self, other: Self) -> Self {
        use DriftSeverity::{Fail, MissingBaseline, Ok, Warn};
        match (self, other) {
            (Fail, _) | (_, Fail) => Fail,
            (Warn, _) | (_, Warn) => Warn,
            (MissingBaseline, _) | (_, MissingBaseline) => MissingBaseline,
            (Ok, Ok) => Ok,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfMeasurementDrift {
    #[serde(default)]
    pub group: Option<PerfSuiteId>,
    pub workload: String,
    pub scale: String,
    pub unit_label: String,
    pub baseline_throughput_per_second: Option<f64>,
    pub current_throughput_per_second: f64,
    pub throughput_delta_pct: Option<f64>,
    pub severity: DriftSeverity,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfFootprintDrift {
    #[serde(default)]
    pub group: Option<PerfSuiteId>,
    pub workload: String,
    pub scale: String,
    pub baseline_estimated_bytes: Option<usize>,
    pub current_estimated_bytes: usize,
    pub estimated_bytes_delta_pct: Option<f64>,
    pub severity: DriftSeverity,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfDriftReport {
    pub baseline_label: String,
    pub generated_at: String,
    #[serde(default)]
    pub suite_id: Option<PerfSuiteId>,
    #[serde(default)]
    pub host_manifest_id: Option<String>,
    #[serde(default)]
    pub host_fingerprint: Option<String>,
    pub budgets: PerfDriftBudget,
    pub measurements: Vec<PerfMeasurementDrift>,
    pub footprints: Vec<PerfFootprintDrift>,
    pub overall: DriftSeverity,
    pub observed_overall: DriftSeverity,
}

#[derive(Clone, Debug)]
pub enum PerfEvent {
    SuiteStart {
        suite_id: PerfSuiteId,
        host_snapshot: PerfHostSnapshot,
        total_workloads: usize,
        samples_per_workload: usize,
    },
    WorkloadGroupStart {
        group: PerfSuiteId,
        workload_count: usize,
    },
    MeasurementStart {
        group: PerfSuiteId,
        workload: &'static str,
        scale: String,
        total_samples: usize,
        units: usize,
        unit_label: &'static str,
        metrics: Vec<PerfScalarMetric>,
        notes: Vec<String>,
    },
    SampleRecorded {
        workload: &'static str,
        scale: String,
        sample_index: usize,
        total_samples: usize,
        elapsed: Duration,
        throughput_per_second: f64,
        mean_so_far: Duration,
        min_so_far: Duration,
        max_so_far: Duration,
    },
    MeasurementComplete {
        measurement: PerfMeasurement,
    },
    FootprintComputed {
        footprint: FootprintEstimate,
    },
}

#[derive(Clone, Debug)]
pub struct AppendFixture {
    pub datoms: Vec<Datom>,
}

#[derive(Clone, Debug)]
pub struct ResolveFixture {
    pub schema: Schema,
    pub datoms: Vec<Datom>,
    pub as_of: ElementId,
}

#[derive(Clone, Debug)]
pub struct CompileFixture {
    pub schema: Schema,
    pub program: RuleProgram,
}

#[derive(Clone, Debug)]
pub struct RuntimeFixture {
    pub state: ResolvedState,
    pub program: CompiledProgram,
    pub expected_tuple_count: usize,
    pub chain_len: usize,
}

#[derive(Clone, Debug)]
pub struct ExplainFixture {
    pub derived: DerivedSet,
    pub tuple_id: TupleId,
    pub chain_len: usize,
}

#[derive(Debug)]
pub struct ServiceFixture {
    pub service: InMemoryKernelService,
    pub request: RunDocumentRequest,
    pub expected_row_count: usize,
    pub task_count: usize,
}

#[derive(Debug)]
pub struct DurableResolveFixture {
    _root: TempDirGuard,
    pub database_path: PathBuf,
    pub schema: Schema,
    pub entity_count: usize,
    pub datom_count: usize,
}

#[derive(Debug)]
pub struct DurableServiceReplayFixture {
    _root: TempDirGuard,
    pub database_path: PathBuf,
    pub request: RunDocumentRequest,
    pub expected_row_count: usize,
    pub task_count: usize,
    pub datom_count: usize,
}

struct HttpFixture {
    runtime: Runtime,
    router: axum::Router,
    token: String,
    explain_tuple_id: TupleId,
    history_datoms: usize,
    coordination_rows: usize,
    delta_changed_rows: usize,
    explain_trace_tuples: usize,
}

#[derive(Debug)]
struct ReplicatedPartitionFixture {
    _root: TempDirGuard,
    root: PathBuf,
    partition: PartitionId,
    follower_replica: ReplicaId,
    configs: Vec<AuthorityPartitionConfig>,
    stale_epoch: LeaderEpoch,
    leader_append_datoms: Vec<Datom>,
    federated_run_request: FederatedRunDocumentRequest,
    federated_history_request: FederatedHistoryRequest,
}

struct MeasurementPlan {
    group: PerfSuiteId,
    workload: &'static str,
    scale: String,
    units: usize,
    unit_label: &'static str,
    metrics: Vec<PerfScalarMetric>,
    notes: Vec<String>,
    samples: usize,
    iterations_per_sample: usize,
}

#[derive(Debug)]
struct TempDirGuard {
    path: PathBuf,
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub fn collect_host_snapshot() -> PerfHostSnapshot {
    let mut system = System::new_all();
    system.refresh_all();

    let hostname = System::host_name()
        .or_else(|| env::var("COMPUTERNAME").ok())
        .or_else(|| env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "unknown".into());
    let cpu_brand = system
        .cpus()
        .first()
        .map(|cpu| cpu.brand().trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".into());

    PerfHostSnapshot {
        hostname,
        os: format!(
            "{} {}",
            System::name().unwrap_or_else(|| env::consts::OS.into()),
            System::os_version().unwrap_or_default()
        )
        .trim()
        .to_string(),
        arch: env::consts::ARCH.into(),
        cpu_brand,
        physical_cores: system.physical_core_count(),
        logical_cores: Some(system.cpus().len()),
        total_memory_bytes: Some(system.total_memory()),
        execution_environment: PerfExecutionEnvironment::detect(),
    }
}

pub fn load_host_manifest(path: impl AsRef<Path>) -> Result<PerfHostManifest, ApiError> {
    let path = path.as_ref();
    let contents = fs::read_to_string(path).map_err(|source| {
        ApiError::Validation(format!(
            "failed to read host manifest {}: {source}",
            path.display()
        ))
    })?;
    serde_json::from_str(&contents).map_err(|source| {
        ApiError::Validation(format!(
            "failed to parse host manifest {}: {source}",
            path.display()
        ))
    })
}

fn build_run_metadata(
    suite_id: PerfSuiteId,
    samples_per_workload: usize,
    host_manifest: Option<&PerfHostManifest>,
    host_snapshot: &PerfHostSnapshot,
) -> PerfRunMetadata {
    PerfRunMetadata {
        suite_id,
        timestamp: timestamp_string(),
        build_profile: if cfg!(debug_assertions) {
            "debug".into()
        } else {
            "release".into()
        },
        samples_per_workload,
        execution_environment: host_snapshot.execution_environment,
        git_commit: git_output(&["rev-parse", "HEAD"]),
        git_dirty: git_dirty(),
        host_manifest_id: host_manifest.map(|manifest| manifest.host_id.clone()),
    }
}

fn git_output(args: &[&str]) -> Option<String> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|value| !value.is_empty())
}

fn git_dirty() -> Option<bool> {
    let output = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(!String::from_utf8_lossy(&output.stdout).trim().is_empty())
}

fn timestamp_string() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_secs().to_string()
}

fn metric(name: impl Into<String>, value: f64, unit: impl Into<String>) -> PerfScalarMetric {
    PerfScalarMetric {
        name: name.into(),
        value,
        unit: unit.into(),
    }
}

fn default_policy_context() -> PolicyContext {
    PolicyContext {
        capabilities: vec!["executor".into()],
        visibilities: Vec::new(),
    }
}

fn suite_groups(suite_id: PerfSuiteId) -> &'static [PerfSuiteId] {
    match suite_id {
        PerfSuiteId::CoreKernel => &[PerfSuiteId::CoreKernel],
        PerfSuiteId::ServiceInProcess => &[PerfSuiteId::ServiceInProcess],
        PerfSuiteId::HttpPilotBoundary => &[PerfSuiteId::HttpPilotBoundary],
        PerfSuiteId::ReplicatedPartition => &[PerfSuiteId::ReplicatedPartition],
        PerfSuiteId::FullStack => &[
            PerfSuiteId::CoreKernel,
            PerfSuiteId::ServiceInProcess,
            PerfSuiteId::HttpPilotBoundary,
            PerfSuiteId::ReplicatedPartition,
        ],
        PerfSuiteId::Legacy => &[PerfSuiteId::CoreKernel, PerfSuiteId::ServiceInProcess],
    }
}

fn display_host_name(bundle: &PerfRunBundle) -> String {
    bundle
        .host_manifest
        .as_ref()
        .map(|manifest| manifest.display_name.clone())
        .unwrap_or_else(|| bundle.host_snapshot.hostname.clone())
}

pub fn build_append_fixture(count: usize) -> AppendFixture {
    AppendFixture {
        datoms: (0..count)
            .map(|index| Datom {
                entity: EntityId::new((index + 1) as u64),
                attribute: AttributeId::new(1),
                value: Value::U64((index + 1) as u64),
                op: OperationKind::Assert,
                element: ElementId::new((index + 1) as u64),
                replica: ReplicaId::new(1),
                causal_context: Default::default(),
                provenance: DatomProvenance::default(),
                policy: None,
            })
            .collect(),
    }
}

pub fn build_resolve_fixture(entity_count: usize) -> ResolveFixture {
    let entity_count = entity_count.max(1);
    let mut schema = Schema::new("perf-v1");
    register_attribute(
        &mut schema,
        AttributeSchema {
            id: AttributeId::new(1),
            name: "task.status".into(),
            class: AttributeClass::ScalarLww,
            value_type: ValueType::String,
        },
    );
    register_attribute(
        &mut schema,
        AttributeSchema {
            id: AttributeId::new(2),
            name: "task.tag".into(),
            class: AttributeClass::SetAddWins,
            value_type: ValueType::String,
        },
    );
    register_attribute(
        &mut schema,
        AttributeSchema {
            id: AttributeId::new(3),
            name: "task.depends_on".into(),
            class: AttributeClass::RefSet,
            value_type: ValueType::Entity,
        },
    );

    let mut datoms = Vec::new();
    let mut next_element = 1u64;
    for entity in 1..=entity_count {
        datoms.push(datom(
            entity as u64,
            1,
            Value::String("queued".into()),
            OperationKind::Assert,
            next_element,
        ));
        next_element += 1;

        if entity % 3 == 0 {
            datoms.push(datom(
                entity as u64,
                1,
                Value::String("running".into()),
                OperationKind::Assert,
                next_element,
            ));
            next_element += 1;
        }

        datoms.push(datom(
            entity as u64,
            2,
            Value::String("ops".into()),
            OperationKind::Add,
            next_element,
        ));
        next_element += 1;

        if entity % 2 == 0 {
            datoms.push(datom(
                entity as u64,
                2,
                Value::String("critical".into()),
                OperationKind::Add,
                next_element,
            ));
            next_element += 1;
            datoms.push(datom(
                entity as u64,
                2,
                Value::String("critical".into()),
                OperationKind::Remove,
                next_element,
            ));
            next_element += 1;
        }

        if entity < entity_count {
            datoms.push(datom(
                entity as u64,
                3,
                Value::Entity(EntityId::new((entity + 1) as u64)),
                OperationKind::Add,
                next_element,
            ));
            next_element += 1;
        }
    }

    let as_of_index = datoms.len().saturating_sub(1) / 2;
    let as_of = datoms[as_of_index].element;

    ResolveFixture {
        schema,
        datoms,
        as_of,
    }
}

pub fn build_compile_fixture(scc_width: usize) -> CompileFixture {
    let scc_width = scc_width.max(2);
    let edge = predicate(1, "edge", 2);
    let mut schema = Schema::new("perf-v1");
    register_predicate(
        &mut schema,
        &edge,
        vec![ValueType::Entity, ValueType::Entity],
    );

    let mut predicates = vec![edge.clone()];
    let mut rules = Vec::new();
    let cycle_predicates = (0..scc_width)
        .map(|offset| predicate((offset + 2) as u64, &format!("cycle_{offset}"), 2))
        .collect::<Vec<_>>();

    for cycle in &cycle_predicates {
        register_predicate(
            &mut schema,
            cycle,
            vec![ValueType::Entity, ValueType::Entity],
        );
        predicates.push(cycle.clone());
    }

    rules.push(RuleAst {
        id: RuleId::new(1),
        head: atom(cycle_predicates[0].clone(), &["x", "y"]),
        body: vec![Literal::Positive(atom(edge.clone(), &["x", "y"]))],
    });

    let mut next_rule_id = 2u64;
    for offset in 1..cycle_predicates.len() {
        rules.push(RuleAst {
            id: RuleId::new(next_rule_id),
            head: atom(cycle_predicates[offset].clone(), &["x", "z"]),
            body: vec![
                Literal::Positive(atom(cycle_predicates[offset - 1].clone(), &["x", "y"])),
                Literal::Positive(atom(edge.clone(), &["y", "z"])),
            ],
        });
        next_rule_id += 1;
    }

    rules.push(RuleAst {
        id: RuleId::new(next_rule_id),
        head: atom(cycle_predicates[0].clone(), &["x", "z"]),
        body: vec![
            Literal::Positive(atom(
                cycle_predicates[cycle_predicates.len() - 1].clone(),
                &["x", "y"],
            )),
            Literal::Positive(atom(edge, &["y", "z"])),
        ],
    });

    CompileFixture {
        schema,
        program: RuleProgram {
            predicates,
            rules,
            materialized: vec![cycle_predicates[0].id],
            facts: Vec::new(),
        },
    }
}

pub fn build_runtime_fixture(chain_len: usize) -> Result<RuntimeFixture, ApiError> {
    let chain_len = validate_chain_len(chain_len)?;
    let schema = dependency_schema();
    let program = dependency_program();
    let datoms = dependency_chain_datoms(chain_len);
    let state = MaterializedResolver.current(&schema, &datoms)?;
    let program = DefaultRuleCompiler.compile(&schema, &program)?;

    Ok(RuntimeFixture {
        state,
        program,
        expected_tuple_count: expected_transitive_pairs(chain_len),
        chain_len,
    })
}

pub fn build_explain_fixture(chain_len: usize) -> Result<ExplainFixture, ApiError> {
    let runtime = build_runtime_fixture(chain_len)?;
    let derived = SemiNaiveRuntime.evaluate(&runtime.state, &runtime.program)?;
    let target = vec![
        Value::Entity(EntityId::new(1)),
        Value::Entity(EntityId::new(runtime.chain_len as u64)),
    ];
    let tuple_id = derived
        .tuples
        .iter()
        .find(|tuple| tuple.tuple.values == target)
        .map(|tuple| tuple.tuple.id)
        .ok_or_else(|| ApiError::Validation("longest transitive tuple was not derived".into()))?;

    Ok(ExplainFixture {
        derived,
        tuple_id,
        chain_len: runtime.chain_len,
    })
}

pub fn build_coordination_service_fixture(task_count: usize) -> Result<ServiceFixture, ApiError> {
    let task_count = task_count.max(1);
    let mut service = InMemoryKernelService::new();
    service.append(AppendRequest {
        datoms: coordination_datoms(task_count),
    })?;

    let ready_tasks = (1..=task_count)
        .filter(|task| !task_is_done(*task) && !task_has_active_claim(*task))
        .count();

    Ok(ServiceFixture {
        service,
        request: RunDocumentRequest {
            dsl: coordination_claimability_dsl(task_count),
            policy_context: None,
        },
        expected_row_count: ready_tasks * 2,
        task_count,
    })
}

pub fn build_durable_resolve_fixture(
    entity_count: usize,
) -> Result<DurableResolveFixture, ApiError> {
    let fixture = build_resolve_fixture(entity_count);
    let root = unique_temp_dir("resolve-restart");
    let data_dir = root.join("data");
    fs::create_dir_all(&data_dir).map_err(|source| {
        ApiError::Validation(format!(
            "failed to create durable resolve fixture directory {}: {source}",
            data_dir.display()
        ))
    })?;
    let database_path = data_dir.join("coordination.sqlite");
    let mut service = SqliteKernelService::open(&database_path)?;
    service.append(AppendRequest {
        datoms: fixture.datoms.clone(),
    })?;

    Ok(DurableResolveFixture {
        _root: TempDirGuard { path: root },
        database_path,
        schema: fixture.schema,
        entity_count: entity_count.max(1),
        datom_count: fixture.datoms.len(),
    })
}

pub fn build_durable_coordination_replay_fixture(
    task_count: usize,
) -> Result<DurableServiceReplayFixture, ApiError> {
    let task_count = task_count.max(1);
    let root = unique_temp_dir("service-restart");
    let data_dir = root.join("data");
    fs::create_dir_all(&data_dir).map_err(|source| {
        ApiError::Validation(format!(
            "failed to create durable service fixture directory {}: {source}",
            data_dir.display()
        ))
    })?;
    let database_path = data_dir.join("coordination.sqlite");
    let mut service = SqliteKernelService::open(&database_path)?;
    let datoms = coordination_datoms(task_count);
    service.append(AppendRequest {
        datoms: datoms.clone(),
    })?;

    Ok(DurableServiceReplayFixture {
        _root: TempDirGuard { path: root },
        database_path,
        request: RunDocumentRequest {
            dsl: coordination_claimability_dsl(task_count),
            policy_context: None,
        },
        expected_row_count: coordination_claimability_rows(task_count),
        task_count,
        datom_count: datoms.len(),
    })
}

pub fn benchmark_append(count: usize, samples: usize) -> Result<PerfMeasurement, ApiError> {
    let mut observer = None;
    benchmark_append_impl(count, samples, &mut observer)
}

fn benchmark_append_impl(
    count: usize,
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_append_fixture(count);
    let notes = vec![format!(
        "{} unique element IDs appended into an empty journal each sample",
        format_count(fixture.datoms.len())
    )];

    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::CoreKernel,
            workload: "Journal append throughput",
            scale: format!("{} datoms", format_count(fixture.datoms.len())),
            units: fixture.datoms.len(),
            unit_label: "datoms/s",
            metrics: vec![metric("datoms", fixture.datoms.len() as f64, "datoms")],
            notes,
            samples,
            iterations_per_sample: 1,
        },
        observer,
        move || {
            let mut journal = InMemoryJournal::new();
            journal.append(&fixture.datoms)?;
            Ok(journal)
        },
    )
}

pub fn benchmark_resolve_current(
    entity_count: usize,
    samples: usize,
) -> Result<PerfMeasurement, ApiError> {
    let mut observer = None;
    benchmark_resolve_current_impl(entity_count, samples, &mut observer)
}

fn benchmark_resolve_current_impl(
    entity_count: usize,
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_resolve_fixture(entity_count);
    let notes = vec![
        format!(
            "{} datoms across scalar, set, and ref attributes",
            format_count(fixture.datoms.len())
        ),
        "16 resolver passes per sample to reduce timer jitter on this state microbenchmark".into(),
    ];

    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::CoreKernel,
            workload: "Resolver current throughput",
            scale: format!("{} entities", format_count(entity_count)),
            units: entity_count,
            unit_label: "entities/s",
            metrics: vec![
                metric("entities", entity_count as f64, "entities"),
                metric("datoms", fixture.datoms.len() as f64, "datoms"),
            ],
            notes,
            samples,
            iterations_per_sample: 16,
        },
        observer,
        move || {
            MaterializedResolver
                .current(&fixture.schema, &fixture.datoms)
                .map_err(ApiError::from)
        },
    )
}

pub fn benchmark_resolve_as_of(
    entity_count: usize,
    samples: usize,
) -> Result<PerfMeasurement, ApiError> {
    let mut observer = None;
    benchmark_resolve_as_of_impl(entity_count, samples, &mut observer)
}

fn benchmark_resolve_as_of_impl(
    entity_count: usize,
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_resolve_fixture(entity_count);
    let notes = vec![
        format!(
            "Inclusive prefix cut at {} across {} datoms",
            fixture.as_of,
            format_count(fixture.datoms.len())
        ),
        "16 resolver passes per sample to reduce timer jitter on this temporal microbenchmark"
            .into(),
    ];

    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::CoreKernel,
            workload: "Resolver as-of throughput",
            scale: format!("{} entities", format_count(entity_count)),
            units: entity_count,
            unit_label: "entities/s",
            metrics: vec![
                metric("entities", entity_count as f64, "entities"),
                metric("datoms", fixture.datoms.len() as f64, "datoms"),
                metric("as_of_element", fixture.as_of.0 as f64, "element"),
            ],
            notes,
            samples,
            iterations_per_sample: 16,
        },
        observer,
        move || {
            MaterializedResolver
                .as_of(&fixture.schema, &fixture.datoms, &fixture.as_of)
                .map_err(ApiError::from)
        },
    )
}

pub fn benchmark_compile_scc(
    scc_width: usize,
    samples: usize,
) -> Result<PerfMeasurement, ApiError> {
    let mut observer = None;
    benchmark_compile_scc_impl(scc_width, samples, &mut observer)
}

fn benchmark_compile_scc_impl(
    scc_width: usize,
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_compile_fixture(scc_width);
    let notes = vec![
        format!(
            "{} predicates and {} rules with one large recursive SCC",
            format_count(fixture.program.predicates.len()),
            format_count(fixture.program.rules.len())
        ),
        "128 compiler passes per sample to reduce timer jitter on this microbenchmark".into(),
    ];

    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::CoreKernel,
            workload: "Compiler SCC planning",
            scale: format!("recursive width {}", format_count(scc_width)),
            units: scc_width,
            unit_label: "predicates/s",
            metrics: vec![
                metric(
                    "predicates",
                    fixture.program.predicates.len() as f64,
                    "predicates",
                ),
                metric("rules", fixture.program.rules.len() as f64, "rules"),
            ],
            notes,
            samples,
            iterations_per_sample: 128,
        },
        observer,
        move || {
            DefaultRuleCompiler
                .compile(&fixture.schema, &fixture.program)
                .map_err(ApiError::from)
        },
    )
}

pub fn benchmark_runtime_closure(
    chain_len: usize,
    samples: usize,
) -> Result<PerfMeasurement, ApiError> {
    let mut observer = None;
    benchmark_runtime_closure_impl(chain_len, samples, &mut observer)
}

fn benchmark_runtime_closure_impl(
    chain_len: usize,
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_runtime_fixture(chain_len)?;
    let notes = vec![format!(
        "{} derived tuples expected from a linear dependency chain",
        format_count(fixture.expected_tuple_count)
    )];

    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::CoreKernel,
            workload: "Recursive closure runtime",
            scale: format!("chain {}", format_count(chain_len)),
            units: fixture.expected_tuple_count,
            unit_label: "tuples/s",
            metrics: vec![
                metric("chain_len", chain_len as f64, "nodes"),
                metric(
                    "expected_tuples",
                    fixture.expected_tuple_count as f64,
                    "tuples",
                ),
            ],
            notes,
            samples,
            iterations_per_sample: 1,
        },
        observer,
        move || {
            SemiNaiveRuntime
                .evaluate(&fixture.state, &fixture.program)
                .map_err(ApiError::from)
        },
    )
}

pub fn benchmark_explain_trace(
    chain_len: usize,
    samples: usize,
) -> Result<PerfMeasurement, ApiError> {
    let mut observer = None;
    benchmark_explain_trace_impl(chain_len, samples, &mut observer)
}

fn benchmark_explain_trace_impl(
    chain_len: usize,
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_explain_fixture(chain_len)?;
    let trace =
        InMemoryExplainer::from_derived_set(&fixture.derived).explain_tuple(&fixture.tuple_id)?;
    let notes = vec![
        format!(
            "{} tuples in the longest proof trace",
            format_count(trace.tuples.len())
        ),
        format!(
            "root tuple carries {} source datom IDs",
            format_count(
                trace
                    .tuples
                    .first()
                    .map(|tuple| tuple.metadata.source_datom_ids.len())
                    .unwrap_or_default()
            )
        ),
        "64 explanation passes per sample to reduce timer jitter on this proof microbenchmark"
            .into(),
    ];

    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::CoreKernel,
            workload: "Tuple explanation runtime",
            scale: format!("chain {}", format_count(chain_len)),
            units: trace.tuples.len(),
            unit_label: "trace-tuples/s",
            metrics: vec![
                metric("chain_len", chain_len as f64, "nodes"),
                metric("trace_tuples", trace.tuples.len() as f64, "tuples"),
            ],
            notes,
            samples,
            iterations_per_sample: 64,
        },
        observer,
        move || {
            let explainer = InMemoryExplainer::from_derived_set(&fixture.derived);
            explainer
                .explain_tuple(&fixture.tuple_id)
                .map_err(ApiError::from)
        },
    )
}

pub fn benchmark_service_coordination(
    task_count: usize,
    samples: usize,
) -> Result<PerfMeasurement, ApiError> {
    let mut observer = None;
    benchmark_service_coordination_impl(task_count, samples, &mut observer)
}

fn benchmark_service_coordination_impl(
    task_count: usize,
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let mut fixture = build_coordination_service_fixture(task_count)?;
    let notes = vec![
        format!(
            "{} expected worker-task claimability rows",
            format_count(fixture.expected_row_count)
        ),
        "includes parse, compile, resolve, evaluate, and query through the kernel service".into(),
    ];

    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::ServiceInProcess,
            workload: "Kernel service coordination run",
            scale: format!("{} tasks", format_count(task_count)),
            units: fixture.expected_row_count.max(1),
            unit_label: "rows/s",
            metrics: vec![
                metric("tasks", task_count as f64, "tasks"),
                metric("rows", fixture.expected_row_count as f64, "rows"),
            ],
            notes,
            samples,
            iterations_per_sample: 1,
        },
        observer,
        move || fixture.service.run_document(fixture.request.clone()),
    )
}

pub fn benchmark_durable_restart_current(
    entity_count: usize,
    samples: usize,
) -> Result<PerfMeasurement, ApiError> {
    let mut observer = None;
    benchmark_durable_restart_current_impl(entity_count, samples, &mut observer)
}

fn benchmark_durable_restart_current_impl(
    entity_count: usize,
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_durable_resolve_fixture(entity_count)?;
    let notes = vec![
        format!(
            "{} durable datoms persisted into a SQLite journal",
            format_count(fixture.datom_count)
        ),
        "each sample reopens the durable kernel and resolves current state from committed history"
            .into(),
        "4 restart-and-replay passes per sample to reduce timer jitter".into(),
    ];

    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::CoreKernel,
            workload: "Durable restart current replay",
            scale: format!("{} entities", format_count(entity_count)),
            units: fixture.entity_count,
            unit_label: "entities/s",
            metrics: vec![
                metric("entities", fixture.entity_count as f64, "entities"),
                metric("datoms", fixture.datom_count as f64, "datoms"),
                metric("restart_passes", 4.0, "passes"),
            ],
            notes,
            samples,
            iterations_per_sample: 4,
        },
        observer,
        move || {
            let service = SqliteKernelService::open(&fixture.database_path)?;
            service.current_state(CurrentStateRequest {
                schema: fixture.schema.clone(),
                datoms: Vec::new(),
                policy_context: None,
            })
        },
    )
}

pub fn benchmark_durable_restart_coordination(
    task_count: usize,
    samples: usize,
) -> Result<PerfMeasurement, ApiError> {
    let mut observer = None;
    benchmark_durable_restart_coordination_impl(task_count, samples, &mut observer)
}

fn benchmark_durable_restart_coordination_impl(
    task_count: usize,
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_durable_coordination_replay_fixture(task_count)?;
    let notes = vec![
        format!(
            "{} expected worker-task claimability rows",
            format_count(fixture.expected_row_count)
        ),
        "each sample reopens the durable kernel, replays the SQLite journal, and runs the coordination document"
            .into(),
        "4 restart-and-replay passes per sample to reduce timer jitter".into(),
    ];

    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::ServiceInProcess,
            workload: "Durable restart coordination replay",
            scale: format!("{} tasks", format_count(task_count)),
            units: fixture.expected_row_count.max(1),
            unit_label: "rows/s",
            metrics: vec![
                metric("tasks", task_count as f64, "tasks"),
                metric("rows", fixture.expected_row_count as f64, "rows"),
                metric("restart_passes", 4.0, "passes"),
            ],
            notes,
            samples,
            iterations_per_sample: 4,
        },
        observer,
        move || {
            let mut service = SqliteKernelService::open(&fixture.database_path)?;
            service.run_document(fixture.request.clone())
        },
    )
}

pub fn estimate_runtime_footprint(chain_len: usize) -> Result<FootprintEstimate, ApiError> {
    let fixture = build_runtime_fixture(chain_len)?;
    let derived = SemiNaiveRuntime.evaluate(&fixture.state, &fixture.program)?;

    Ok(FootprintEstimate {
        group: Some(PerfSuiteId::CoreKernel),
        workload: "Derived-set footprint estimate".into(),
        scale: format!("chain {}", format_count(chain_len)),
        estimated_bytes: estimate_derived_set_bytes(&derived),
        metrics: vec![
            metric("chain_len", chain_len as f64, "nodes"),
            metric("tuples", derived.tuples.len() as f64, "tuples"),
            metric("iterations", derived.iterations.len() as f64, "iterations"),
        ],
        notes: vec![
            format!(
                "{} tuples across {} iterations",
                format_count(derived.tuples.len()),
                format_count(derived.iterations.len())
            ),
            "structural lower-bound estimate for regression tracking".into(),
        ],
    })
}

pub fn estimate_trace_footprint(chain_len: usize) -> Result<FootprintEstimate, ApiError> {
    let fixture = build_explain_fixture(chain_len)?;
    let trace =
        InMemoryExplainer::from_derived_set(&fixture.derived).explain_tuple(&fixture.tuple_id)?;

    Ok(FootprintEstimate {
        group: Some(PerfSuiteId::CoreKernel),
        workload: "Derivation-trace footprint estimate".into(),
        scale: format!("chain {}", format_count(chain_len)),
        estimated_bytes: estimate_derivation_trace_bytes(&trace),
        metrics: vec![
            metric("chain_len", chain_len as f64, "nodes"),
            metric("trace_tuples", trace.tuples.len() as f64, "tuples"),
        ],
        notes: vec![
            format!(
                "{} tuples in the reconstructed proof graph",
                format_count(trace.tuples.len())
            ),
            "structural lower-bound estimate for regression tracking".into(),
        ],
    })
}

fn build_http_fixture() -> Result<HttpFixture, ApiError> {
    let token = "perf-operator-token".to_string();
    let mut service = InMemoryKernelService::new();
    let history = coordination_pilot_seed_history();
    service.append(AppendRequest {
        datoms: history.clone(),
    })?;

    let report = build_coordination_pilot_report(&mut service)?;
    let explain_tuple_id = report
        .current_authorized
        .iter()
        .find_map(|row| row.tuple_id)
        .ok_or_else(|| {
            ApiError::Validation(
                "coordination pilot seed did not produce an explainable authorization tuple".into(),
            )
        })?;
    let explain_trace_tuples = report
        .trace
        .as_ref()
        .map(|trace| trace.tuple_count)
        .unwrap_or(0);
    let coordination_rows = report.current_authorized.len()
        + report.claimable.len()
        + report.live_heartbeats.len()
        + report.accepted_outcomes.len()
        + report.rejected_outcomes.len();
    let delta = build_coordination_delta_report(
        &mut service,
        CoordinationDeltaReportRequest {
            left: CoordinationCut::AsOf {
                element: ElementId::new(COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT),
            },
            right: CoordinationCut::Current,
            policy_context: None,
        },
    )?;
    let delta_changed_rows = total_delta_rows(&delta);

    let options = HttpKernelOptions::new()
        .with_auth(HttpAuthConfig::new().with_token_context(
            token.clone(),
            "perf-operator",
            [
                AuthScope::Append,
                AuthScope::Query,
                AuthScope::Explain,
                AuthScope::Ops,
            ],
            default_policy_context(),
        ))
        .with_service_status(ServiceStatusResponse {
            status: "ok".into(),
            build_version: env!("CARGO_PKG_VERSION").into(),
            config_version: "pilot-v1".into(),
            schema_version: "v1".into(),
            bind_addr: Some("127.0.0.1:3000".into()),
            service_mode: ServiceMode::SingleNode,
            storage: ServiceStatusStorage::default(),
            principals: Vec::new(),
            replicas: Vec::new(),
        });
    let runtime = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| {
            ApiError::Validation(format!("failed to build http benchmark runtime: {source}"))
        })?;

    Ok(HttpFixture {
        runtime,
        router: http_router_with_options(service, options),
        token,
        explain_tuple_id,
        history_datoms: history.len(),
        coordination_rows,
        delta_changed_rows,
        explain_trace_tuples,
    })
}

fn http_get<TResponse: DeserializeOwned>(
    fixture: &HttpFixture,
    path: &str,
) -> Result<TResponse, ApiError> {
    fixture.runtime.block_on(async {
        let request = Request::builder()
            .method(Method::GET)
            .uri(path)
            .header("authorization", format!("Bearer {}", fixture.token))
            .body(Body::empty())
            .map_err(|source| {
                ApiError::Validation(format!(
                    "failed to build benchmark GET request {path}: {source}"
                ))
            })?;
        let response = fixture
            .router
            .clone()
            .oneshot(request)
            .await
            .map_err(|source| {
                ApiError::Validation(format!("benchmark GET {path} failed: {source}"))
            })?;
        parse_http_response(path, response).await
    })
}

fn http_post_json<TRequest: Serialize, TResponse: DeserializeOwned>(
    fixture: &HttpFixture,
    path: &str,
    request: &TRequest,
) -> Result<TResponse, ApiError> {
    fixture.runtime.block_on(async {
        let body = serde_json::to_vec(request).map_err(|source| {
            ApiError::Validation(format!(
                "failed to serialize benchmark request for {path}: {source}"
            ))
        })?;
        let request = Request::builder()
            .method(Method::POST)
            .uri(path)
            .header("authorization", format!("Bearer {}", fixture.token))
            .header("content-type", "application/json")
            .body(Body::from(body))
            .map_err(|source| {
                ApiError::Validation(format!(
                    "failed to build benchmark POST request {path}: {source}"
                ))
            })?;
        let response = fixture
            .router
            .clone()
            .oneshot(request)
            .await
            .map_err(|source| {
                ApiError::Validation(format!("benchmark POST {path} failed: {source}"))
            })?;
        parse_http_response(path, response).await
    })
}

async fn parse_http_response<TResponse: DeserializeOwned>(
    path: &str,
    response: axum::response::Response,
) -> Result<TResponse, ApiError> {
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(|source| {
            ApiError::Validation(format!(
                "failed to read benchmark response body for {path}: {source}"
            ))
        })?;
    if status != StatusCode::OK {
        return Err(ApiError::Validation(format!(
            "benchmark request {path} returned {status}: {}",
            String::from_utf8_lossy(&body)
        )));
    }
    serde_json::from_slice(&body).map_err(|source| {
        ApiError::Validation(format!(
            "failed to decode benchmark response for {path}: {source}"
        ))
    })
}

fn total_delta_rows(report: &crate::CoordinationDeltaReport) -> usize {
    [
        &report.current_authorized,
        &report.claimable,
        &report.live_heartbeats,
        &report.accepted_outcomes,
        &report.rejected_outcomes,
    ]
    .into_iter()
    .map(|section| section.added.len() + section.removed.len() + section.changed.len())
    .sum()
}

fn benchmark_http_health_impl(
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_http_fixture()?;
    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::HttpPilotBoundary,
            workload: "HTTP pilot health endpoint",
            scale: "pilot boundary".into(),
            units: 1,
            unit_label: "requests/s",
            metrics: vec![metric(
                "history_datoms",
                fixture.history_datoms as f64,
                "datoms",
            )],
            notes: vec!["authenticated in-process GET /health over the pilot router".into()],
            samples,
            iterations_per_sample: 1,
        },
        observer,
        move || http_get::<HealthResponse>(&fixture, "/health"),
    )
}

fn benchmark_http_status_impl(
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_http_fixture()?;
    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::HttpPilotBoundary,
            workload: "HTTP service status endpoint",
            scale: "pilot boundary".into(),
            units: 1,
            unit_label: "requests/s",
            metrics: vec![metric("service_mode", 1.0, "single_node")],
            notes: vec!["authenticated in-process GET /v1/status over the pilot router".into()],
            samples,
            iterations_per_sample: 1,
        },
        observer,
        move || http_get::<ServiceStatusResponse>(&fixture, "/v1/status"),
    )
}

fn benchmark_http_history_impl(
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_http_fixture()?;
    let history_rows = fixture.history_datoms.max(1);
    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::HttpPilotBoundary,
            workload: "HTTP history endpoint",
            scale: format!("{} datoms", format_count(fixture.history_datoms)),
            units: history_rows,
            unit_label: "rows/s",
            metrics: vec![metric("datoms", fixture.history_datoms as f64, "datoms")],
            notes: vec!["authenticated in-process GET /v1/history over the pilot router".into()],
            samples,
            iterations_per_sample: 1,
        },
        observer,
        move || http_get::<crate::HistoryResponse>(&fixture, "/v1/history"),
    )
}

fn benchmark_http_coordination_report_impl(
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_http_fixture()?;
    let row_count = fixture.coordination_rows.max(1);
    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::HttpPilotBoundary,
            workload: "HTTP coordination report endpoint",
            scale: "pilot coordination".into(),
            units: row_count,
            unit_label: "rows/s",
            metrics: vec![metric("rows", fixture.coordination_rows as f64, "rows")],
            notes: vec![
                "authenticated in-process POST /v1/reports/pilot/coordination over the pilot router"
                    .into(),
            ],
            samples,
            iterations_per_sample: 1,
        },
        observer,
        move || {
            http_post_json::<CoordinationPilotReportRequest, crate::CoordinationPilotReport>(
                &fixture,
                "/v1/reports/pilot/coordination",
                &CoordinationPilotReportRequest {
                    policy_context: None,
                },
            )
        },
    )
}

fn benchmark_http_explain_tuple_impl(
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_http_fixture()?;
    let trace_tuples = fixture.explain_trace_tuples.max(1);
    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::HttpPilotBoundary,
            workload: "HTTP tuple explain endpoint",
            scale: format!("trace {}", format_count(fixture.explain_trace_tuples)),
            units: trace_tuples,
            unit_label: "trace-tuples/s",
            metrics: vec![metric(
                "trace_tuples",
                fixture.explain_trace_tuples as f64,
                "tuples",
            )],
            notes: vec![
                "authenticated in-process POST /v1/explain/tuple over the pilot router".into(),
            ],
            samples,
            iterations_per_sample: 1,
        },
        observer,
        move || {
            http_post_json::<ExplainTupleRequest, crate::ExplainTupleResponse>(
                &fixture,
                "/v1/explain/tuple",
                &ExplainTupleRequest {
                    tuple_id: fixture.explain_tuple_id,
                    policy_context: None,
                },
            )
        },
    )
}

fn benchmark_http_coordination_delta_impl(
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_http_fixture()?;
    let changed_rows = fixture.delta_changed_rows.max(1);
    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::HttpPilotBoundary,
            workload: "HTTP coordination delta endpoint",
            scale: format!("{} changed rows", format_count(fixture.delta_changed_rows)),
            units: changed_rows,
            unit_label: "row-diffs/s",
            metrics: vec![metric(
                "changed_rows",
                fixture.delta_changed_rows as f64,
                "rows",
            )],
            notes: vec![
                "authenticated in-process POST /v1/reports/pilot/coordination-delta over the pilot router"
                    .into(),
            ],
            samples,
            iterations_per_sample: 1,
        },
        observer,
        move || {
            http_post_json::<CoordinationDeltaReportRequest, crate::CoordinationDeltaReport>(
                &fixture,
                "/v1/reports/pilot/coordination-delta",
                &CoordinationDeltaReportRequest {
                    left: CoordinationCut::AsOf {
                        element: ElementId::new(COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT),
                    },
                    right: CoordinationCut::Current,
                    policy_context: None,
                },
            )
        },
    )
}

fn replicated_partition_configs() -> Vec<AuthorityPartitionConfig> {
    vec![
        AuthorityPartitionConfig {
            partition: PartitionId::new("readiness"),
            replicas: vec![
                ReplicaConfig {
                    replica_id: ReplicaId::new(1),
                    database_path: PathBuf::from("readiness-leader.sqlite"),
                    role: ReplicaRole::Leader,
                },
                ReplicaConfig {
                    replica_id: ReplicaId::new(2),
                    database_path: PathBuf::from("readiness-follower.sqlite"),
                    role: ReplicaRole::Follower,
                },
            ],
        },
        AuthorityPartitionConfig {
            partition: PartitionId::new("authority"),
            replicas: vec![
                ReplicaConfig {
                    replica_id: ReplicaId::new(1),
                    database_path: PathBuf::from("authority-leader.sqlite"),
                    role: ReplicaRole::Leader,
                },
                ReplicaConfig {
                    replica_id: ReplicaId::new(2),
                    database_path: PathBuf::from("authority-follower.sqlite"),
                    role: ReplicaRole::Follower,
                },
            ],
        },
    ]
}

fn partition_status_datom(entity: u64, status: &str, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: AttributeId::new(1),
        value: Value::String(status.into()),
        op: OperationKind::Assert,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

fn partition_owner_datom(entity: u64, owner: &str, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: AttributeId::new(1),
        value: Value::String(owner.into()),
        op: OperationKind::Assert,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
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

fn build_replicated_partition_fixture() -> Result<ReplicatedPartitionFixture, ApiError> {
    let root = unique_temp_dir("replicated-perf");
    fs::create_dir_all(&root).map_err(|source| {
        ApiError::Validation(format!(
            "failed to create replicated perf root {}: {source}",
            root.display()
        ))
    })?;
    let guard = TempDirGuard { path: root.clone() };
    let configs = replicated_partition_configs();
    let mut service = ReplicatedAuthorityPartitionService::open(&root, configs.clone())?;
    service.append_partition(PartitionAppendRequest {
        partition: PartitionId::new("readiness"),
        leader_epoch: None,
        datoms: vec![partition_status_datom(1, "ready", 1)],
    })?;
    service.append_partition(PartitionAppendRequest {
        partition: PartitionId::new("authority"),
        leader_epoch: None,
        datoms: vec![partition_owner_datom(1, "worker-a", 3)],
    })?;

    Ok(ReplicatedPartitionFixture {
        _root: guard,
        root,
        partition: PartitionId::new("authority"),
        follower_replica: ReplicaId::new(2),
        configs,
        stale_epoch: LeaderEpoch::new(1),
        leader_append_datoms: vec![partition_owner_datom(1, "worker-b", 4)],
        federated_run_request: FederatedRunDocumentRequest {
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
        },
        federated_history_request: FederatedHistoryRequest {
            cut: FederatedCut {
                cuts: vec![
                    PartitionCut::as_of("readiness", ElementId::new(1)),
                    PartitionCut::as_of("authority", ElementId::new(3)),
                ],
            },
            policy_context: None,
        },
    })
}

fn clone_replicated_fixture_root(root: &Path) -> Result<TempDirGuard, ApiError> {
    let cloned = unique_temp_dir("replicated-perf-copy");
    copy_dir_all(root, &cloned)?;
    Ok(TempDirGuard { path: cloned })
}

fn copy_dir_all(from: &Path, to: &Path) -> Result<(), ApiError> {
    fs::create_dir_all(to).map_err(|source| {
        ApiError::Validation(format!(
            "failed to create perf directory {}: {source}",
            to.display()
        ))
    })?;
    for entry in fs::read_dir(from).map_err(|source| {
        ApiError::Validation(format!(
            "failed to read perf directory {}: {source}",
            from.display()
        ))
    })? {
        let entry = entry.map_err(|source| {
            ApiError::Validation(format!(
                "failed to read perf directory entry {}: {source}",
                from.display()
            ))
        })?;
        let source_path = entry.path();
        let destination_path = to.join(entry.file_name());
        if entry
            .file_type()
            .map_err(|source| {
                ApiError::Validation(format!(
                    "failed to read file type for {}: {source}",
                    source_path.display()
                ))
            })?
            .is_dir()
        {
            copy_dir_all(&source_path, &destination_path)?;
        } else {
            fs::copy(&source_path, &destination_path).map_err(|source| {
                ApiError::Validation(format!(
                    "failed to copy {} to {}: {source}",
                    source_path.display(),
                    destination_path.display()
                ))
            })?;
        }
    }
    Ok(())
}

fn benchmark_replicated_leader_append_impl(
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_replicated_partition_fixture()?;
    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::ReplicatedPartition,
            workload: "Replicated leader append admission",
            scale: format!(
                "{} datoms",
                format_count(fixture.leader_append_datoms.len())
            ),
            units: fixture.leader_append_datoms.len().max(1),
            unit_label: "datoms/s",
            metrics: vec![
                metric("replicas", 2.0, "replicas"),
                metric("leader_epoch", fixture.stale_epoch.0 as f64, "epoch"),
            ],
            notes: vec![
                "fresh SQLite-backed leader/follower partition per sample".into(),
                "append admission includes synchronous follower replay in leader order".into(),
            ],
            samples,
            iterations_per_sample: 1,
        },
        observer,
        move || {
            let cloned = clone_replicated_fixture_root(&fixture.root)?;
            let mut service =
                ReplicatedAuthorityPartitionService::open(&cloned.path, fixture.configs.clone())?;
            service.append_partition(PartitionAppendRequest {
                partition: fixture.partition.clone(),
                leader_epoch: None,
                datoms: fixture.leader_append_datoms.clone(),
            })
        },
    )
}

fn benchmark_replicated_follower_replay_impl(
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_replicated_partition_fixture()?;
    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::ReplicatedPartition,
            workload: "Replicated follower replay catch-up",
            scale: "authority follower".into(),
            units: 1,
            unit_label: "followers/s",
            metrics: vec![
                metric("replicas", 2.0, "replicas"),
                metric("batch_datoms", fixture.leader_append_datoms.len() as f64, "datoms"),
            ],
            notes: vec![
                "measures append followed by follower status confirmation on a fresh replicated root"
                    .into(),
            ],
            samples,
            iterations_per_sample: 1,
        },
        observer,
        move || {
            let cloned = clone_replicated_fixture_root(&fixture.root)?;
            let mut service =
                ReplicatedAuthorityPartitionService::open(&cloned.path, fixture.configs.clone())?;
            service.append_partition(PartitionAppendRequest {
                partition: fixture.partition.clone(),
                leader_epoch: None,
                datoms: fixture.leader_append_datoms.clone(),
            })?;
            let status = service.partition_status()?;
            let authority = status
                .partitions
                .iter()
                .find(|partition| partition.partition == fixture.partition)
                .ok_or_else(|| ApiError::Validation("missing authority partition status".into()))?;
            authority
                .replicas
                .iter()
                .find(|replica| replica.replica_id == fixture.follower_replica)
                .cloned()
                .ok_or_else(|| ApiError::Validation("missing follower replica status".into()))
        },
    )
}

fn benchmark_replicated_federated_history_impl(
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_replicated_partition_fixture()?;
    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::ReplicatedPartition,
            workload: "Replicated federated history read",
            scale: "2 partitions".into(),
            units: fixture.federated_history_request.cut.cuts.len(),
            unit_label: "partitions/s",
            metrics: vec![
                metric(
                    "import_streams",
                    fixture.federated_run_request.imports.len() as f64,
                    "streams",
                ),
                metric(
                    "partitions",
                    fixture.federated_history_request.cut.cuts.len() as f64,
                    "partitions",
                ),
            ],
            notes: vec![
                "exact federated history over explicit partition cuts on a fresh replicated root"
                    .into(),
            ],
            samples,
            iterations_per_sample: 1,
        },
        observer,
        move || {
            let cloned = clone_replicated_fixture_root(&fixture.root)?;
            let service =
                ReplicatedAuthorityPartitionService::open(&cloned.path, fixture.configs.clone())?;
            service.federated_history(fixture.federated_history_request.clone())
        },
    )
}

fn benchmark_replicated_federated_report_impl(
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_replicated_partition_fixture()?;
    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::ReplicatedPartition,
            workload: "Replicated federated run report",
            scale: format!("{} import streams", fixture.federated_run_request.imports.len()),
            units: fixture.federated_run_request.imports.len().max(1),
            unit_label: "streams/s",
            metrics: vec![
                metric(
                    "import_streams",
                    fixture.federated_run_request.imports.len() as f64,
                    "streams",
                ),
                metric("named_queries", 1.0, "queries"),
            ],
            notes: vec![
                "builds the federated explain/report artifact over provenance-exact imported streams"
                    .into(),
            ],
            samples,
            iterations_per_sample: 1,
        },
        observer,
        move || {
            let cloned = clone_replicated_fixture_root(&fixture.root)?;
            let mut service =
                ReplicatedAuthorityPartitionService::open(&cloned.path, fixture.configs.clone())?;
            service.build_federated_explain_report(fixture.federated_run_request.clone())
        },
    )
}

fn benchmark_replicated_manual_promotion_impl(
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_replicated_partition_fixture()?;
    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::ReplicatedPartition,
            workload: "Replicated manual promotion",
            scale: "authority follower".into(),
            units: 1,
            unit_label: "promotions/s",
            metrics: vec![metric(
                "starting_epoch",
                fixture.stale_epoch.0 as f64,
                "epoch",
            )],
            notes: vec![
                "manual follower promotion increments the leader epoch and fences stale writes"
                    .into(),
            ],
            samples,
            iterations_per_sample: 1,
        },
        observer,
        move || {
            let cloned = clone_replicated_fixture_root(&fixture.root)?;
            let mut service =
                ReplicatedAuthorityPartitionService::open(&cloned.path, fixture.configs.clone())?;
            service.promote_replica(PromoteReplicaRequest {
                partition: fixture.partition.clone(),
                replica_id: fixture.follower_replica,
            })
        },
    )
}

fn benchmark_replicated_stale_append_impl(
    samples: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfMeasurement, ApiError> {
    let fixture = build_replicated_partition_fixture()?;
    benchmark_measurement(
        MeasurementPlan {
            group: PerfSuiteId::ReplicatedPartition,
            workload: "Replicated stale leader append rejection",
            scale: "stale epoch".into(),
            units: 1,
            unit_label: "rejections/s",
            metrics: vec![
                metric("stale_epoch", fixture.stale_epoch.0 as f64, "epoch"),
                metric(
                    "batch_datoms",
                    fixture.leader_append_datoms.len() as f64,
                    "datoms",
                ),
            ],
            notes: vec![
                "manual promotion is followed by an append from the old leader epoch and must fail"
                    .into(),
            ],
            samples,
            iterations_per_sample: 1,
        },
        observer,
        move || {
            let cloned = clone_replicated_fixture_root(&fixture.root)?;
            let mut service =
                ReplicatedAuthorityPartitionService::open(&cloned.path, fixture.configs.clone())?;
            let _ = service.promote_replica(PromoteReplicaRequest {
                partition: fixture.partition.clone(),
                replica_id: fixture.follower_replica,
            })?;
            match service.append_partition(PartitionAppendRequest {
                partition: fixture.partition.clone(),
                leader_epoch: Some(fixture.stale_epoch.clone()),
                datoms: fixture.leader_append_datoms.clone(),
            }) {
                Ok(_) => Err(ApiError::Validation(
                    "stale leader append benchmark unexpectedly succeeded".into(),
                )),
                Err(ApiError::Validation(message)) if message.contains("stale leader epoch") => {
                    Ok(message)
                }
                Err(error) => Err(error),
            }
        },
    )
}

pub fn default_performance_bundle() -> Result<PerfRunBundle, ApiError> {
    performance_bundle_for_suite(
        PerfSuiteId::FullStack,
        DEFAULT_REPORT_SAMPLES,
        None::<&Path>,
    )
}

pub fn performance_bundle_for_suite(
    suite_id: PerfSuiteId,
    samples_per_workload: usize,
    host_manifest_path: Option<impl AsRef<Path>>,
) -> Result<PerfRunBundle, ApiError> {
    performance_bundle_for_suite_impl(
        suite_id,
        samples_per_workload,
        host_manifest_path.as_ref().map(|path| path.as_ref()),
        None,
    )
}

pub fn default_performance_report() -> Result<PerfReport, ApiError> {
    Ok(default_performance_bundle()?.report)
}

pub fn default_performance_report_with_events<F>(mut observer: F) -> Result<PerfReport, ApiError>
where
    F: FnMut(PerfEvent),
{
    Ok(performance_bundle_for_suite_impl(
        PerfSuiteId::FullStack,
        DEFAULT_REPORT_SAMPLES,
        None,
        Some(&mut observer),
    )?
    .report)
}

pub fn baseline_from_bundle(label: impl Into<String>, bundle: &PerfRunBundle) -> PerfBaseline {
    PerfBaseline {
        label: label.into(),
        generated_at: bundle.generated_at.clone(),
        suite_id: Some(bundle.run.suite_id),
        host_snapshot: Some(bundle.host_snapshot.clone()),
        host_manifest_id: bundle.run.host_manifest_id.clone(),
        run_metadata: Some(bundle.run.clone()),
        report: bundle.report.clone(),
    }
}

fn performance_bundle_for_suite_impl(
    suite_id: PerfSuiteId,
    samples_per_workload: usize,
    host_manifest_path: Option<&Path>,
    mut observer: Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfRunBundle, ApiError> {
    let host_snapshot = collect_host_snapshot();
    let host_manifest = resolve_host_manifest(host_manifest_path, &host_snapshot)?;
    let run = build_run_metadata(
        suite_id,
        samples_per_workload,
        host_manifest.as_ref(),
        &host_snapshot,
    );
    emit_event(
        &mut observer,
        PerfEvent::SuiteStart {
            suite_id,
            host_snapshot: host_snapshot.clone(),
            total_workloads: suite_workload_count(suite_id),
            samples_per_workload,
        },
    );

    let mut measurements = Vec::new();
    for group in suite_groups(suite_id) {
        emit_event(
            &mut observer,
            PerfEvent::WorkloadGroupStart {
                group: *group,
                workload_count: group_workload_count(*group),
            },
        );
        measurements.extend(run_measurement_group(
            *group,
            samples_per_workload,
            &mut observer,
        )?);
    }

    let footprints = run_footprints_for_suite(suite_id)?;
    for footprint in &footprints {
        emit_event(
            &mut observer,
            PerfEvent::FootprintComputed {
                footprint: footprint.clone(),
            },
        );
    }

    Ok(PerfRunBundle {
        label: format!(
            "{}:{}",
            suite_id,
            host_manifest
                .as_ref()
                .map(|manifest| manifest.host_id.as_str())
                .unwrap_or_else(|| host_snapshot.hostname.as_str())
        ),
        generated_at: run.timestamp.clone(),
        host_snapshot,
        host_manifest,
        run,
        report: PerfReport {
            samples_per_workload,
            measurements,
            footprints,
        },
    })
}

fn resolve_host_manifest(
    explicit_path: Option<&Path>,
    host_snapshot: &PerfHostSnapshot,
) -> Result<Option<PerfHostManifest>, ApiError> {
    if let Some(path) = explicit_path {
        return load_host_manifest(path).map(Some);
    }
    let Some(default_path) = default_host_manifest_path(host_snapshot.execution_environment) else {
        return Ok(None);
    };
    let path = Path::new(default_path);
    if path.exists() {
        load_host_manifest(path).map(Some)
    } else {
        Ok(None)
    }
}

fn default_host_manifest_path(environment: PerfExecutionEnvironment) -> Option<&'static str> {
    match environment {
        PerfExecutionEnvironment::NativeWindows => {
            Some("fixtures/performance/hosts/dev-chad-windows-native.json")
        }
        PerfExecutionEnvironment::WslUbuntu => {
            Some("fixtures/performance/hosts/dev-chad-wsl-ubuntu.json")
        }
        PerfExecutionEnvironment::GithubWindows => {
            Some("fixtures/performance/hosts/github-windows-latest.json")
        }
        PerfExecutionEnvironment::GithubUbuntu => {
            Some("fixtures/performance/hosts/github-ubuntu-latest.json")
        }
        PerfExecutionEnvironment::NativeLinux | PerfExecutionEnvironment::Unknown => None,
    }
}

fn suite_workload_count(suite_id: PerfSuiteId) -> usize {
    suite_groups(suite_id)
        .iter()
        .map(|group| group_workload_count(*group))
        .sum()
}

fn group_workload_count(group: PerfSuiteId) -> usize {
    match group {
        PerfSuiteId::CoreKernel => 10,
        PerfSuiteId::ServiceInProcess => 2,
        PerfSuiteId::HttpPilotBoundary => 6,
        PerfSuiteId::ReplicatedPartition => 6,
        PerfSuiteId::FullStack => 24,
        PerfSuiteId::Legacy => 12,
    }
}

fn run_measurement_group(
    group: PerfSuiteId,
    samples_per_workload: usize,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<Vec<PerfMeasurement>, ApiError> {
    match group {
        PerfSuiteId::CoreKernel => Ok(vec![
            benchmark_append_impl(10_000, samples_per_workload, observer)?,
            benchmark_append_impl(50_000, samples_per_workload, observer)?,
            benchmark_resolve_current_impl(1_000, samples_per_workload, observer)?,
            benchmark_resolve_as_of_impl(1_000, samples_per_workload, observer)?,
            benchmark_durable_restart_current_impl(1_000, samples_per_workload, observer)?,
            benchmark_compile_scc_impl(16, samples_per_workload, observer)?,
            benchmark_compile_scc_impl(64, samples_per_workload, observer)?,
            benchmark_runtime_closure_impl(64, samples_per_workload, observer)?,
            benchmark_runtime_closure_impl(128, samples_per_workload, observer)?,
            benchmark_explain_trace_impl(128, samples_per_workload, observer)?,
        ]),
        PerfSuiteId::ServiceInProcess => Ok(vec![
            benchmark_service_coordination_impl(128, samples_per_workload, observer)?,
            benchmark_durable_restart_coordination_impl(128, samples_per_workload, observer)?,
        ]),
        PerfSuiteId::HttpPilotBoundary => Ok(vec![
            benchmark_http_health_impl(samples_per_workload, observer)?,
            benchmark_http_status_impl(samples_per_workload, observer)?,
            benchmark_http_history_impl(samples_per_workload, observer)?,
            benchmark_http_coordination_report_impl(samples_per_workload, observer)?,
            benchmark_http_explain_tuple_impl(samples_per_workload, observer)?,
            benchmark_http_coordination_delta_impl(samples_per_workload, observer)?,
        ]),
        PerfSuiteId::ReplicatedPartition => Ok(vec![
            benchmark_replicated_leader_append_impl(samples_per_workload, observer)?,
            benchmark_replicated_follower_replay_impl(samples_per_workload, observer)?,
            benchmark_replicated_federated_history_impl(samples_per_workload, observer)?,
            benchmark_replicated_federated_report_impl(samples_per_workload, observer)?,
            benchmark_replicated_manual_promotion_impl(samples_per_workload, observer)?,
            benchmark_replicated_stale_append_impl(samples_per_workload, observer)?,
        ]),
        PerfSuiteId::FullStack => unreachable!("full_stack is expanded via suite_groups"),
        PerfSuiteId::Legacy => Ok(vec![
            benchmark_append_impl(10_000, samples_per_workload, observer)?,
            benchmark_append_impl(50_000, samples_per_workload, observer)?,
            benchmark_resolve_current_impl(1_000, samples_per_workload, observer)?,
            benchmark_resolve_as_of_impl(1_000, samples_per_workload, observer)?,
            benchmark_durable_restart_current_impl(1_000, samples_per_workload, observer)?,
            benchmark_compile_scc_impl(16, samples_per_workload, observer)?,
            benchmark_compile_scc_impl(64, samples_per_workload, observer)?,
            benchmark_runtime_closure_impl(64, samples_per_workload, observer)?,
            benchmark_runtime_closure_impl(128, samples_per_workload, observer)?,
            benchmark_explain_trace_impl(128, samples_per_workload, observer)?,
            benchmark_service_coordination_impl(128, samples_per_workload, observer)?,
            benchmark_durable_restart_coordination_impl(128, samples_per_workload, observer)?,
        ]),
    }
}

fn run_footprints_for_suite(suite_id: PerfSuiteId) -> Result<Vec<FootprintEstimate>, ApiError> {
    if matches!(
        suite_id,
        PerfSuiteId::CoreKernel | PerfSuiteId::FullStack | PerfSuiteId::Legacy
    ) {
        Ok(vec![
            estimate_runtime_footprint(128)?,
            estimate_trace_footprint(128)?,
        ])
    } else {
        Ok(Vec::new())
    }
}

pub fn render_markdown_report(report: &PerfReport) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "# AETHER Performance Report");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "Collected on `{}` / `{}` with {} timed samples per workload.",
        std::env::consts::OS,
        std::env::consts::ARCH,
        report.samples_per_workload
    );
    let _ = writeln!(output);
    render_markdown_report_sections(&mut output, report);
    output
}

pub fn render_markdown_bundle(bundle: &PerfRunBundle) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "# AETHER Performance Report");
    let _ = writeln!(output);
    let _ = writeln!(output, "- Suite: `{}`", bundle.run.suite_id);
    let _ = writeln!(
        output,
        "- Host: `{}`",
        bundle
            .host_manifest
            .as_ref()
            .map(|manifest| manifest.host_id.as_str())
            .unwrap_or_else(|| bundle.host_snapshot.hostname.as_str())
    );
    let _ = writeln!(output, "- Display name: `{}`", display_host_name(bundle));
    let _ = writeln!(output, "- Generated at: `{}`", bundle.generated_at);
    let _ = writeln!(output, "- Build profile: `{}`", bundle.run.build_profile);
    let _ = writeln!(
        output,
        "- Samples per workload: `{}`",
        bundle.report.samples_per_workload
    );
    let _ = writeln!(
        output,
        "- Execution environment: `{}`",
        bundle.run.execution_environment
    );
    let _ = writeln!(output, "- CPU: `{}`", bundle.host_snapshot.cpu_brand);
    let _ = writeln!(
        output,
        "- Logical cores: `{}`",
        bundle
            .host_snapshot
            .logical_cores
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".into())
    );
    let _ = writeln!(
        output,
        "- RAM bytes: `{}`",
        bundle
            .host_snapshot
            .total_memory_bytes
            .map(|value| format_count(value as usize))
            .unwrap_or_else(|| "-".into())
    );
    if let Some(commit) = &bundle.run.git_commit {
        let _ = writeln!(output, "- Commit: `{commit}`");
    }
    if let Some(dirty) = bundle.run.git_dirty {
        let _ = writeln!(output, "- Dirty worktree: `{dirty}`");
    }
    let _ = writeln!(output);
    render_markdown_report_sections(&mut output, &bundle.report);
    output
}

fn render_markdown_report_sections(output: &mut String, report: &PerfReport) {
    let mut grouped: BTreeMap<String, Vec<&PerfMeasurement>> = BTreeMap::new();
    for measurement in &report.measurements {
        let key = measurement
            .group
            .map(|group| group.to_string())
            .unwrap_or_else(|| "ungrouped".into());
        grouped.entry(key).or_default().push(measurement);
    }
    for (group, measurements) in grouped {
        let _ = writeln!(output, "## {}", group.replace('_', " "));
        let _ = writeln!(output);
        let _ = writeln!(
            output,
            "| Workload | Scale | Mean | Min | Max | Throughput | Metrics | Notes |"
        );
        let _ = writeln!(
            output,
            "| --- | --- | ---: | ---: | ---: | ---: | --- | --- |"
        );
        for measurement in measurements {
            let _ = writeln!(
                output,
                "| {} | {} | {} | {} | {} | {}/{} | {} | {} |",
                measurement.workload,
                measurement.scale,
                format_duration(measurement.latency.mean),
                format_duration(measurement.latency.min),
                format_duration(measurement.latency.max),
                format_rate(measurement.throughput_per_second),
                measurement.unit_label,
                format_metrics(&measurement.metrics),
                measurement.notes.join("<br>")
            );
        }
        let _ = writeln!(output);
    }

    let _ = writeln!(output, "## Footprint Estimates");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "| Group | Workload | Scale | Estimated bytes | Metrics | Notes |"
    );
    let _ = writeln!(output, "| --- | --- | --- | ---: | --- | --- |");
    for footprint in &report.footprints {
        let _ = writeln!(
            output,
            "| {} | {} | {} | {} | {} | {} |",
            footprint
                .group
                .map(|group| group.to_string())
                .unwrap_or_else(|| "-".into()),
            footprint.workload,
            footprint.scale,
            format_count(footprint.estimated_bytes),
            format_metrics(&footprint.metrics),
            footprint.notes.join("<br>")
        );
    }
    let _ = writeln!(output);
    let _ = writeln!(output, "## Interpretation");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "- Core kernel and in-process service workloads are the current release-gated regression surfaces."
    );
    let _ = writeln!(
        output,
        "- HTTP pilot boundary and replicated partition workloads are measured immediately, but remain observational until their variance is understood."
    );
    let _ = writeln!(
        output,
        "- Durable restart and replicated-path timings include replay work; they are meant for operator planning as much as raw speed tracking."
    );
    let _ = writeln!(
        output,
        "- Footprint figures are structural lower-bound estimates, not allocator-exact telemetry."
    );
}

pub fn compare_perf_reports(
    current: &PerfReport,
    baseline: &PerfBaseline,
    budgets: &PerfDriftBudget,
    generated_at: impl Into<String>,
) -> PerfDriftReport {
    compare_reports_internal(
        current,
        PerfSuiteId::Legacy,
        baseline,
        budgets,
        generated_at.into(),
        baseline.host_manifest_id.clone(),
        baseline
            .host_snapshot
            .as_ref()
            .map(PerfHostSnapshot::fingerprint),
    )
}

pub fn compare_perf_bundle_to_baseline(
    current: &PerfRunBundle,
    baseline: &PerfBaseline,
    budgets: &PerfDriftBudget,
    generated_at: impl Into<String>,
) -> Result<PerfDriftReport, ApiError> {
    validate_drift_compatibility(current, baseline)?;
    Ok(compare_reports_internal(
        &current.report,
        current.run.suite_id,
        baseline,
        budgets,
        generated_at.into(),
        current.run.host_manifest_id.clone(),
        Some(current.host_snapshot.fingerprint()),
    ))
}

fn validate_drift_compatibility(
    current: &PerfRunBundle,
    baseline: &PerfBaseline,
) -> Result<(), ApiError> {
    let baseline_suite = baseline
        .run_metadata
        .as_ref()
        .map(|metadata| metadata.suite_id)
        .or(baseline.suite_id)
        .unwrap_or(PerfSuiteId::Legacy);
    if baseline_suite != PerfSuiteId::Legacy && baseline_suite != current.run.suite_id {
        return Err(ApiError::Validation(format!(
            "baseline suite `{baseline_suite}` does not match current suite `{}`",
            current.run.suite_id
        )));
    }

    let baseline_host_id = baseline
        .run_metadata
        .as_ref()
        .and_then(|metadata| metadata.host_manifest_id.clone())
        .or_else(|| baseline.host_manifest_id.clone());
    if let (Some(current_host_id), Some(baseline_host_id)) = (
        current.run.host_manifest_id.as_ref(),
        baseline_host_id.as_ref(),
    ) {
        if current_host_id != baseline_host_id {
            return Err(ApiError::Validation(format!(
                "baseline host `{baseline_host_id}` does not match current host `{current_host_id}`"
            )));
        }
    }

    Ok(())
}

fn compare_reports_internal(
    current: &PerfReport,
    suite_id: PerfSuiteId,
    baseline: &PerfBaseline,
    budgets: &PerfDriftBudget,
    generated_at: String,
    host_manifest_id: Option<String>,
    host_fingerprint: Option<String>,
) -> PerfDriftReport {
    let baseline_measurements = baseline
        .report
        .measurements
        .iter()
        .map(|measurement| {
            (
                perf_key(&measurement.workload, &measurement.scale),
                measurement.throughput_per_second,
            )
        })
        .collect::<std::collections::HashMap<_, _>>();
    let baseline_footprints = baseline
        .report
        .footprints
        .iter()
        .map(|footprint| {
            (
                perf_key(&footprint.workload, &footprint.scale),
                footprint.estimated_bytes,
            )
        })
        .collect::<HashMap<_, _>>();

    let mut overall = DriftSeverity::Ok;
    let mut observed_overall = DriftSeverity::Ok;
    let measurements = current
        .measurements
        .iter()
        .map(|measurement| {
            let baseline_value = baseline_measurements
                .get(&perf_key(&measurement.workload, &measurement.scale))
                .copied();
            let throughput_delta_pct = baseline_value
                .map(|baseline| percent_delta(baseline, measurement.throughput_per_second));
            let severity = match throughput_delta_pct {
                Some(delta) if delta <= -budgets.fail_throughput_regression_pct => {
                    DriftSeverity::Fail
                }
                Some(delta) if delta <= -budgets.warn_throughput_regression_pct => {
                    DriftSeverity::Warn
                }
                Some(_) => DriftSeverity::Ok,
                None => DriftSeverity::MissingBaseline,
            };
            observed_overall = observed_overall.merge(severity);
            let gated_group = measurement.group.unwrap_or(suite_id);
            if gated_group.is_release_gated() {
                overall = overall.merge(severity);
            }

            PerfMeasurementDrift {
                group: measurement.group,
                workload: measurement.workload.clone(),
                scale: measurement.scale.clone(),
                unit_label: measurement.unit_label.clone(),
                baseline_throughput_per_second: baseline_value,
                current_throughput_per_second: measurement.throughput_per_second,
                throughput_delta_pct,
                severity,
            }
        })
        .collect();

    let footprints = current
        .footprints
        .iter()
        .map(|footprint| {
            let baseline_value = baseline_footprints
                .get(&perf_key(&footprint.workload, &footprint.scale))
                .copied();
            let estimated_bytes_delta_pct = baseline_value
                .map(|baseline| percent_delta(baseline as f64, footprint.estimated_bytes as f64));
            let severity = match estimated_bytes_delta_pct {
                Some(delta) if delta >= budgets.fail_footprint_growth_pct => DriftSeverity::Fail,
                Some(delta) if delta >= budgets.warn_footprint_growth_pct => DriftSeverity::Warn,
                Some(_) => DriftSeverity::Ok,
                None => DriftSeverity::MissingBaseline,
            };
            observed_overall = observed_overall.merge(severity);
            let gated_group = footprint.group.unwrap_or(suite_id);
            if gated_group.is_release_gated() {
                overall = overall.merge(severity);
            }

            PerfFootprintDrift {
                group: footprint.group,
                workload: footprint.workload.clone(),
                scale: footprint.scale.clone(),
                baseline_estimated_bytes: baseline_value,
                current_estimated_bytes: footprint.estimated_bytes,
                estimated_bytes_delta_pct,
                severity,
            }
        })
        .collect();

    PerfDriftReport {
        baseline_label: baseline.label.clone(),
        generated_at,
        suite_id: Some(suite_id),
        host_manifest_id,
        host_fingerprint,
        budgets: budgets.clone(),
        measurements,
        footprints,
        overall,
        observed_overall,
    }
}

pub fn render_markdown_drift_report(report: &PerfDriftReport) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "# AETHER Performance Drift Report");
    let _ = writeln!(output);
    let _ = writeln!(output, "- Generated at: `{}`", report.generated_at);
    let _ = writeln!(output, "- Baseline: `{}`", report.baseline_label);
    if let Some(suite_id) = report.suite_id {
        let _ = writeln!(output, "- Suite: `{suite_id}`");
    }
    if let Some(host_manifest_id) = &report.host_manifest_id {
        let _ = writeln!(output, "- Host: `{host_manifest_id}`");
    }
    let _ = writeln!(
        output,
        "- Gated overall: `{}`",
        format_severity(report.overall)
    );
    let _ = writeln!(
        output,
        "- Observed overall: `{}`",
        format_severity(report.observed_overall)
    );
    let _ = writeln!(output);

    let _ = writeln!(output, "## Throughput Drift");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "| Group | Workload | Scale | Baseline | Current | Delta | Severity |"
    );
    let _ = writeln!(output, "| --- | --- | --- | ---: | ---: | ---: | --- |");
    for measurement in &report.measurements {
        let _ = writeln!(
            output,
            "| {} | {} | {} | {} | {} | {} | {} |",
            measurement
                .group
                .map(|group| group.to_string())
                .unwrap_or_else(|| "-".into()),
            measurement.workload,
            measurement.scale,
            measurement
                .baseline_throughput_per_second
                .map(format_rate)
                .unwrap_or_else(|| "-".into()),
            format_rate(measurement.current_throughput_per_second),
            measurement
                .throughput_delta_pct
                .map(format_pct)
                .unwrap_or_else(|| "-".into()),
            format_severity(measurement.severity),
        );
    }

    let _ = writeln!(output);
    let _ = writeln!(output, "## Footprint Drift");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "| Group | Workload | Scale | Baseline bytes | Current bytes | Delta | Severity |"
    );
    let _ = writeln!(output, "| --- | --- | --- | ---: | ---: | ---: | --- |");
    for footprint in &report.footprints {
        let _ = writeln!(
            output,
            "| {} | {} | {} | {} | {} | {} | {} |",
            footprint
                .group
                .map(|group| group.to_string())
                .unwrap_or_else(|| "-".into()),
            footprint.workload,
            footprint.scale,
            footprint
                .baseline_estimated_bytes
                .map(format_count)
                .unwrap_or_else(|| "-".into()),
            format_count(footprint.current_estimated_bytes),
            footprint
                .estimated_bytes_delta_pct
                .map(format_pct)
                .unwrap_or_else(|| "-".into()),
            format_severity(footprint.severity),
        );
    }

    output
}

pub fn build_matrix_report(bundles: &[PerfRunBundle]) -> PerfMatrixReport {
    let mut rows = BTreeMap::<String, PerfMatrixRow>::new();
    for bundle in bundles {
        let host_id = bundle
            .host_manifest
            .as_ref()
            .map(|manifest| manifest.host_id.clone())
            .unwrap_or_else(|| bundle.host_snapshot.fingerprint());
        let host_name = display_host_name(bundle);
        for measurement in &bundle.report.measurements {
            let key = format!(
                "{}|measurement|{}|{}|{}",
                bundle.run.suite_id,
                measurement.workload,
                measurement.scale,
                measurement.unit_label
            );
            let row = rows.entry(key).or_insert_with(|| PerfMatrixRow {
                suite_id: bundle.run.suite_id,
                group: measurement.group,
                workload: measurement.workload.clone(),
                scale: measurement.scale.clone(),
                kind: "measurement".into(),
                unit: measurement.unit_label.clone(),
                cells: Vec::new(),
            });
            row.cells.push(PerfMatrixCell {
                host_id: host_id.clone(),
                host_name: host_name.clone(),
                suite_id: bundle.run.suite_id,
                generated_at: bundle.generated_at.clone(),
                mean_latency_ms: Some(measurement.latency.mean.as_secs_f64() * 1000.0),
                throughput_per_second: Some(measurement.throughput_per_second),
                estimated_bytes: None,
            });
        }
        for footprint in &bundle.report.footprints {
            let key = format!(
                "{}|footprint|{}|{}|bytes",
                bundle.run.suite_id, footprint.workload, footprint.scale
            );
            let row = rows.entry(key).or_insert_with(|| PerfMatrixRow {
                suite_id: bundle.run.suite_id,
                group: footprint.group,
                workload: footprint.workload.clone(),
                scale: footprint.scale.clone(),
                kind: "footprint".into(),
                unit: "bytes".into(),
                cells: Vec::new(),
            });
            row.cells.push(PerfMatrixCell {
                host_id: host_id.clone(),
                host_name: host_name.clone(),
                suite_id: bundle.run.suite_id,
                generated_at: bundle.generated_at.clone(),
                mean_latency_ms: None,
                throughput_per_second: None,
                estimated_bytes: Some(footprint.estimated_bytes),
            });
        }
    }

    PerfMatrixReport {
        generated_at: timestamp_string(),
        rows: rows.into_values().collect(),
    }
}

pub fn render_markdown_matrix_report(report: &PerfMatrixReport) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "# AETHER Performance Matrix");
    let _ = writeln!(output);
    let _ = writeln!(output, "- Generated at: `{}`", report.generated_at);
    let _ = writeln!(output);
    let mut grouped: BTreeMap<String, Vec<&PerfMatrixRow>> = BTreeMap::new();
    for row in &report.rows {
        grouped
            .entry(row.suite_id.to_string())
            .or_default()
            .push(row);
    }
    for (suite_id, rows) in grouped {
        let _ = writeln!(output, "## {}", suite_id.replace('_', " "));
        let _ = writeln!(output);
        for row in rows {
            let _ = writeln!(output, "### {} | {}", row.workload, row.scale);
            let _ = writeln!(output);
            let _ = writeln!(
                output,
                "| Host | Group | Kind | Throughput | Mean latency (ms) | Estimated bytes | Generated |"
            );
            let _ = writeln!(output, "| --- | --- | --- | ---: | ---: | ---: | --- |");
            for cell in &row.cells {
                let _ = writeln!(
                    output,
                    "| {} | {} | {} | {} | {} | {} | {} |",
                    cell.host_name,
                    row.group
                        .map(|group| group.to_string())
                        .unwrap_or_else(|| "-".into()),
                    row.kind,
                    cell.throughput_per_second
                        .map(format_rate)
                        .unwrap_or_else(|| "-".into()),
                    cell.mean_latency_ms
                        .map(|value| format!("{value:.3}"))
                        .unwrap_or_else(|| "-".into()),
                    cell.estimated_bytes
                        .map(format_count)
                        .unwrap_or_else(|| "-".into()),
                    cell.generated_at
                );
            }
            let _ = writeln!(output);
        }
    }
    output
}

pub fn expected_transitive_pairs(chain_len: usize) -> usize {
    chain_len.saturating_mul(chain_len.saturating_sub(1)) / 2
}

pub fn estimate_derived_set_bytes(derived: &DerivedSet) -> usize {
    let mut bytes = size_of::<DerivedSet>();
    bytes += derived.tuples.capacity() * size_of::<aether_ast::DerivedTuple>();
    bytes += derived.iterations.capacity() * size_of::<RuntimeIteration>();
    bytes +=
        derived.predicate_index.capacity() * (size_of::<PredicateId>() + size_of::<Vec<TupleId>>());

    for tuple in &derived.tuples {
        bytes += size_of::<aether_ast::Tuple>();
        bytes += tuple.tuple.values.capacity() * size_of::<Value>();
        bytes += tuple.metadata.parent_tuple_ids.capacity() * size_of::<TupleId>();
        bytes += tuple.metadata.source_datom_ids.capacity() * size_of::<ElementId>();
        for value in &tuple.tuple.values {
            bytes += estimate_value_bytes(value);
        }
    }

    for tuple_ids in derived.predicate_index.values() {
        bytes += tuple_ids.capacity() * size_of::<TupleId>();
    }

    bytes
}

pub fn estimate_derivation_trace_bytes(trace: &DerivationTrace) -> usize {
    let mut bytes = size_of::<DerivationTrace>();
    bytes += trace.tuples.capacity() * size_of::<aether_ast::DerivedTuple>();
    for tuple in &trace.tuples {
        bytes += size_of::<aether_ast::Tuple>();
        bytes += tuple.tuple.values.capacity() * size_of::<Value>();
        bytes += tuple.metadata.parent_tuple_ids.capacity() * size_of::<TupleId>();
        bytes += tuple.metadata.source_datom_ids.capacity() * size_of::<ElementId>();
        for value in &tuple.tuple.values {
            bytes += estimate_value_bytes(value);
        }
    }
    bytes
}

fn benchmark_measurement<T, F>(
    plan: MeasurementPlan,
    observer: &mut Option<&mut dyn FnMut(PerfEvent)>,
    mut operation: F,
) -> Result<PerfMeasurement, ApiError>
where
    F: FnMut() -> Result<T, ApiError>,
{
    let samples = plan.samples.max(1);
    let iterations_per_sample = plan.iterations_per_sample.max(1);
    let effective_units = plan.units.saturating_mul(iterations_per_sample);
    let mut durations = Vec::with_capacity(samples);
    emit_event(
        observer,
        PerfEvent::MeasurementStart {
            group: plan.group,
            workload: plan.workload,
            scale: plan.scale.clone(),
            total_samples: samples,
            units: effective_units,
            unit_label: plan.unit_label,
            metrics: plan.metrics.clone(),
            notes: plan.notes.clone(),
        },
    );

    let mut total = Duration::default();
    let mut min = Duration::default();
    let mut max = Duration::default();
    for _ in 0..samples {
        let started = Instant::now();
        for _ in 0..iterations_per_sample {
            let result = operation()?;
            black_box(result);
        }
        let elapsed = started.elapsed();
        durations.push(elapsed);
        total += elapsed;
        if durations.len() == 1 {
            min = elapsed;
            max = elapsed;
        } else {
            min = min.min(elapsed);
            max = max.max(elapsed);
        }
        let sample_throughput = if elapsed.is_zero() {
            0.0
        } else {
            effective_units as f64 / elapsed.as_secs_f64()
        };
        emit_event(
            observer,
            PerfEvent::SampleRecorded {
                workload: plan.workload,
                scale: plan.scale.clone(),
                sample_index: durations.len(),
                total_samples: samples,
                elapsed,
                throughput_per_second: sample_throughput,
                mean_so_far: total / (durations.len() as u32),
                min_so_far: min,
                max_so_far: max,
            },
        );
    }

    let mean = total / (samples as u32);
    let throughput_per_second = if mean.is_zero() {
        0.0
    } else {
        effective_units as f64 / mean.as_secs_f64()
    };

    let measurement = PerfMeasurement {
        group: Some(plan.group),
        workload: plan.workload.into(),
        scale: plan.scale,
        units: effective_units,
        unit_label: plan.unit_label.into(),
        latency: LatencyStats {
            samples,
            mean,
            min,
            max,
        },
        throughput_per_second,
        metrics: plan.metrics,
        notes: plan.notes,
    };
    emit_event(
        observer,
        PerfEvent::MeasurementComplete {
            measurement: measurement.clone(),
        },
    );
    Ok(measurement)
}

fn validate_chain_len(chain_len: usize) -> Result<usize, ApiError> {
    if chain_len < 2 {
        return Err(ApiError::Validation(
            "transitive-closure fixtures require a chain length of at least 2".into(),
        ));
    }
    Ok(chain_len)
}

fn dependency_schema() -> Schema {
    let mut schema = Schema::new("perf-v1");
    register_attribute(
        &mut schema,
        AttributeSchema {
            id: AttributeId::new(1),
            name: "task.depends_on".into(),
            class: AttributeClass::RefSet,
            value_type: ValueType::Entity,
        },
    );
    register_predicate(
        &mut schema,
        &predicate(1, "task_depends_on", 2),
        vec![ValueType::Entity, ValueType::Entity],
    );
    register_predicate(
        &mut schema,
        &predicate(2, "depends_transitive", 2),
        vec![ValueType::Entity, ValueType::Entity],
    );
    schema
}

fn dependency_program() -> RuleProgram {
    let task_depends_on = predicate(1, "task_depends_on", 2);
    let depends_transitive = predicate(2, "depends_transitive", 2);

    RuleProgram {
        predicates: vec![task_depends_on.clone(), depends_transitive.clone()],
        rules: vec![
            RuleAst {
                id: RuleId::new(1),
                head: atom(depends_transitive.clone(), &["x", "y"]),
                body: vec![Literal::Positive(atom(
                    task_depends_on.clone(),
                    &["x", "y"],
                ))],
            },
            RuleAst {
                id: RuleId::new(2),
                head: atom(depends_transitive, &["x", "z"]),
                body: vec![
                    Literal::Positive(atom(predicate(2, "depends_transitive", 2), &["x", "y"])),
                    Literal::Positive(atom(task_depends_on, &["y", "z"])),
                ],
            },
        ],
        materialized: vec![PredicateId::new(2)],
        facts: Vec::new(),
    }
}

fn dependency_chain_datoms(chain_len: usize) -> Vec<Datom> {
    (1..chain_len)
        .map(|entity| {
            datom(
                entity as u64,
                1,
                Value::Entity(EntityId::new((entity + 1) as u64)),
                OperationKind::Add,
                entity as u64,
            )
        })
        .collect()
}

fn coordination_datoms(task_count: usize) -> Vec<Datom> {
    let mut datoms = Vec::new();
    let mut next_element = 1u64;
    for task in 1..=task_count {
        if task_is_done(task) {
            datoms.push(datom(
                task as u64,
                2,
                Value::String("done".into()),
                OperationKind::Assert,
                next_element,
            ));
            next_element += 1;
        }

        if task_has_active_claim(task) {
            datoms.push(datom(
                task as u64,
                3,
                Value::String("worker-a".into()),
                OperationKind::Assert,
                next_element,
            ));
            next_element += 1;
            datoms.push(datom(
                task as u64,
                4,
                Value::U64(1),
                OperationKind::Assert,
                next_element,
            ));
            next_element += 1;
            datoms.push(datom(
                task as u64,
                5,
                Value::String("active".into()),
                OperationKind::Assert,
                next_element,
            ));
            next_element += 1;
        }
    }
    datoms
}

fn task_is_done(task: usize) -> bool {
    task % 4 == 0
}

fn task_has_active_claim(task: usize) -> bool {
    task % 7 == 0
}

fn coordination_claimability_rows(task_count: usize) -> usize {
    let ready_tasks = (1..=task_count)
        .filter(|task| !task_is_done(*task) && !task_has_active_claim(*task))
        .count();
    ready_tasks * 2
}

fn coordination_claimability_dsl(task_count: usize) -> String {
    let mut facts = String::new();
    for task in 1..=task_count {
        let _ = writeln!(facts, "  task(entity({task}))");
    }
    let _ = writeln!(facts, "  worker(\"worker-a\")");
    let _ = writeln!(facts, "  worker(\"worker-b\")");
    let _ = writeln!(facts, "  worker_capability(\"worker-a\", \"executor\")");
    let _ = writeln!(facts, "  worker_capability(\"worker-b\", \"executor\")");

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
  worker(String)
  worker_capability(String, String)
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
  worker_can_claim(Entity, String)
}}

facts {{
{facts}}}

rules {{
  task_complete(t) <- task_status(t, "done")
  dependency_blocked(t) <- task_depends_on(t, dep), not task_complete(dep)
  lease_active(t, w, epoch) <- task_claimed_by(t, w), task_lease_epoch(t, epoch), task_lease_state(t, "active")
  active_claim(t) <- lease_active(t, w, epoch)
  task_ready(t) <- task(t), not task_complete(t), not dependency_blocked(t), not active_claim(t)
  worker_can_claim(t, w) <- task_ready(t), worker(w), worker_capability(w, "executor")
}}

materialize {{
  task_ready
  worker_can_claim
}}

query {{
  current
  goal worker_can_claim(t, w)
  keep t, w
}}
"#
    )
}

fn predicate(id: u64, name: &str, arity: usize) -> PredicateRef {
    PredicateRef {
        id: PredicateId::new(id),
        name: name.into(),
        arity,
    }
}

fn atom(predicate: PredicateRef, vars: &[&str]) -> Atom {
    Atom {
        predicate,
        terms: vars
            .iter()
            .map(|name| Term::Variable(Variable::new(*name)))
            .collect(),
    }
}

fn datom(entity: u64, attribute: u64, value: Value, op: OperationKind, element: u64) -> Datom {
    Datom {
        entity: EntityId::new(entity),
        attribute: AttributeId::new(attribute),
        value,
        op,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}

fn register_attribute(schema: &mut Schema, attribute: AttributeSchema) {
    schema
        .register_attribute(attribute)
        .expect("performance fixture should register attribute");
}

fn register_predicate(schema: &mut Schema, predicate: &PredicateRef, fields: Vec<ValueType>) {
    schema
        .register_predicate(PredicateSignature {
            id: predicate.id,
            name: predicate.name.clone(),
            fields,
        })
        .expect("performance fixture should register predicate");
}

fn estimate_value_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Bool(_) => size_of::<bool>(),
        Value::I64(_) => size_of::<i64>(),
        Value::U64(_) => size_of::<u64>(),
        Value::F64(_) => size_of::<f64>(),
        Value::String(text) => text.capacity(),
        Value::Bytes(bytes) => bytes.capacity(),
        Value::Entity(_) => size_of::<EntityId>(),
        Value::List(values) => {
            values.capacity() * size_of::<Value>()
                + values.iter().map(estimate_value_bytes).sum::<usize>()
        }
    }
}

fn format_duration(duration: Duration) -> String {
    if duration.as_secs_f64() >= 1.0 {
        format!("{:.2} s", duration.as_secs_f64())
    } else if duration.as_secs_f64() >= 0.001 {
        format!("{:.2} ms", duration.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.2} us", duration.as_secs_f64() * 1_000_000.0)
    }
}

fn format_rate(value: f64) -> String {
    if value >= 1_000_000.0 {
        format!("{:.2}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.2}K", value / 1_000.0)
    } else {
        format!("{value:.2}")
    }
}

fn format_metrics(metrics: &[PerfScalarMetric]) -> String {
    if metrics.is_empty() {
        return "-".into();
    }
    metrics
        .iter()
        .map(|metric| format!("{}={} {}", metric.name, metric.value, metric.unit))
        .collect::<Vec<_>>()
        .join("<br>")
}

fn format_pct(value: f64) -> String {
    format!("{value:+.2}%")
}

fn format_count(value: usize) -> String {
    let digits = value.to_string();
    let mut output = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, ch) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            output.push(',');
        }
        output.push(ch);
    }
    output
}

fn format_severity(severity: DriftSeverity) -> &'static str {
    match severity {
        DriftSeverity::Ok => "ok",
        DriftSeverity::Warn => "warn",
        DriftSeverity::Fail => "fail",
        DriftSeverity::MissingBaseline => "missing-baseline",
    }
}

fn perf_key(workload: &str, scale: &str) -> String {
    format!("{workload}|{scale}")
}

fn percent_delta(baseline: f64, current: f64) -> f64 {
    if baseline == 0.0 {
        0.0
    } else {
        ((current - baseline) / baseline) * 100.0
    }
}

fn emit_event(observer: &mut Option<&mut dyn FnMut(PerfEvent)>, event: PerfEvent) {
    if let Some(observer) = observer.as_deref_mut() {
        observer(event);
    }
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("aether-{prefix}-{unique}"))
}

#[cfg(test)]
mod tests {
    use super::{
        baseline_from_bundle, build_matrix_report, collect_host_snapshot,
        compare_perf_bundle_to_baseline, compare_perf_reports, DriftSeverity, FootprintEstimate,
        LatencyStats, PerfBaseline, PerfDriftBudget, PerfHostManifest, PerfHostSnapshot,
        PerfMeasurement, PerfReport, PerfRunBundle, PerfRunMetadata, PerfSuiteId,
    };
    use std::time::Duration;

    #[test]
    fn drift_report_flags_throughput_regressions_and_footprint_growth() {
        let baseline = PerfBaseline {
            label: "pilot-baseline".into(),
            generated_at: "2026-03-19 00:00:00".into(),
            suite_id: None,
            host_snapshot: None,
            host_manifest_id: None,
            run_metadata: None,
            report: PerfReport {
                samples_per_workload: 5,
                measurements: vec![PerfMeasurement {
                    group: Some(PerfSuiteId::ServiceInProcess),
                    workload: "Kernel service coordination run".into(),
                    scale: "128 tasks".into(),
                    units: 164,
                    unit_label: "rows/s".into(),
                    latency: LatencyStats {
                        samples: 5,
                        mean: Duration::from_millis(3),
                        min: Duration::from_millis(2),
                        max: Duration::from_millis(4),
                    },
                    throughput_per_second: 50_000.0,
                    metrics: Vec::new(),
                    notes: Vec::new(),
                }],
                footprints: vec![FootprintEstimate {
                    group: Some(PerfSuiteId::CoreKernel),
                    workload: "Derived-set footprint estimate".into(),
                    scale: "chain 128".into(),
                    estimated_bytes: 1_000,
                    metrics: Vec::new(),
                    notes: Vec::new(),
                }],
            },
        };
        let current = PerfReport {
            samples_per_workload: 5,
            measurements: vec![PerfMeasurement {
                group: Some(PerfSuiteId::ServiceInProcess),
                workload: "Kernel service coordination run".into(),
                scale: "128 tasks".into(),
                units: 164,
                unit_label: "rows/s".into(),
                latency: LatencyStats {
                    samples: 5,
                    mean: Duration::from_millis(5),
                    min: Duration::from_millis(4),
                    max: Duration::from_millis(6),
                },
                throughput_per_second: 35_000.0,
                metrics: Vec::new(),
                notes: Vec::new(),
            }],
            footprints: vec![FootprintEstimate {
                group: Some(PerfSuiteId::CoreKernel),
                workload: "Derived-set footprint estimate".into(),
                scale: "chain 128".into(),
                estimated_bytes: 1_250,
                metrics: Vec::new(),
                notes: Vec::new(),
            }],
        };

        let drift = compare_perf_reports(
            &current,
            &baseline,
            &PerfDriftBudget::default(),
            "2026-03-20 00:00:00",
        );

        assert_eq!(drift.measurements[0].severity, DriftSeverity::Fail);
        assert_eq!(drift.footprints[0].severity, DriftSeverity::Fail);
        assert_eq!(drift.overall, DriftSeverity::Fail);
    }

    #[test]
    fn drift_report_marks_missing_baseline_entries() {
        let baseline = PerfBaseline {
            label: "pilot-baseline".into(),
            generated_at: "2026-03-19 00:00:00".into(),
            suite_id: None,
            host_snapshot: None,
            host_manifest_id: None,
            run_metadata: None,
            report: PerfReport {
                samples_per_workload: 5,
                measurements: Vec::new(),
                footprints: Vec::new(),
            },
        };
        let current = PerfReport {
            samples_per_workload: 5,
            measurements: vec![PerfMeasurement {
                group: Some(PerfSuiteId::CoreKernel),
                workload: "Resolver current throughput".into(),
                scale: "1,000 entities".into(),
                units: 1_000,
                unit_label: "entities/s".into(),
                latency: LatencyStats {
                    samples: 5,
                    mean: Duration::from_millis(3),
                    min: Duration::from_millis(2),
                    max: Duration::from_millis(4),
                },
                throughput_per_second: 300_000.0,
                metrics: Vec::new(),
                notes: Vec::new(),
            }],
            footprints: Vec::new(),
        };

        let drift = compare_perf_reports(
            &current,
            &baseline,
            &PerfDriftBudget::default(),
            "2026-03-20 00:00:00",
        );

        assert_eq!(
            drift.measurements[0].severity,
            DriftSeverity::MissingBaseline
        );
        assert_eq!(drift.overall, DriftSeverity::MissingBaseline);
    }

    #[test]
    fn strict_bundle_compare_rejects_host_mismatch() {
        let bundle = fake_bundle("host-a", PerfSuiteId::CoreKernel);
        let baseline = PerfBaseline {
            label: "baseline".into(),
            generated_at: "2026-03-27".into(),
            suite_id: Some(PerfSuiteId::CoreKernel),
            host_snapshot: Some(PerfHostSnapshot {
                hostname: "host-b".into(),
                ..collect_host_snapshot()
            }),
            host_manifest_id: Some("host-b".into()),
            run_metadata: Some(PerfRunMetadata {
                suite_id: PerfSuiteId::CoreKernel,
                timestamp: "2026-03-27".into(),
                build_profile: "release".into(),
                samples_per_workload: 1,
                execution_environment: bundle.run.execution_environment,
                git_commit: None,
                git_dirty: Some(false),
                host_manifest_id: Some("host-b".into()),
            }),
            report: bundle.report.clone(),
        };

        let error = compare_perf_bundle_to_baseline(
            &bundle,
            &baseline,
            &PerfDriftBudget::default(),
            "2026-03-27",
        )
        .expect_err("host mismatch should fail");
        assert!(error.to_string().contains("baseline host"));
    }

    #[test]
    fn baseline_from_bundle_preserves_suite_metadata() {
        let bundle = fake_bundle("dev-chad-windows-native", PerfSuiteId::ServiceInProcess);
        let baseline = baseline_from_bundle("accepted", &bundle);
        assert_eq!(baseline.suite_id, Some(PerfSuiteId::ServiceInProcess));
        assert_eq!(
            baseline
                .run_metadata
                .as_ref()
                .and_then(|run| run.host_manifest_id.clone()),
            Some("dev-chad-windows-native".into())
        );
    }

    #[test]
    fn matrix_report_groups_rows_by_host_and_suite() {
        let report = build_matrix_report(&[
            fake_bundle("host-a", PerfSuiteId::CoreKernel),
            fake_bundle("host-b", PerfSuiteId::CoreKernel),
        ]);
        assert!(!report.rows.is_empty());
        assert!(report.rows.iter().any(|row| row.cells.len() == 2));
    }

    fn fake_bundle(host_id: &str, suite_id: PerfSuiteId) -> PerfRunBundle {
        let host_snapshot = collect_host_snapshot();
        PerfRunBundle {
            label: format!("{suite_id}:{host_id}"),
            generated_at: "2026-03-27".into(),
            host_snapshot: host_snapshot.clone(),
            host_manifest: Some(PerfHostManifest {
                host_id: host_id.into(),
                display_name: host_id.into(),
                host_class: "test".into(),
                execution_environment: Some(host_snapshot.execution_environment),
                vendor: None,
                model: None,
                owner: None,
                notes: Vec::new(),
                tags: Vec::new(),
            }),
            run: PerfRunMetadata {
                suite_id,
                timestamp: "2026-03-27".into(),
                build_profile: "debug".into(),
                samples_per_workload: 1,
                execution_environment: host_snapshot.execution_environment,
                git_commit: None,
                git_dirty: Some(false),
                host_manifest_id: Some(host_id.into()),
            },
            report: PerfReport {
                samples_per_workload: 1,
                measurements: vec![PerfMeasurement {
                    group: Some(suite_id),
                    workload: "Journal append throughput".into(),
                    scale: "10,000 datoms".into(),
                    units: 10_000,
                    unit_label: "datoms/s".into(),
                    latency: LatencyStats {
                        samples: 1,
                        mean: Duration::from_millis(1),
                        min: Duration::from_millis(1),
                        max: Duration::from_millis(1),
                    },
                    throughput_per_second: 10_000.0,
                    metrics: Vec::new(),
                    notes: Vec::new(),
                }],
                footprints: vec![FootprintEstimate {
                    group: Some(suite_id),
                    workload: "Derived-set footprint estimate".into(),
                    scale: "chain 128".into(),
                    estimated_bytes: 1_000,
                    metrics: Vec::new(),
                    notes: Vec::new(),
                }],
            },
        }
    }
}
