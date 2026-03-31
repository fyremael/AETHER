use super::*;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

const BOARD_LADDER: [usize; 5] = [128, 512, 1_024, 4_096, 8_192];
const CLOSURE_LADDER: [usize; 4] = [128, 256, 512, 1_024];
const EXPLAIN_LADDER: [usize; 3] = [128, 256, 512];
const REPLAY_ENTITY_LADDER: [usize; 3] = [1_000, 5_000, 10_000];
const REPLAY_BOARD_LADDER: [usize; 4] = [128, 512, 1_024, 4_096];
const CONCURRENCY_LADDER: [usize; 5] = [1, 4, 8, 16, 32];
const DEFAULT_CONCURRENCY_OPERATIONS_PER_WORKER: usize = 12;
const SNAPSHOT_OVERHEAD_FACTOR: f64 = 1.20;
const REFERENCE_WORKLOAD: &str = "governed incident board / pilot coordination surface";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CapacityNodeClass {
    #[serde(rename = "S")]
    S,
    #[serde(rename = "M")]
    M,
    #[serde(rename = "L")]
    L,
    #[serde(rename = "XL")]
    Xl,
}

impl CapacityNodeClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::S => "S",
            Self::M => "M",
            Self::L => "L",
            Self::Xl => "XL",
        }
    }
}

impl std::fmt::Display for CapacityNodeClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacityLimitingFactor {
    Cpu,
    Memory,
    ReplayWindow,
    ReportLatency,
}

impl std::fmt::Display for CapacityLimitingFactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Cpu => "cpu",
            Self::Memory => "memory",
            Self::ReplayWindow => "replay_window",
            Self::ReportLatency => "report_latency",
        };
        f.write_str(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacityConfidenceLevel {
    Low,
    Medium,
    High,
}

impl std::fmt::Display for CapacityConfidenceLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        };
        f.write_str(value)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CapacityHardwareClass {
    pub node_class: CapacityNodeClass,
    pub vcpu: usize,
    pub ram_bytes: u64,
    pub nvme_bytes: u64,
    pub target_p95_latency_ms: f64,
    pub target_replay_seconds: f64,
    #[serde(default)]
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfCapacityCurvePoint {
    pub scale_label: String,
    pub scale_value: usize,
    pub units: usize,
    pub unit_label: String,
    pub mean_latency_ms: f64,
    pub throughput_per_second: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_bytes: Option<u64>,
    #[serde(default)]
    pub metrics: Vec<PerfScalarMetric>,
    #[serde(default)]
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfCapacityCurve {
    pub family: String,
    pub label: String,
    pub points: Vec<PerfCapacityCurvePoint>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfCapacityConcurrencyPoint {
    pub concurrency: usize,
    pub total_operations: usize,
    pub throughput_per_second: f64,
    pub mean_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfCapacityConcurrencyPack {
    pub label: String,
    pub operations_per_worker: usize,
    pub operation_mix: Vec<String>,
    pub first_saturation_point: Option<usize>,
    pub points: Vec<PerfCapacityConcurrencyPoint>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfCapacityStoragePoint {
    pub family: String,
    pub scale_label: String,
    pub scale_value: usize,
    pub datom_count: usize,
    pub database_bytes: u64,
    pub wal_bytes: u64,
    pub shm_bytes: u64,
    pub snapshot_bytes: u64,
    pub restore_replay_seconds: f64,
    pub peak_rss_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidecar_catalog_bytes: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfCapacityInputBundle {
    pub generated_at: String,
    pub reference_workload: String,
    pub host_snapshot: PerfHostSnapshot,
    #[serde(default)]
    pub host_manifest: Option<PerfHostManifest>,
    pub curves: Vec<PerfCapacityCurve>,
    pub concurrency_pack: PerfCapacityConcurrencyPack,
    pub storage_points: Vec<PerfCapacityStoragePoint>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfCapacityArtifactPaths {
    pub perturbation_json_path: String,
    pub matrix_json_path: String,
    pub capacity_input_json_path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PerfPerturbationDriftStatus {
    pub suite: String,
    pub status: String,
    #[serde(default)]
    pub baseline_path: Option<String>,
    #[serde(default)]
    pub report_path: Option<String>,
    #[serde(default)]
    pub bundle_path: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PerfPerturbationPerformance {
    #[serde(default)]
    pub drift: Vec<PerfPerturbationDriftStatus>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PerfPerturbationArtifactPointer {
    pub json_path: String,
    pub report_path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PerfPerturbationSummary {
    pub generated_at: String,
    pub host_id: String,
    pub host_manifest_path: String,
    pub run_directory: String,
    pub performance: PerfPerturbationPerformance,
    #[serde(default)]
    pub capacity_inputs: Option<PerfPerturbationArtifactPointer>,
}

#[derive(Clone, Copy)]
enum MixedOperation {
    Health,
    Status,
    History,
    Report,
    Delta,
    Explain,
}

impl MixedOperation {
    fn label(self) -> &'static str {
        match self {
            Self::Health => "/health",
            Self::Status => "/v1/status",
            Self::History => "/v1/history",
            Self::Report => "/v1/reports/pilot/coordination",
            Self::Delta => "/v1/reports/pilot/coordination-delta",
            Self::Explain => "/v1/explain/tuple",
        }
    }
}

pub fn build_capacity_input_bundle(
    samples_per_point: usize,
    host_manifest_path: Option<&Path>,
) -> Result<PerfCapacityInputBundle, ApiError> {
    let host_snapshot = collect_host_snapshot();
    let host_manifest = host_manifest_path.map(load_host_manifest).transpose()?;
    let samples = samples_per_point.max(1);

    let mut curves = Vec::new();
    curves.push(build_capacity_curve(
        "pilot_board",
        "Pilot coordination board size",
        &BOARD_LADDER,
        |scale| {
            let measurement = benchmark_service_coordination(scale, samples)?;
            Ok(curve_point_from_measurement(scale, &measurement, None))
        },
    )?);
    curves.push(build_capacity_curve(
        "closure",
        "Recursive closure chain size",
        &CLOSURE_LADDER,
        |scale| {
            let measurement = benchmark_runtime_closure(scale, samples)?;
            let footprint = estimate_runtime_footprint(scale)?;
            Ok(curve_point_from_measurement(
                scale,
                &measurement,
                Some(footprint.estimated_bytes as u64),
            ))
        },
    )?);
    curves.push(build_capacity_curve(
        "explain",
        "Tuple explanation trace depth",
        &EXPLAIN_LADDER,
        |scale| {
            let measurement = benchmark_explain_trace(scale, samples)?;
            let footprint = estimate_trace_footprint(scale)?;
            Ok(curve_point_from_measurement(
                scale,
                &measurement,
                Some(footprint.estimated_bytes as u64),
            ))
        },
    )?);
    curves.push(build_capacity_curve(
        "durable_replay",
        "Durable replay entity count",
        &REPLAY_ENTITY_LADDER,
        |scale| {
            let measurement = benchmark_durable_restart_current(scale, samples)?;
            Ok(curve_point_from_measurement(scale, &measurement, None))
        },
    )?);
    curves.push(build_capacity_curve(
        "durable_coordination",
        "Durable coordination replay board size",
        &REPLAY_BOARD_LADDER,
        |scale| {
            let measurement = benchmark_durable_restart_coordination(scale, samples)?;
            Ok(curve_point_from_measurement(scale, &measurement, None))
        },
    )?);

    Ok(PerfCapacityInputBundle {
        generated_at: timestamp_string(),
        reference_workload: REFERENCE_WORKLOAD.into(),
        host_snapshot,
        host_manifest,
        curves,
        concurrency_pack: build_mixed_operator_concurrency_pack()?,
        storage_points: collect_storage_points()?,
    })
}

pub fn load_capacity_input_bundle(
    path: impl AsRef<Path>,
) -> Result<PerfCapacityInputBundle, ApiError> {
    let path = path.as_ref();
    serde_json::from_str(&fs::read_to_string(path).map_err(|source| {
        ApiError::Validation(format!(
            "failed to read capacity input bundle {}: {source}",
            path.display()
        ))
    })?)
    .map_err(|source| {
        ApiError::Validation(format!(
            "failed to parse capacity input bundle {}: {source}",
            path.display()
        ))
    })
}

pub fn render_markdown_capacity_input_bundle(bundle: &PerfCapacityInputBundle) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "# AETHER Capacity Curves");
    let _ = writeln!(output);
    let _ = writeln!(output, "- Generated at: `{}`", bundle.generated_at);
    let _ = writeln!(
        output,
        "- Reference workload: `{}`",
        bundle.reference_workload
    );
    let _ = writeln!(output, "- Host: `{}`", bundle.host_snapshot.hostname);
    if let Some(manifest) = &bundle.host_manifest {
        let _ = writeln!(output, "- Host manifest: `{}`", manifest.host_id);
    }
    let _ = writeln!(output);

    for curve in &bundle.curves {
        let _ = writeln!(output, "## {}", curve.label);
        let _ = writeln!(output);
        let _ = writeln!(
            output,
            "| Scale | Mean latency (ms) | Throughput | Footprint |"
        );
        let _ = writeln!(output, "| --- | ---: | ---: | ---: |");
        for point in &curve.points {
            let _ = writeln!(
                output,
                "| {} | {:.3} | {}/{} | {} |",
                point.scale_label,
                point.mean_latency_ms,
                format_rate(point.throughput_per_second),
                point.unit_label,
                point
                    .estimated_bytes
                    .map(format_capacity_bytes)
                    .unwrap_or_else(|| "-".into())
            );
        }
        let _ = writeln!(output);
    }

    let _ = writeln!(output, "## Mixed Operator Concurrency");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "| Concurrency | Throughput | Mean latency (ms) | p95 (ms) | p99 (ms) |"
    );
    let _ = writeln!(output, "| ---: | ---: | ---: | ---: | ---: |");
    for point in &bundle.concurrency_pack.points {
        let _ = writeln!(
            output,
            "| {} | {:.2} ops/s | {:.3} | {:.3} | {:.3} |",
            point.concurrency,
            point.throughput_per_second,
            point.mean_latency_ms,
            point.p95_latency_ms,
            point.p99_latency_ms
        );
    }
    if let Some(first) = bundle.concurrency_pack.first_saturation_point {
        let _ = writeln!(output);
        let _ = writeln!(
            output,
            "- First saturation point: `{first}` concurrent workers"
        );
    }
    let _ = writeln!(output);

    let _ = writeln!(output, "## Storage Planning Inputs");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "| Family | Scale | Datoms | DB | WAL | SHM | Snapshot | Replay | Peak RSS | Sidecar |"
    );
    let _ = writeln!(
        output,
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );
    for point in &bundle.storage_points {
        let _ = writeln!(
            output,
            "| {} | {} | {} | {} | {} | {} | {} | {:.3}s | {} | {} |",
            point.family,
            point.scale_label,
            format_count(point.datom_count),
            format_capacity_bytes(point.database_bytes),
            format_capacity_bytes(point.wal_bytes),
            format_capacity_bytes(point.shm_bytes),
            format_capacity_bytes(point.snapshot_bytes),
            point.restore_replay_seconds,
            format_capacity_bytes(point.peak_rss_bytes),
            point
                .sidecar_catalog_bytes
                .map(format_capacity_bytes)
                .unwrap_or_else(|| "-".into())
        );
    }

    output
}

pub fn load_perturbation_summary(
    path: impl AsRef<Path>,
) -> Result<PerfPerturbationSummary, ApiError> {
    let path = path.as_ref();
    serde_json::from_str(&fs::read_to_string(path).map_err(|source| {
        ApiError::Validation(format!(
            "failed to read perturbation summary {}: {source}",
            path.display()
        ))
    })?)
    .map_err(|source| {
        ApiError::Validation(format!(
            "failed to parse perturbation summary {}: {source}",
            path.display()
        ))
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfCapacityClassEnvelope {
    pub node_class: CapacityNodeClass,
    pub maximum_recommended_pilot_board_size: usize,
    pub maximum_recommended_mixed_operator_concurrency: usize,
    pub maximum_recommended_durable_replay_size: usize,
    pub projected_steady_state_storage_bytes: u64,
    pub projected_30_day_retained_journal_bytes: u64,
    pub projected_backup_restore_scratch_bytes: u64,
    pub snapshot_overhead_assumption: f64,
    pub limiting_factor: CapacityLimitingFactor,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfCapacityCeilingSignal {
    pub category: String,
    pub status: String,
    pub evidence: String,
    pub threshold: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfCapacityScaleOutTrigger {
    pub category: String,
    pub fired: bool,
    pub recommendation: String,
    pub evidence: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfCapacityProjectionBasis {
    pub perturbation_generated_at: String,
    pub matrix_generated_at: String,
    pub capacity_inputs_generated_at: String,
    pub calibration_host: PerfHostSnapshot,
    #[serde(default)]
    pub calibration_host_manifest: Option<PerfHostManifest>,
    pub calibration_node_class: CapacityNodeClass,
    pub compared_host_ids: Vec<String>,
    pub accepted_drift_statuses: Vec<PerfPerturbationDriftStatus>,
    pub assumptions: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfCapacityReport {
    pub generated_at: String,
    pub host_id: String,
    pub reference_workload: String,
    pub node_class: CapacityNodeClass,
    pub recommended_hardware: CapacityHardwareClass,
    pub current_limiting_factor: CapacityLimitingFactor,
    pub single_node_envelopes: Vec<PerfCapacityClassEnvelope>,
    pub ceiling_signals: Vec<PerfCapacityCeilingSignal>,
    pub scale_out_triggers: Vec<PerfCapacityScaleOutTrigger>,
    pub projection_basis: PerfCapacityProjectionBasis,
    pub confidence_level: CapacityConfidenceLevel,
    pub artifact_paths: PerfCapacityArtifactPaths,
    pub measured_curves: Vec<PerfCapacityCurve>,
    pub concurrency_pack: PerfCapacityConcurrencyPack,
    pub storage_points: Vec<PerfCapacityStoragePoint>,
}

pub fn build_capacity_report(
    perturbation: &PerfPerturbationSummary,
    matrix: &PerfMatrixReport,
    inputs: &PerfCapacityInputBundle,
    artifact_paths: PerfCapacityArtifactPaths,
) -> Result<PerfCapacityReport, ApiError> {
    let calibration_host = inputs.host_snapshot.clone();
    let calibration_node_class = assign_host_class(&calibration_host);
    let recommended_hardware = hardware_classes()
        .into_iter()
        .find(|class| class.node_class == CapacityNodeClass::M)
        .expect("M hardware class");

    let board_curve = find_curve(inputs, "pilot_board")?;
    let durable_replay_curve = find_curve(inputs, "durable_replay")?;
    let storage_points = &inputs.storage_points;
    let concurrency_pack = inputs.concurrency_pack.clone();

    let envelopes = hardware_classes()
        .into_iter()
        .map(|class| {
            let max_board =
                recommend_board_size(&class, &calibration_host, board_curve, storage_points);
            let max_concurrency =
                recommend_concurrency(&class, &calibration_host, &concurrency_pack);
            let max_replay =
                recommend_durable_replay(&class, &calibration_host, durable_replay_curve);
            let storage = project_storage_budget(max_board, storage_points);
            let closure_limit = closure_memory_threshold_status(&class, inputs);
            let limiting_factor =
                determine_limiting_factor(&class, max_board, max_concurrency, &closure_limit);

            PerfCapacityClassEnvelope {
                node_class: class.node_class,
                maximum_recommended_pilot_board_size: max_board,
                maximum_recommended_mixed_operator_concurrency: max_concurrency,
                maximum_recommended_durable_replay_size: max_replay,
                projected_steady_state_storage_bytes: storage.steady_state_bytes,
                projected_30_day_retained_journal_bytes: storage.journal_30_day_bytes,
                projected_backup_restore_scratch_bytes: storage.backup_restore_scratch_bytes,
                snapshot_overhead_assumption: SNAPSHOT_OVERHEAD_FACTOR,
                limiting_factor,
            }
        })
        .collect::<Vec<_>>();

    let current_envelope = envelopes
        .iter()
        .find(|envelope| envelope.node_class == CapacityNodeClass::M)
        .ok_or_else(|| ApiError::Validation("missing M class capacity envelope".into()))?;

    let ceiling_signals = build_ceiling_signals(inputs, current_envelope);
    let scale_out_triggers = build_scale_out_triggers(&ceiling_signals);
    let confidence_level = determine_confidence_level(
        calibration_node_class,
        &perturbation.performance.drift,
        matrix,
    );

    Ok(PerfCapacityReport {
        generated_at: timestamp_string(),
        host_id: perturbation.host_id.clone(),
        reference_workload: inputs.reference_workload.clone(),
        node_class: CapacityNodeClass::M,
        recommended_hardware,
        current_limiting_factor: current_envelope.limiting_factor,
        single_node_envelopes: envelopes,
        ceiling_signals,
        scale_out_triggers,
        projection_basis: PerfCapacityProjectionBasis {
            perturbation_generated_at: perturbation.generated_at.clone(),
            matrix_generated_at: matrix.generated_at.clone(),
            capacity_inputs_generated_at: inputs.generated_at.clone(),
            calibration_host,
            calibration_host_manifest: inputs.host_manifest.clone(),
            calibration_node_class,
            compared_host_ids: compared_host_ids(matrix),
            accepted_drift_statuses: perturbation.performance.drift.clone(),
            assumptions: vec![
                "The current pilot coordination surface is the primary customer-shaped workload; recursive closure remains a separate limiting curve.".into(),
                "Scale-up recommendations are conservative and capped at the largest measured ladder point rather than extrapolating optimistic upper bounds.".into(),
                "Closure headroom uses a 25% RAM budget to reserve space for the OS, allocator variance, burst load, and sidecars.".into(),
                "Storage planning assumes one board-equivalent of retained journal growth per day over 30 days plus 20% snapshot overhead.".into(),
            ],
        },
        confidence_level,
        artifact_paths,
        measured_curves: inputs.curves.clone(),
        concurrency_pack,
        storage_points: storage_points.clone(),
    })
}

pub fn render_markdown_capacity_report(report: &PerfCapacityReport) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "# AETHER Capacity Report");
    let _ = writeln!(output);
    let _ = writeln!(output, "- Generated at: `{}`", report.generated_at);
    let _ = writeln!(output, "- Host: `{}`", report.host_id);
    let _ = writeln!(
        output,
        "- Reference workload: `{}`",
        report.reference_workload
    );
    let _ = writeln!(
        output,
        "- Recommended default node class: `{}`",
        report.node_class
    );
    let _ = writeln!(
        output,
        "- Recommended hardware: `{}` vCPU / `{}` RAM / `{}` NVMe",
        report.recommended_hardware.vcpu,
        format_capacity_bytes(report.recommended_hardware.ram_bytes),
        format_capacity_bytes(report.recommended_hardware.nvme_bytes)
    );
    let _ = writeln!(
        output,
        "- Current limiting factor: `{}`",
        report.current_limiting_factor
    );
    let _ = writeln!(output, "- Confidence: `{}`", report.confidence_level);
    let _ = writeln!(output);

    let _ = writeln!(output, "## Single-Node Envelopes");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "| Class | Pilot board | Mixed concurrency | Durable replay | Storage | 30-day journal | Scratch | Limiting factor |"
    );
    let _ = writeln!(
        output,
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |"
    );
    for envelope in &report.single_node_envelopes {
        let _ = writeln!(
            output,
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
            envelope.node_class,
            format_count(envelope.maximum_recommended_pilot_board_size),
            format_count(envelope.maximum_recommended_mixed_operator_concurrency),
            format_count(envelope.maximum_recommended_durable_replay_size),
            format_capacity_bytes(envelope.projected_steady_state_storage_bytes),
            format_capacity_bytes(envelope.projected_30_day_retained_journal_bytes),
            format_capacity_bytes(envelope.projected_backup_restore_scratch_bytes),
            envelope.limiting_factor
        );
    }
    let _ = writeln!(output);

    let _ = writeln!(output, "## Ceiling Signals");
    let _ = writeln!(output);
    let _ = writeln!(output, "| Category | Status | Threshold | Evidence |");
    let _ = writeln!(output, "| --- | --- | --- | --- |");
    for signal in &report.ceiling_signals {
        let _ = writeln!(
            output,
            "| {} | {} | {} | {} |",
            signal.category, signal.status, signal.threshold, signal.evidence
        );
    }
    let _ = writeln!(output);

    let _ = writeln!(output, "## Scale-Out Triggers");
    let _ = writeln!(output);
    let _ = writeln!(output, "| Category | Fired | Recommendation | Evidence |");
    let _ = writeln!(output, "| --- | --- | --- | --- |");
    for trigger in &report.scale_out_triggers {
        let _ = writeln!(
            output,
            "| {} | {} | {} | {} |",
            trigger.category,
            if trigger.fired { "yes" } else { "no" },
            trigger.recommendation,
            trigger.evidence
        );
    }
    let _ = writeln!(output);

    let _ = writeln!(output, "## Calibration Basis");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "- Calibration host class: `{}`",
        report.projection_basis.calibration_node_class
    );
    let _ = writeln!(
        output,
        "- Matrix generated at: `{}` across `{}` host ids",
        report.projection_basis.matrix_generated_at,
        format_count(report.projection_basis.compared_host_ids.len())
    );
    let compared_hosts = if report.projection_basis.compared_host_ids.is_empty() {
        "-".to_string()
    } else {
        report.projection_basis.compared_host_ids.join("`, `")
    };
    let _ = writeln!(output, "- Compared hosts: `{}`", compared_hosts);
    let _ = writeln!(output);
    let _ = writeln!(output, "### Accepted Drift");
    let _ = writeln!(output);
    for drift in &report.projection_basis.accepted_drift_statuses {
        let _ = writeln!(output, "- `{}`: `{}`", drift.suite, drift.status);
    }
    let _ = writeln!(output);
    let _ = writeln!(output, "### Assumptions");
    let _ = writeln!(output);
    for assumption in &report.projection_basis.assumptions {
        let _ = writeln!(output, "- {}", assumption);
    }
    let _ = writeln!(output);

    let _ = writeln!(output, "## Artifact Paths");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "- Perturbation input: `{}`",
        report.artifact_paths.perturbation_json_path
    );
    let _ = writeln!(
        output,
        "- Matrix input: `{}`",
        report.artifact_paths.matrix_json_path
    );
    let _ = writeln!(
        output,
        "- Capacity curves input: `{}`",
        report.artifact_paths.capacity_input_json_path
    );

    output
}

pub fn load_capacity_report(path: impl AsRef<Path>) -> Result<PerfCapacityReport, ApiError> {
    let path = path.as_ref();
    serde_json::from_str(&fs::read_to_string(path).map_err(|source| {
        ApiError::Validation(format!(
            "failed to read capacity report {}: {source}",
            path.display()
        ))
    })?)
    .map_err(|source| {
        ApiError::Validation(format!(
            "failed to parse capacity report {}: {source}",
            path.display()
        ))
    })
}

pub fn write_capacity_report(
    report: &PerfCapacityReport,
    json_path: impl AsRef<Path>,
    markdown_path: impl AsRef<Path>,
) -> Result<(), ApiError> {
    let json_path = json_path.as_ref();
    let markdown_path = markdown_path.as_ref();
    if let Some(parent) = json_path.parent() {
        fs::create_dir_all(parent).map_err(|source| {
            ApiError::Validation(format!(
                "failed to create capacity json directory {}: {source}",
                parent.display()
            ))
        })?;
    }
    if let Some(parent) = markdown_path.parent() {
        fs::create_dir_all(parent).map_err(|source| {
            ApiError::Validation(format!(
                "failed to create capacity markdown directory {}: {source}",
                parent.display()
            ))
        })?;
    }
    fs::write(
        json_path,
        serde_json::to_string_pretty(report).map_err(|source| {
            ApiError::Validation(format!(
                "failed to serialize capacity report {}: {source}",
                json_path.display()
            ))
        })?,
    )
    .map_err(|source| {
        ApiError::Validation(format!(
            "failed to write capacity report json {}: {source}",
            json_path.display()
        ))
    })?;
    fs::write(markdown_path, render_markdown_capacity_report(report)).map_err(|source| {
        ApiError::Validation(format!(
            "failed to write capacity report markdown {}: {source}",
            markdown_path.display()
        ))
    })?;
    Ok(())
}

fn build_capacity_curve<F>(
    family: &str,
    label: &str,
    scales: &[usize],
    mut build_point: F,
) -> Result<PerfCapacityCurve, ApiError>
where
    F: FnMut(usize) -> Result<PerfCapacityCurvePoint, ApiError>,
{
    let mut points = Vec::new();
    for &scale in scales {
        points.push(build_point(scale)?);
    }
    Ok(PerfCapacityCurve {
        family: family.into(),
        label: label.into(),
        points,
    })
}

fn curve_point_from_measurement(
    scale_value: usize,
    measurement: &PerfMeasurement,
    estimated_bytes: Option<u64>,
) -> PerfCapacityCurvePoint {
    PerfCapacityCurvePoint {
        scale_label: measurement.scale.clone(),
        scale_value,
        units: measurement.units,
        unit_label: measurement.unit_label.clone(),
        mean_latency_ms: measurement.latency.mean.as_secs_f64() * 1000.0,
        throughput_per_second: measurement.throughput_per_second,
        estimated_bytes,
        metrics: measurement.metrics.clone(),
        notes: measurement.notes.clone(),
    }
}

fn build_mixed_operator_concurrency_pack() -> Result<PerfCapacityConcurrencyPack, ApiError> {
    let operations = [
        MixedOperation::Health,
        MixedOperation::Status,
        MixedOperation::History,
        MixedOperation::Report,
        MixedOperation::Delta,
        MixedOperation::Explain,
    ];
    let mut points = Vec::new();
    let mut prior_throughput = None;
    let mut first_saturation_point = None;

    for concurrency in CONCURRENCY_LADDER {
        let started = Instant::now();
        let mut handles = Vec::new();
        for worker in 0..concurrency {
            handles.push(thread::spawn(move || -> Result<Vec<f64>, ApiError> {
                let fixture = build_http_fixture()?;
                let mut latencies = Vec::with_capacity(DEFAULT_CONCURRENCY_OPERATIONS_PER_WORKER);
                for offset in 0..DEFAULT_CONCURRENCY_OPERATIONS_PER_WORKER {
                    let operation = operations[(worker + offset) % operations.len()];
                    let op_started = Instant::now();
                    run_mixed_operation(&fixture, operation)?;
                    latencies.push(op_started.elapsed().as_secs_f64() * 1000.0);
                }
                Ok(latencies)
            }));
        }

        let mut latencies = Vec::new();
        for handle in handles {
            let worker_latencies = handle.join().map_err(|_| {
                ApiError::Validation(
                    "mixed operator concurrency worker thread panicked unexpectedly".into(),
                )
            })??;
            latencies.extend(worker_latencies);
        }
        latencies.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));

        let total_operations = concurrency * DEFAULT_CONCURRENCY_OPERATIONS_PER_WORKER;
        let elapsed = started.elapsed().as_secs_f64().max(0.000_001);
        let throughput = total_operations as f64 / elapsed;
        let mean_latency_ms = latencies.iter().sum::<f64>() / latencies.len().max(1) as f64;
        let p95_latency_ms = percentile(&latencies, 0.95);
        let p99_latency_ms = percentile(&latencies, 0.99);

        if first_saturation_point.is_none() {
            if let Some(previous) = prior_throughput {
                if throughput < previous * 1.15 {
                    first_saturation_point = Some(concurrency);
                }
            }
        }
        prior_throughput = Some(throughput);

        points.push(PerfCapacityConcurrencyPoint {
            concurrency,
            total_operations,
            throughput_per_second: throughput,
            mean_latency_ms,
            p95_latency_ms,
            p99_latency_ms,
        });
    }

    Ok(PerfCapacityConcurrencyPack {
        label: "Mixed operator-service pilot surface".into(),
        operations_per_worker: DEFAULT_CONCURRENCY_OPERATIONS_PER_WORKER,
        operation_mix: operations
            .into_iter()
            .map(|operation| operation.label().into())
            .collect(),
        first_saturation_point,
        points,
    })
}

fn run_mixed_operation(fixture: &HttpFixture, operation: MixedOperation) -> Result<(), ApiError> {
    match operation {
        MixedOperation::Health => {
            let _: HealthResponse = http_get(fixture, "/health")?;
        }
        MixedOperation::Status => {
            let _: ServiceStatusResponse = http_get(fixture, "/v1/status")?;
        }
        MixedOperation::History => {
            let _: crate::HistoryResponse = http_get(fixture, "/v1/history")?;
        }
        MixedOperation::Report => {
            let _: crate::CoordinationPilotReport = http_post_json(
                fixture,
                "/v1/reports/pilot/coordination",
                &CoordinationPilotReportRequest {
                    policy_context: None,
                },
            )?;
        }
        MixedOperation::Delta => {
            let _: crate::CoordinationDeltaReport = http_post_json(
                fixture,
                "/v1/reports/pilot/coordination-delta",
                &CoordinationDeltaReportRequest {
                    left: CoordinationCut::AsOf {
                        element: ElementId::new(COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT),
                    },
                    right: CoordinationCut::Current,
                    policy_context: None,
                },
            )?;
        }
        MixedOperation::Explain => {
            let _: crate::ExplainTupleResponse = http_post_json(
                fixture,
                "/v1/explain/tuple",
                &ExplainTupleRequest {
                    tuple_id: fixture.explain_tuple_id,
                    policy_context: None,
                },
            )?;
        }
    }
    Ok(())
}

fn collect_storage_points() -> Result<Vec<PerfCapacityStoragePoint>, ApiError> {
    let mut points = Vec::new();
    for scale in REPLAY_ENTITY_LADDER {
        points.push(measure_durable_resolve_storage(scale)?);
    }
    for scale in REPLAY_BOARD_LADDER {
        points.push(measure_durable_coordination_storage(scale)?);
    }
    Ok(points)
}

fn measure_durable_resolve_storage(scale: usize) -> Result<PerfCapacityStoragePoint, ApiError> {
    let fixture = build_durable_resolve_fixture(scale)?;
    let database_path = fixture.database_path.clone();
    let (restore_replay_seconds, peak_rss_bytes) = measure_peak_rss(|| {
        let started = Instant::now();
        let service = SqliteKernelService::open(&database_path)?;
        let _ = service.current_state(CurrentStateRequest {
            schema: fixture.schema.clone(),
            datoms: Vec::new(),
            policy_context: None,
        })?;
        Ok(started.elapsed().as_secs_f64())
    })?;

    Ok(PerfCapacityStoragePoint {
        family: "durable_resolve".into(),
        scale_label: format!("{} entities", format_count(scale)),
        scale_value: scale,
        datom_count: fixture.datom_count,
        database_bytes: file_len(&database_path),
        wal_bytes: file_len(&wal_path_for(&database_path)),
        shm_bytes: file_len(&shm_path_for(&database_path)),
        snapshot_bytes: snapshot_bytes_for(&database_path)?,
        restore_replay_seconds,
        peak_rss_bytes,
        sidecar_catalog_bytes: sidecar_catalog_bytes(&database_path),
    })
}

fn measure_durable_coordination_storage(
    scale: usize,
) -> Result<PerfCapacityStoragePoint, ApiError> {
    let fixture = build_durable_coordination_replay_fixture(scale)?;
    let database_path = fixture.database_path.clone();
    let request = fixture.request.clone();
    let (restore_replay_seconds, peak_rss_bytes) = measure_peak_rss(|| {
        let started = Instant::now();
        let mut service = SqliteKernelService::open(&database_path)?;
        let _ = service.run_document(request.clone())?;
        Ok(started.elapsed().as_secs_f64())
    })?;

    Ok(PerfCapacityStoragePoint {
        family: "durable_coordination".into(),
        scale_label: format!("{} tasks", format_count(scale)),
        scale_value: scale,
        datom_count: fixture.datom_count,
        database_bytes: file_len(&database_path),
        wal_bytes: file_len(&wal_path_for(&database_path)),
        shm_bytes: file_len(&shm_path_for(&database_path)),
        snapshot_bytes: snapshot_bytes_for(&database_path)?,
        restore_replay_seconds,
        peak_rss_bytes,
        sidecar_catalog_bytes: sidecar_catalog_bytes(&database_path),
    })
}

fn measure_peak_rss<T, F>(operation: F) -> Result<(T, u64), ApiError>
where
    F: FnOnce() -> Result<T, ApiError>,
{
    let running = Arc::new(AtomicBool::new(true));
    let peak = Arc::new(AtomicU64::new(0));
    let monitor_running = Arc::clone(&running);
    let monitor_peak = Arc::clone(&peak);
    let pid = sysinfo::Pid::from_u32(std::process::id());
    let monitor = thread::spawn(move || {
        let mut system = System::new_all();
        while monitor_running.load(AtomicOrdering::Relaxed) {
            system.refresh_process(pid);
            if let Some(process) = system.process(pid) {
                monitor_peak.fetch_max(process.memory(), AtomicOrdering::Relaxed);
            }
            thread::sleep(Duration::from_millis(10));
        }
        system.refresh_process(pid);
        if let Some(process) = system.process(pid) {
            monitor_peak.fetch_max(process.memory(), AtomicOrdering::Relaxed);
        }
    });

    let result = operation();
    running.store(false, AtomicOrdering::Relaxed);
    let _ = monitor.join();

    Ok((result?, peak.load(AtomicOrdering::Relaxed)))
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn wal_path_for(database_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}-wal", database_path.display()))
}

fn shm_path_for(database_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}-shm", database_path.display()))
}

fn sidecar_catalog_bytes(database_path: &Path) -> Option<u64> {
    let catalog = crate::sidecar::sidecar_catalog_path_for_journal(database_path);
    if catalog.exists() {
        Some(file_len(&catalog))
    } else {
        None
    }
}

fn snapshot_bytes_for(database_path: &Path) -> Result<u64, ApiError> {
    let snapshot_root = unique_temp_dir("capacity-snapshot");
    fs::create_dir_all(&snapshot_root).map_err(|source| {
        ApiError::Validation(format!(
            "failed to create capacity snapshot directory {}: {source}",
            snapshot_root.display()
        ))
    })?;
    let mut total = 0u64;
    for source in [
        database_path.to_path_buf(),
        wal_path_for(database_path),
        shm_path_for(database_path),
    ] {
        if source.exists() {
            let destination = snapshot_root.join(
                source
                    .file_name()
                    .map(|name| name.to_os_string())
                    .unwrap_or_default(),
            );
            total = total.saturating_add(fs::copy(&source, &destination).map_err(|err| {
                ApiError::Validation(format!(
                    "failed to copy {} into {}: {err}",
                    source.display(),
                    destination.display()
                ))
            })?);
        }
    }
    let _ = fs::remove_dir_all(&snapshot_root);
    Ok(total)
}

fn hardware_classes() -> Vec<CapacityHardwareClass> {
    vec![
        CapacityHardwareClass {
            node_class: CapacityNodeClass::S,
            vcpu: 8,
            ram_bytes: 32 * 1024 * 1024 * 1024,
            nvme_bytes: 250 * 1024 * 1024 * 1024,
            target_p95_latency_ms: 2_500.0,
            target_replay_seconds: 90.0,
            notes: vec!["Entry design-partner node".into()],
        },
        CapacityHardwareClass {
            node_class: CapacityNodeClass::M,
            vcpu: 16,
            ram_bytes: 64 * 1024 * 1024 * 1024,
            nvme_bytes: 500 * 1024 * 1024 * 1024,
            target_p95_latency_ms: 2_000.0,
            target_replay_seconds: 60.0,
            notes: vec!["Default design-partner single-node recommendation".into()],
        },
        CapacityHardwareClass {
            node_class: CapacityNodeClass::L,
            vcpu: 32,
            ram_bytes: 128 * 1024 * 1024 * 1024,
            nvme_bytes: 1_000 * 1024 * 1024 * 1024,
            target_p95_latency_ms: 1_500.0,
            target_replay_seconds: 45.0,
            notes: vec!["Scale-up headroom for larger operator cells".into()],
        },
        CapacityHardwareClass {
            node_class: CapacityNodeClass::Xl,
            vcpu: 64,
            ram_bytes: 256 * 1024 * 1024 * 1024,
            nvme_bytes: 2_000 * 1024 * 1024 * 1024,
            target_p95_latency_ms: 1_000.0,
            target_replay_seconds: 30.0,
            notes: vec!["Temporary single-node ceiling, not the long-term strategy".into()],
        },
    ]
}

fn assign_host_class(host: &PerfHostSnapshot) -> CapacityNodeClass {
    let logical = host.logical_cores.unwrap_or(16) as i64;
    let memory = host.total_memory_bytes.unwrap_or(64 * 1024 * 1024 * 1024) as i128;
    hardware_classes()
        .into_iter()
        .min_by(|left, right| {
            let left_score = (left.vcpu as i64 - logical).abs() as i128
                + (left.ram_bytes as i128 - memory).abs();
            let right_score = (right.vcpu as i64 - logical).abs() as i128
                + (right.ram_bytes as i128 - memory).abs();
            left_score.cmp(&right_score)
        })
        .map(|class| class.node_class)
        .unwrap_or(CapacityNodeClass::M)
}

fn find_curve<'a>(
    inputs: &'a PerfCapacityInputBundle,
    family: &str,
) -> Result<&'a PerfCapacityCurve, ApiError> {
    inputs
        .curves
        .iter()
        .find(|curve| curve.family == family)
        .ok_or_else(|| ApiError::Validation(format!("missing capacity curve `{family}`")))
}

fn cpu_scale_for_class(class: &CapacityHardwareClass, calibration_host: &PerfHostSnapshot) -> f64 {
    let calibration_vcpu = calibration_host.logical_cores.unwrap_or(16).max(1) as f64;
    let raw_ratio = class.vcpu as f64 / calibration_vcpu;
    if raw_ratio >= 1.0 {
        1.0 + (raw_ratio - 1.0) * 0.75
    } else {
        raw_ratio
    }
}

fn recommend_board_size(
    class: &CapacityHardwareClass,
    calibration_host: &PerfHostSnapshot,
    board_curve: &PerfCapacityCurve,
    storage_points: &[PerfCapacityStoragePoint],
) -> usize {
    let cpu_scale = cpu_scale_for_class(class, calibration_host).max(0.25);
    let mut recommended = board_curve
        .points
        .first()
        .map(|point| point.scale_value)
        .unwrap_or(0);
    for point in &board_curve.points {
        let projected_p95_ms = point.mean_latency_ms * 1.75 / cpu_scale;
        let projected_replay_seconds =
            project_coordination_replay_seconds(point.scale_value, storage_points);
        let projected_steady_state =
            project_storage_budget(point.scale_value, storage_points).steady_state_bytes;
        if projected_p95_ms <= class.target_p95_latency_ms
            && projected_replay_seconds <= class.target_replay_seconds
            && projected_steady_state <= (class.nvme_bytes as f64 * 0.80) as u64
        {
            recommended = point.scale_value;
        }
    }
    recommended
}

fn recommend_concurrency(
    class: &CapacityHardwareClass,
    calibration_host: &PerfHostSnapshot,
    pack: &PerfCapacityConcurrencyPack,
) -> usize {
    let cpu_scale = cpu_scale_for_class(class, calibration_host).max(0.25);
    let saturation_cap = pack.first_saturation_point.unwrap_or(usize::MAX);
    let mut recommended = pack
        .points
        .first()
        .map(|point| point.concurrency)
        .unwrap_or(0);
    for point in &pack.points {
        let projected_p95_ms = point.p95_latency_ms / cpu_scale;
        if projected_p95_ms <= class.target_p95_latency_ms && point.concurrency <= saturation_cap {
            recommended = point.concurrency;
        }
    }
    recommended
}

fn recommend_durable_replay(
    class: &CapacityHardwareClass,
    calibration_host: &PerfHostSnapshot,
    durable_replay_curve: &PerfCapacityCurve,
) -> usize {
    let cpu_scale = cpu_scale_for_class(class, calibration_host).max(0.25);
    let mut recommended = durable_replay_curve
        .points
        .first()
        .map(|point| point.scale_value)
        .unwrap_or(0);
    for point in &durable_replay_curve.points {
        let projected_replay_seconds = (point.mean_latency_ms / 1000.0) / cpu_scale;
        if projected_replay_seconds <= class.target_replay_seconds {
            recommended = point.scale_value;
        }
    }
    recommended
}

struct StorageBudget {
    steady_state_bytes: u64,
    journal_30_day_bytes: u64,
    backup_restore_scratch_bytes: u64,
}

fn project_storage_budget(
    board_size: usize,
    storage_points: &[PerfCapacityStoragePoint],
) -> StorageBudget {
    let source = storage_points
        .iter()
        .filter(|point| point.family == "durable_coordination")
        .max_by_key(|point| point.scale_value);
    let per_task_bytes = source
        .map(|point| {
            let denom = point.scale_value.max(1) as f64;
            (
                point.database_bytes as f64 / denom,
                point.wal_bytes as f64 / denom,
                point.shm_bytes as f64 / denom,
                point.sidecar_catalog_bytes.unwrap_or(0) as f64 / denom,
            )
        })
        .unwrap_or((0.0, 0.0, 0.0, 0.0));
    let steady_state_bytes =
        ((per_task_bytes.0 + per_task_bytes.1 + per_task_bytes.2 + per_task_bytes.3)
            * board_size as f64
            * SNAPSHOT_OVERHEAD_FACTOR)
            .round() as u64;
    StorageBudget {
        steady_state_bytes,
        journal_30_day_bytes: steady_state_bytes.saturating_mul(30),
        backup_restore_scratch_bytes: steady_state_bytes.saturating_mul(2),
    }
}

fn project_coordination_replay_seconds(
    board_size: usize,
    storage_points: &[PerfCapacityStoragePoint],
) -> f64 {
    let source = storage_points
        .iter()
        .filter(|point| point.family == "durable_coordination")
        .max_by_key(|point| point.scale_value);
    source
        .map(|point| {
            let per_task = point.restore_replay_seconds / point.scale_value.max(1) as f64;
            per_task * board_size as f64
        })
        .unwrap_or(0.0)
}

struct ClosureThresholdStatus {
    status: String,
    evidence: String,
    threshold: String,
}

fn closure_memory_threshold_status(
    class: &CapacityHardwareClass,
    inputs: &PerfCapacityInputBundle,
) -> ClosureThresholdStatus {
    let budget_bytes = class.ram_bytes / 4;
    let closure_curve = find_curve(inputs, "closure").expect("closure curve");
    let mut status = "clear".to_string();
    let mut evidence = format!(
        "measured closure ladder stays within {} of RAM through chain {}",
        format_capacity_bytes(budget_bytes),
        closure_curve
            .points
            .last()
            .map(|point| format_count(point.scale_value))
            .unwrap_or_else(|| "0".into())
    );
    if let Some(point) = closure_curve
        .points
        .iter()
        .find(|point| point.estimated_bytes.unwrap_or(0) > budget_bytes)
    {
        status = "fired".into();
        evidence = format!(
            "measured closure ladder exceeds the 25% RAM budget at {} with {}",
            point.scale_label,
            point
                .estimated_bytes
                .map(format_capacity_bytes)
                .unwrap_or_else(|| "-".into())
        );
    } else if let Some(point) = closure_curve
        .points
        .iter()
        .max_by_key(|point| point.scale_value)
    {
        let projected_bytes = point
            .estimated_bytes
            .unwrap_or(0)
            .saturating_mul(4)
            .saturating_add(point.estimated_bytes.unwrap_or(0) / 2);
        if projected_bytes > budget_bytes {
            status = "near".into();
            evidence = format!(
                "the next closure rung beyond {} is projected to cross the 25% RAM budget ({})",
                point.scale_label,
                format_capacity_bytes(budget_bytes)
            );
        }
    }
    ClosureThresholdStatus {
        status,
        evidence,
        threshold: format!("25% RAM = {}", format_capacity_bytes(budget_bytes)),
    }
}

fn determine_limiting_factor(
    class: &CapacityHardwareClass,
    max_board: usize,
    max_concurrency: usize,
    closure_limit: &ClosureThresholdStatus,
) -> CapacityLimitingFactor {
    if closure_limit.status == "near" || closure_limit.status == "fired" {
        return CapacityLimitingFactor::Memory;
    }
    if max_board < *BOARD_LADDER.last().unwrap_or(&max_board) {
        return CapacityLimitingFactor::ReportLatency;
    }
    if max_concurrency < *CONCURRENCY_LADDER.last().unwrap_or(&max_concurrency) {
        return CapacityLimitingFactor::Cpu;
    }
    if class.target_replay_seconds <= 45.0 {
        CapacityLimitingFactor::ReplayWindow
    } else {
        CapacityLimitingFactor::ReportLatency
    }
}

fn build_ceiling_signals(
    inputs: &PerfCapacityInputBundle,
    current_envelope: &PerfCapacityClassEnvelope,
) -> Vec<PerfCapacityCeilingSignal> {
    let current_class = hardware_classes()
        .into_iter()
        .find(|class| class.node_class == current_envelope.node_class)
        .expect("current hardware class");
    let closure_status = closure_memory_threshold_status(&current_class, inputs);
    let board_status = if current_envelope.maximum_recommended_pilot_board_size
        < *BOARD_LADDER.last().unwrap_or(&0)
    {
        PerfCapacityCeilingSignal {
            category: "board ceiling".into(),
            status: "near".into(),
            evidence: format!(
                "mixed operator and coordination latency guidance caps the board at {} tasks on {}",
                format_count(current_envelope.maximum_recommended_pilot_board_size),
                current_envelope.node_class
            ),
            threshold: format!(
                "p95 operator/report latency under {:.1} ms",
                current_class.target_p95_latency_ms
            ),
        }
    } else {
        PerfCapacityCeilingSignal {
            category: "board ceiling".into(),
            status: "clear".into(),
            evidence: format!(
                "the measured board ladder remained inside latency and replay targets through {} tasks",
                format_count(current_envelope.maximum_recommended_pilot_board_size)
            ),
            threshold: format!(
                "p95 operator/report latency under {:.1} ms",
                current_class.target_p95_latency_ms
            ),
        }
    };
    let replay_status = if current_envelope.maximum_recommended_durable_replay_size
        < *REPLAY_ENTITY_LADDER.last().unwrap_or(&0)
    {
        PerfCapacityCeilingSignal {
            category: "replay ceiling".into(),
            status: "near".into(),
            evidence: format!(
                "restart/replay guidance caps current replay at {} entities on {}",
                format_count(current_envelope.maximum_recommended_durable_replay_size),
                current_envelope.node_class
            ),
            threshold: format!(
                "restart/replay under {:.1}s",
                current_class.target_replay_seconds
            ),
        }
    } else {
        PerfCapacityCeilingSignal {
            category: "replay ceiling".into(),
            status: "clear".into(),
            evidence: format!(
                "the measured replay ladder stayed inside the {} recovery target",
                current_envelope.node_class
            ),
            threshold: format!(
                "restart/replay under {:.1}s",
                current_class.target_replay_seconds
            ),
        }
    };
    let storage_ratio = current_envelope.projected_steady_state_storage_bytes as f64
        / current_class.nvme_bytes as f64;
    let storage_status = if storage_ratio >= 0.80 {
        (
            "fired",
            "steady-state storage is already above the 80% NVMe budget",
        )
    } else if storage_ratio >= 0.50 {
        (
            "near",
            "steady-state storage is above the 50% NVMe planning line",
        )
    } else {
        (
            "clear",
            "steady-state storage remains well below the NVMe planning line",
        )
    };

    vec![
        PerfCapacityCeilingSignal {
            category: "closure ceiling".into(),
            status: closure_status.status,
            evidence: closure_status.evidence,
            threshold: closure_status.threshold,
        },
        board_status,
        replay_status,
        PerfCapacityCeilingSignal {
            category: "storage ceiling".into(),
            status: storage_status.0.into(),
            evidence: storage_status.1.into(),
            threshold: "steady-state storage under 80% of local NVMe".into(),
        },
    ]
}

fn build_scale_out_triggers(
    ceiling_signals: &[PerfCapacityCeilingSignal],
) -> Vec<PerfCapacityScaleOutTrigger> {
    ceiling_signals
        .iter()
        .map(|signal| {
            let fired = signal.status == "fired";
            let recommendation = match signal.category.as_str() {
                "replay ceiling" => {
                    "Add snapshot/checkpoint acceleration before pushing a larger single-node replay window."
                }
                "closure ceiling" | "board ceiling" => {
                    "Partition by workspace, incident domain, or tenant before widening into a larger operator cell."
                }
                "storage ceiling" => {
                    "Move to explicit checkpoint and archival rotation, then federate bounded operator cells instead of chasing one giant node."
                }
                _ => "Federate across explicit cuts once one-node operator cells are well bounded.",
            };
            PerfCapacityScaleOutTrigger {
                category: signal.category.clone(),
                fired,
                recommendation: recommendation.into(),
                evidence: signal.evidence.clone(),
            }
        })
        .collect()
}

fn determine_confidence_level(
    calibration_node_class: CapacityNodeClass,
    drifts: &[PerfPerturbationDriftStatus],
    matrix: &PerfMatrixReport,
) -> CapacityConfidenceLevel {
    let all_ok = drifts.iter().all(|drift| drift.status == "ok");
    let any_fail = drifts.iter().any(|drift| drift.status == "fail");
    let host_count = compared_host_ids(matrix).len();
    if calibration_node_class == CapacityNodeClass::M && all_ok && host_count >= 2 {
        CapacityConfidenceLevel::High
    } else if !any_fail && host_count >= 1 {
        CapacityConfidenceLevel::Medium
    } else {
        CapacityConfidenceLevel::Low
    }
}

fn compared_host_ids(matrix: &PerfMatrixReport) -> Vec<String> {
    let mut host_ids = BTreeMap::<String, ()>::new();
    for row in &matrix.rows {
        for cell in &row.cells {
            host_ids.insert(cell.host_id.clone(), ());
        }
    }
    host_ids.into_keys().collect()
}

fn percentile(values: &[f64], fraction: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let rank = ((values.len() - 1) as f64 * fraction).round() as usize;
    values[rank.min(values.len() - 1)]
}

fn format_capacity_bytes(bytes: u64) -> String {
    if bytes >= 1024_u64.pow(3) {
        format!("{:.2} GiB", bytes as f64 / 1024_f64.powi(3))
    } else if bytes >= 1024_u64.pow(2) {
        format!("{:.2} MiB", bytes as f64 / 1024_f64.powi(2))
    } else if bytes >= 1024 {
        format!("{:.2} KiB", bytes as f64 / 1024_f64)
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_host(logical_cores: usize, ram_gb: u64) -> PerfHostSnapshot {
        PerfHostSnapshot {
            hostname: "test".into(),
            os: "windows".into(),
            arch: "x86_64".into(),
            cpu_brand: "test".into(),
            physical_cores: Some(logical_cores / 2),
            logical_cores: Some(logical_cores),
            total_memory_bytes: Some(ram_gb * 1024 * 1024 * 1024),
            execution_environment: PerfExecutionEnvironment::NativeWindows,
        }
    }

    #[test]
    fn host_assignment_maps_canonical_m_shape_to_m() {
        assert_eq!(assign_host_class(&fake_host(16, 64)), CapacityNodeClass::M);
    }

    #[test]
    fn storage_budget_scales_linearly_from_largest_measured_board() {
        let budget = project_storage_budget(
            8_192,
            &[PerfCapacityStoragePoint {
                family: "durable_coordination".into(),
                scale_label: "4,096 tasks".into(),
                scale_value: 4_096,
                datom_count: 20_000,
                database_bytes: 4_096,
                wal_bytes: 2_048,
                shm_bytes: 1_024,
                snapshot_bytes: 0,
                restore_replay_seconds: 2.0,
                peak_rss_bytes: 0,
                sidecar_catalog_bytes: Some(512),
            }],
        );
        assert!(budget.steady_state_bytes > 0);
        assert_eq!(budget.journal_30_day_bytes, budget.steady_state_bytes * 30);
    }

    #[test]
    fn scale_out_triggers_match_ceiling_categories() {
        let triggers = build_scale_out_triggers(&[PerfCapacityCeilingSignal {
            category: "closure ceiling".into(),
            status: "fired".into(),
            evidence: "memory budget exceeded".into(),
            threshold: "25% RAM".into(),
        }]);
        assert_eq!(triggers[0].category, "closure ceiling");
        assert!(triggers[0].fired);
        assert!(triggers[0].recommendation.contains("Partition"));
    }
}
