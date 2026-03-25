use crate::{
    ApiError, AppendRequest, CurrentStateRequest, InMemoryKernelService, KernelService,
    RunDocumentRequest, SqliteKernelService,
};
use aether_ast::{
    Atom, AttributeId, Datom, DatomProvenance, DerivationTrace, ElementId, EntityId, Literal,
    OperationKind, PredicateId, PredicateRef, ReplicaId, RuleAst, RuleId, RuleProgram, Term,
    TupleId, Value, Variable,
};
use aether_explain::{Explainer, InMemoryExplainer};
use aether_plan::CompiledProgram;
use aether_resolver::{MaterializedResolver, ResolvedState, Resolver};
use aether_rules::{DefaultRuleCompiler, RuleCompiler};
use aether_runtime::{DerivedSet, RuleRuntime, RuntimeIteration, SemiNaiveRuntime};
use aether_schema::{AttributeClass, AttributeSchema, PredicateSignature, Schema, ValueType};
use aether_storage::{InMemoryJournal, Journal};
use serde::{Deserialize, Serialize};
use std::hint::black_box;
use std::mem::size_of;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{fmt::Write as _, fs, path::PathBuf};

pub const DEFAULT_REPORT_SAMPLES: usize = 5;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LatencyStats {
    pub samples: usize,
    pub mean: Duration,
    pub min: Duration,
    pub max: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerfMeasurement {
    pub workload: String,
    pub scale: String,
    pub units: usize,
    pub unit_label: String,
    pub latency: LatencyStats,
    pub throughput_per_second: f64,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FootprintEstimate {
    pub workload: String,
    pub scale: String,
    pub estimated_bytes: usize,
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
    pub budgets: PerfDriftBudget,
    pub measurements: Vec<PerfMeasurementDrift>,
    pub footprints: Vec<PerfFootprintDrift>,
    pub overall: DriftSeverity,
}

#[derive(Clone, Debug)]
pub enum PerfEvent {
    SuiteStart {
        total_workloads: usize,
        samples_per_workload: usize,
    },
    MeasurementStart {
        workload: &'static str,
        scale: String,
        total_samples: usize,
        units: usize,
        unit_label: &'static str,
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
}

struct MeasurementPlan {
    workload: &'static str,
    scale: String,
    units: usize,
    unit_label: &'static str,
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
    service.append(AppendRequest {
        datoms: coordination_datoms(task_count),
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
            workload: "Journal append throughput",
            scale: format!("{} datoms", format_count(fixture.datoms.len())),
            units: fixture.datoms.len(),
            unit_label: "datoms/s",
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
    let notes = vec![format!(
        "{} datoms across scalar, set, and ref attributes",
        format_count(fixture.datoms.len())
    )];

    benchmark_measurement(
        MeasurementPlan {
            workload: "Resolver current throughput",
            scale: format!("{} entities", format_count(entity_count)),
            units: entity_count,
            unit_label: "entities/s",
            notes,
            samples,
            iterations_per_sample: 1,
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
    let notes = vec![format!(
        "Inclusive prefix cut at {} across {} datoms",
        fixture.as_of,
        format_count(fixture.datoms.len())
    )];

    benchmark_measurement(
        MeasurementPlan {
            workload: "Resolver as-of throughput",
            scale: format!("{} entities", format_count(entity_count)),
            units: entity_count,
            unit_label: "entities/s",
            notes,
            samples,
            iterations_per_sample: 1,
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
            workload: "Compiler SCC planning",
            scale: format!("recursive width {}", format_count(scc_width)),
            units: scc_width,
            unit_label: "predicates/s",
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
            workload: "Recursive closure runtime",
            scale: format!("chain {}", format_count(chain_len)),
            units: fixture.expected_tuple_count,
            unit_label: "tuples/s",
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
    ];

    benchmark_measurement(
        MeasurementPlan {
            workload: "Tuple explanation runtime",
            scale: format!("chain {}", format_count(chain_len)),
            units: trace.tuples.len(),
            unit_label: "trace-tuples/s",
            notes,
            samples,
            iterations_per_sample: 1,
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
            workload: "Kernel service coordination run",
            scale: format!("{} tasks", format_count(task_count)),
            units: fixture.expected_row_count.max(1),
            unit_label: "rows/s",
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
            workload: "Durable restart current replay",
            scale: format!("{} entities", format_count(entity_count)),
            units: fixture.entity_count,
            unit_label: "entities/s",
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
            workload: "Durable restart coordination replay",
            scale: format!("{} tasks", format_count(task_count)),
            units: fixture.expected_row_count.max(1),
            unit_label: "rows/s",
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
        workload: "Derived-set footprint estimate".into(),
        scale: format!("chain {}", format_count(chain_len)),
        estimated_bytes: estimate_derived_set_bytes(&derived),
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
        workload: "Derivation-trace footprint estimate".into(),
        scale: format!("chain {}", format_count(chain_len)),
        estimated_bytes: estimate_derivation_trace_bytes(&trace),
        notes: vec![
            format!(
                "{} tuples in the reconstructed proof graph",
                format_count(trace.tuples.len())
            ),
            "structural lower-bound estimate for regression tracking".into(),
        ],
    })
}

pub fn default_performance_report() -> Result<PerfReport, ApiError> {
    default_performance_report_impl(None)
}

pub fn default_performance_report_with_events<F>(mut observer: F) -> Result<PerfReport, ApiError>
where
    F: FnMut(PerfEvent),
{
    default_performance_report_impl(Some(&mut observer))
}

fn default_performance_report_impl(
    mut observer: Option<&mut dyn FnMut(PerfEvent)>,
) -> Result<PerfReport, ApiError> {
    let samples = DEFAULT_REPORT_SAMPLES;
    emit_event(
        &mut observer,
        PerfEvent::SuiteStart {
            total_workloads: 12,
            samples_per_workload: samples,
        },
    );
    let measurements = vec![
        benchmark_append_impl(10_000, samples, &mut observer)?,
        benchmark_append_impl(50_000, samples, &mut observer)?,
        benchmark_resolve_current_impl(1_000, samples, &mut observer)?,
        benchmark_resolve_as_of_impl(1_000, samples, &mut observer)?,
        benchmark_durable_restart_current_impl(1_000, samples, &mut observer)?,
        benchmark_compile_scc_impl(16, samples, &mut observer)?,
        benchmark_compile_scc_impl(64, samples, &mut observer)?,
        benchmark_runtime_closure_impl(64, samples, &mut observer)?,
        benchmark_runtime_closure_impl(128, samples, &mut observer)?,
        benchmark_explain_trace_impl(128, samples, &mut observer)?,
        benchmark_service_coordination_impl(128, samples, &mut observer)?,
        benchmark_durable_restart_coordination_impl(128, samples, &mut observer)?,
    ];
    let footprints = vec![
        estimate_runtime_footprint(128)?,
        estimate_trace_footprint(128)?,
    ];
    for footprint in &footprints {
        emit_event(
            &mut observer,
            PerfEvent::FootprintComputed {
                footprint: footprint.clone(),
            },
        );
    }

    Ok(PerfReport {
        samples_per_workload: samples,
        measurements,
        footprints,
    })
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
    let _ = writeln!(output, "## Timed Workloads");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "| Workload | Scale | Mean | Min | Max | Throughput | Notes |"
    );
    let _ = writeln!(output, "| --- | --- | ---: | ---: | ---: | ---: | --- |");
    for measurement in &report.measurements {
        let notes = measurement.notes.join("<br>");
        let _ = writeln!(
            output,
            "| {} | {} | {} | {} | {} | {}/{} | {} |",
            measurement.workload,
            measurement.scale,
            format_duration(measurement.latency.mean),
            format_duration(measurement.latency.min),
            format_duration(measurement.latency.max),
            format_rate(measurement.throughput_per_second),
            measurement.unit_label,
            notes
        );
    }
    let _ = writeln!(output);
    let _ = writeln!(output, "## Footprint Estimates");
    let _ = writeln!(output);
    let _ = writeln!(output, "| Workload | Scale | Estimated bytes | Notes |");
    let _ = writeln!(output, "| --- | --- | ---: | --- |");
    for footprint in &report.footprints {
        let _ = writeln!(
            output,
            "| {} | {} | {} | {} |",
            footprint.workload,
            footprint.scale,
            format_count(footprint.estimated_bytes),
            footprint.notes.join("<br>")
        );
    }
    let _ = writeln!(output);
    let _ = writeln!(output, "## Interpretation");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "- These are local single-node baselines intended for regression tracking and operator planning."
    );
    let _ = writeln!(
        output,
        "- The end-to-end service number includes parsing, compilation, resolution, runtime evaluation, and query execution."
    );
    let _ = writeln!(
        output,
        "- Durable restart/replay timings include reopening the SQLite-backed kernel and replaying committed history before answering."
    );
    let _ = writeln!(
        output,
        "- Footprint figures are stable structural estimates, not allocator-exact memory telemetry."
    );

    output
}

pub fn compare_perf_reports(
    current: &PerfReport,
    baseline: &PerfBaseline,
    budgets: &PerfDriftBudget,
    generated_at: impl Into<String>,
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
        .collect::<std::collections::HashMap<_, _>>();

    let mut overall = DriftSeverity::Ok;
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
            overall = overall.merge(severity);

            PerfMeasurementDrift {
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
            overall = overall.merge(severity);

            PerfFootprintDrift {
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
        generated_at: generated_at.into(),
        budgets: budgets.clone(),
        measurements,
        footprints,
        overall,
    }
}

pub fn render_markdown_drift_report(report: &PerfDriftReport) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "# AETHER Performance Drift Report");
    let _ = writeln!(output);
    let _ = writeln!(output, "- Generated at: `{}`", report.generated_at);
    let _ = writeln!(output, "- Baseline: `{}`", report.baseline_label);
    let _ = writeln!(output, "- Overall: `{}`", format_severity(report.overall));
    let _ = writeln!(output);

    let _ = writeln!(output, "## Throughput Drift");
    let _ = writeln!(output);
    let _ = writeln!(
        output,
        "| Workload | Scale | Baseline | Current | Delta | Severity |"
    );
    let _ = writeln!(output, "| --- | --- | ---: | ---: | ---: | --- |");
    for measurement in &report.measurements {
        let _ = writeln!(
            output,
            "| {} | {} | {} | {} | {} | {} |",
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
        "| Workload | Scale | Baseline bytes | Current bytes | Delta | Severity |"
    );
    let _ = writeln!(output, "| --- | --- | ---: | ---: | ---: | --- |");
    for footprint in &report.footprints {
        let _ = writeln!(
            output,
            "| {} | {} | {} | {} | {} | {} |",
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
            workload: plan.workload,
            scale: plan.scale.clone(),
            total_samples: samples,
            units: effective_units,
            unit_label: plan.unit_label,
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
        compare_perf_reports, DriftSeverity, FootprintEstimate, LatencyStats, PerfBaseline,
        PerfDriftBudget, PerfMeasurement, PerfReport,
    };
    use std::time::Duration;

    #[test]
    fn drift_report_flags_throughput_regressions_and_footprint_growth() {
        let baseline = PerfBaseline {
            label: "pilot-baseline".into(),
            generated_at: "2026-03-19 00:00:00".into(),
            report: PerfReport {
                samples_per_workload: 5,
                measurements: vec![PerfMeasurement {
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
                    notes: Vec::new(),
                }],
                footprints: vec![FootprintEstimate {
                    workload: "Derived-set footprint estimate".into(),
                    scale: "chain 128".into(),
                    estimated_bytes: 1_000,
                    notes: Vec::new(),
                }],
            },
        };
        let current = PerfReport {
            samples_per_workload: 5,
            measurements: vec![PerfMeasurement {
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
                notes: Vec::new(),
            }],
            footprints: vec![FootprintEstimate {
                workload: "Derived-set footprint estimate".into(),
                scale: "chain 128".into(),
                estimated_bytes: 1_250,
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
            report: PerfReport {
                samples_per_workload: 5,
                measurements: Vec::new(),
                footprints: Vec::new(),
            },
        };
        let current = PerfReport {
            samples_per_workload: 5,
            measurements: vec![PerfMeasurement {
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
}
