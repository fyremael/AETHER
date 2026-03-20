use crate::{ApiError, AppendRequest, InMemoryKernelService, KernelService, RunDocumentRequest};
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
use std::fmt::Write as _;
use std::hint::black_box;
use std::mem::size_of;
use std::time::{Duration, Instant};

pub const DEFAULT_REPORT_SAMPLES: usize = 5;

#[derive(Clone, Debug)]
pub struct LatencyStats {
    pub samples: usize,
    pub mean: Duration,
    pub min: Duration,
    pub max: Duration,
}

#[derive(Clone, Debug)]
pub struct PerfMeasurement {
    pub workload: &'static str,
    pub scale: String,
    pub units: usize,
    pub unit_label: &'static str,
    pub latency: LatencyStats,
    pub throughput_per_second: f64,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct FootprintEstimate {
    pub workload: &'static str,
    pub scale: String,
    pub estimated_bytes: usize,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct PerfReport {
    pub samples_per_workload: usize,
    pub measurements: Vec<PerfMeasurement>,
    pub footprints: Vec<FootprintEstimate>,
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

struct MeasurementPlan {
    workload: &'static str,
    scale: String,
    units: usize,
    unit_label: &'static str,
    notes: Vec<String>,
    samples: usize,
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
        },
        expected_row_count: ready_tasks * 2,
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
    let notes = vec![format!(
        "{} predicates and {} rules with one large recursive SCC",
        format_count(fixture.program.predicates.len()),
        format_count(fixture.program.rules.len())
    )];

    benchmark_measurement(
        MeasurementPlan {
            workload: "Compiler SCC planning",
            scale: format!("recursive width {}", format_count(scc_width)),
            units: scc_width,
            unit_label: "predicates/s",
            notes,
            samples,
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
        },
        observer,
        move || fixture.service.run_document(fixture.request.clone()),
    )
}

pub fn estimate_runtime_footprint(chain_len: usize) -> Result<FootprintEstimate, ApiError> {
    let fixture = build_runtime_fixture(chain_len)?;
    let derived = SemiNaiveRuntime.evaluate(&fixture.state, &fixture.program)?;

    Ok(FootprintEstimate {
        workload: "Derived-set footprint estimate",
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
        workload: "Derivation-trace footprint estimate",
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
            total_workloads: 10,
            samples_per_workload: samples,
        },
    );
    let measurements = vec![
        benchmark_append_impl(10_000, samples, &mut observer)?,
        benchmark_append_impl(50_000, samples, &mut observer)?,
        benchmark_resolve_current_impl(1_000, samples, &mut observer)?,
        benchmark_resolve_as_of_impl(1_000, samples, &mut observer)?,
        benchmark_compile_scc_impl(16, samples, &mut observer)?,
        benchmark_compile_scc_impl(64, samples, &mut observer)?,
        benchmark_runtime_closure_impl(64, samples, &mut observer)?,
        benchmark_runtime_closure_impl(128, samples, &mut observer)?,
        benchmark_explain_trace_impl(128, samples, &mut observer)?,
        benchmark_service_coordination_impl(128, samples, &mut observer)?,
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
        "- Footprint figures are stable structural estimates, not allocator-exact memory telemetry."
    );

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
    let mut durations = Vec::with_capacity(samples);
    emit_event(
        observer,
        PerfEvent::MeasurementStart {
            workload: plan.workload,
            scale: plan.scale.clone(),
            total_samples: samples,
            units: plan.units,
            unit_label: plan.unit_label,
            notes: plan.notes.clone(),
        },
    );

    let mut total = Duration::default();
    let mut min = Duration::default();
    let mut max = Duration::default();
    for _ in 0..samples {
        let started = Instant::now();
        let result = operation()?;
        black_box(result);
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
            plan.units as f64 / elapsed.as_secs_f64()
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
        plan.units as f64 / mean.as_secs_f64()
    };

    let measurement = PerfMeasurement {
        workload: plan.workload,
        scale: plan.scale,
        units: plan.units,
        unit_label: plan.unit_label,
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

fn emit_event(observer: &mut Option<&mut dyn FnMut(PerfEvent)>, event: PerfEvent) {
    if let Some(observer) = observer.as_deref_mut() {
        observer(event);
    }
}
