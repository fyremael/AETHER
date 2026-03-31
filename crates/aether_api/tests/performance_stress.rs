use aether_api::{perf, CurrentStateRequest, KernelService, SqliteKernelService};
use aether_explain::{Explainer, InMemoryExplainer};
use aether_runtime::{RuleRuntime, SemiNaiveRuntime};
use std::time::Instant;

fn run_runtime_stress(chain_len: usize) {
    let fixture = perf::build_runtime_fixture(chain_len).expect("build runtime stress fixture");
    let started = Instant::now();
    let derived = SemiNaiveRuntime
        .evaluate(&fixture.state, &fixture.program)
        .expect("evaluate recursive closure");
    let elapsed = started.elapsed();

    assert_eq!(derived.tuples.len(), fixture.expected_tuple_count);
    assert!(derived.has_converged());

    eprintln!(
        "runtime stress: chain={} tuples={} elapsed={} estimated_bytes={}",
        fixture.chain_len,
        derived.tuples.len(),
        elapsed.as_secs_f64(),
        perf::estimate_derived_set_bytes(&derived)
    );
}

fn run_explain_stress(chain_len: usize) {
    let fixture = perf::build_explain_fixture(chain_len).expect("build explain stress fixture");
    let started = Instant::now();
    let trace = InMemoryExplainer::from_derived_set(&fixture.derived)
        .explain_tuple(&fixture.tuple_id)
        .expect("explain deep trace");
    let elapsed = started.elapsed();

    assert_eq!(trace.root, fixture.tuple_id);
    assert!(
        trace
            .tuples
            .first()
            .map(|tuple| tuple.metadata.source_datom_ids.len())
            .unwrap_or_default()
            >= fixture.chain_len - 1
    );

    eprintln!(
        "explain stress: chain={} trace_tuples={} elapsed={} estimated_bytes={}",
        fixture.chain_len,
        trace.tuples.len(),
        elapsed.as_secs_f64(),
        perf::estimate_derivation_trace_bytes(&trace)
    );
}

fn run_service_stress(task_count: usize) {
    let mut fixture =
        perf::build_coordination_service_fixture(task_count).expect("build service stress fixture");
    let started = Instant::now();
    let response = fixture
        .service
        .run_document(fixture.request.clone())
        .expect("run coordination document");
    let elapsed = started.elapsed();
    let rows = response.query.expect("query result").rows.len();

    assert_eq!(rows, fixture.expected_row_count);

    eprintln!(
        "service stress: tasks={} rows={} elapsed={}",
        fixture.task_count,
        rows,
        elapsed.as_secs_f64()
    );
}

fn run_durable_resolve_stress(entity_count: usize) {
    let fixture =
        perf::build_durable_resolve_fixture(entity_count).expect("build durable resolve fixture");
    let started = Instant::now();
    let service = SqliteKernelService::open(&fixture.database_path).expect("open durable service");
    let response = service
        .current_state(CurrentStateRequest {
            schema: fixture.schema.clone(),
            datoms: Vec::new(),
            policy_context: None,
        })
        .expect("resolve current state after restart");
    let elapsed = started.elapsed();

    assert_eq!(response.state.entities.len(), fixture.entity_count);

    eprintln!(
        "durable resolve stress: entities={} datoms={} elapsed={}",
        fixture.entity_count,
        fixture.datom_count,
        elapsed.as_secs_f64()
    );
}

fn run_durable_coordination_stress(task_count: usize) {
    let fixture = perf::build_durable_coordination_replay_fixture(task_count)
        .expect("build durable coordination fixture");
    let started = Instant::now();
    let mut service =
        SqliteKernelService::open(&fixture.database_path).expect("open durable coordination");
    let response = service
        .run_document(fixture.request.clone())
        .expect("run durable coordination replay");
    let elapsed = started.elapsed();
    let rows = response.query.expect("query result").rows.len();

    assert_eq!(rows, fixture.expected_row_count);

    eprintln!(
        "durable coordination stress: tasks={} rows={} elapsed={}",
        fixture.task_count,
        rows,
        elapsed.as_secs_f64()
    );
}

#[test]
#[ignore = "stress workload for release-mode benchmarking"]
fn stress_recursive_closure_handles_512_node_chain() {
    run_runtime_stress(512);
}

#[test]
#[ignore = "stress workload for release-mode benchmarking"]
fn stress_recursive_closure_handles_1024_node_chain() {
    run_runtime_stress(1_024);
}

#[test]
#[ignore = "stress workload for release-mode benchmarking"]
fn stress_explainer_handles_deep_recursive_trace() {
    run_explain_stress(384);
}

#[test]
#[ignore = "stress workload for release-mode benchmarking"]
fn stress_explainer_handles_512_node_trace() {
    run_explain_stress(512);
}

#[test]
#[ignore = "stress workload for release-mode benchmarking"]
fn stress_service_claimability_handles_1024_tasks() {
    run_service_stress(1_024);
}

#[test]
#[ignore = "stress workload for release-mode benchmarking"]
fn stress_service_claimability_handles_4096_tasks() {
    run_service_stress(4_096);
}

#[test]
#[ignore = "stress workload for release-mode benchmarking"]
fn stress_durable_restart_current_handles_5000_entities() {
    run_durable_resolve_stress(5_000);
}

#[test]
#[ignore = "stress workload for release-mode benchmarking"]
fn stress_durable_restart_coordination_handles_4096_tasks() {
    run_durable_coordination_stress(4_096);
}
