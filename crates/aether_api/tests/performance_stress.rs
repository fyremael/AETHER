use aether_api::perf;
use aether_api::KernelService;
use aether_explain::{Explainer, InMemoryExplainer};
use aether_runtime::{RuleRuntime, SemiNaiveRuntime};
use std::time::Instant;

#[test]
#[ignore = "stress workload for release-mode benchmarking"]
fn stress_recursive_closure_handles_512_node_chain() {
    let fixture = perf::build_runtime_fixture(512).expect("build runtime stress fixture");
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

#[test]
#[ignore = "stress workload for release-mode benchmarking"]
fn stress_explainer_handles_deep_recursive_trace() {
    let fixture = perf::build_explain_fixture(384).expect("build explain stress fixture");
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

#[test]
#[ignore = "stress workload for release-mode benchmarking"]
fn stress_service_claimability_handles_1024_tasks() {
    let mut fixture =
        perf::build_coordination_service_fixture(1_024).expect("build service stress fixture");
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
