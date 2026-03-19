use aether_api::perf;
use aether_api::KernelService;
use aether_explain::{Explainer, InMemoryExplainer};
use aether_resolver::Resolver;
use aether_rules::{DefaultRuleCompiler, RuleCompiler};
use aether_runtime::{RuleRuntime, SemiNaiveRuntime};
use aether_storage::{InMemoryJournal, Journal};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use std::hint::black_box;

fn bench_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("journal_append");
    for count in [10_000usize, 50_000usize] {
        let fixture = perf::build_append_fixture(count);
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter_batched(
                InMemoryJournal::new,
                |mut journal| {
                    journal
                        .append(black_box(&fixture.datoms))
                        .expect("append fixture");
                    black_box(journal);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_resolve(c: &mut Criterion) {
    let mut current = c.benchmark_group("resolver_current");
    for entity_count in [1_000usize, 5_000usize] {
        let fixture = perf::build_resolve_fixture(entity_count);
        current.bench_with_input(
            BenchmarkId::from_parameter(entity_count),
            &entity_count,
            |b, _| {
                b.iter(|| {
                    let state = aether_resolver::MaterializedResolver
                        .current(black_box(&fixture.schema), black_box(&fixture.datoms))
                        .expect("resolve current");
                    black_box(state);
                });
            },
        );
    }
    current.finish();

    let mut as_of = c.benchmark_group("resolver_as_of");
    for entity_count in [1_000usize, 5_000usize] {
        let fixture = perf::build_resolve_fixture(entity_count);
        as_of.bench_with_input(
            BenchmarkId::from_parameter(entity_count),
            &entity_count,
            |b, _| {
                b.iter(|| {
                    let state = aether_resolver::MaterializedResolver
                        .as_of(
                            black_box(&fixture.schema),
                            black_box(&fixture.datoms),
                            black_box(&fixture.as_of),
                        )
                        .expect("resolve as_of");
                    black_box(state);
                });
            },
        );
    }
    as_of.finish();
}

fn bench_compile(c: &mut Criterion) {
    let mut group = c.benchmark_group("compiler_scc");
    for width in [16usize, 64usize, 96usize] {
        let fixture = perf::build_compile_fixture(width);
        group.bench_with_input(BenchmarkId::from_parameter(width), &width, |b, _| {
            b.iter(|| {
                let compiled = DefaultRuleCompiler
                    .compile(black_box(&fixture.schema), black_box(&fixture.program))
                    .expect("compile program");
                black_box(compiled);
            });
        });
    }
    group.finish();
}

fn bench_runtime(c: &mut Criterion) {
    let mut group = c.benchmark_group("runtime_closure");
    for chain_len in [64usize, 128usize, 192usize] {
        let fixture = perf::build_runtime_fixture(chain_len).expect("runtime fixture");
        group.bench_with_input(
            BenchmarkId::from_parameter(chain_len),
            &chain_len,
            |b, _| {
                b.iter(|| {
                    let derived = SemiNaiveRuntime
                        .evaluate(black_box(&fixture.state), black_box(&fixture.program))
                        .expect("evaluate closure");
                    black_box(derived);
                });
            },
        );
    }
    group.finish();
}

fn bench_explain(c: &mut Criterion) {
    let mut group = c.benchmark_group("explain_tuple");
    for chain_len in [128usize, 192usize] {
        let fixture = perf::build_explain_fixture(chain_len).expect("explain fixture");
        group.bench_with_input(
            BenchmarkId::from_parameter(chain_len),
            &chain_len,
            |b, _| {
                b.iter(|| {
                    let trace = InMemoryExplainer::from_derived_set(black_box(&fixture.derived))
                        .explain_tuple(black_box(&fixture.tuple_id))
                        .expect("explain tuple");
                    black_box(trace);
                });
            },
        );
    }
    group.finish();
}

fn bench_service(c: &mut Criterion) {
    let mut group = c.benchmark_group("service_coordination");
    for task_count in [128usize, 512usize] {
        let mut fixture =
            perf::build_coordination_service_fixture(task_count).expect("service fixture");
        let response = fixture
            .service
            .run_document(fixture.request.clone())
            .expect("warm service document");
        assert_eq!(
            response.query.as_ref().expect("query rows").rows.len(),
            fixture.expected_row_count
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(task_count),
            &task_count,
            |b, _| {
                b.iter(|| {
                    let response = fixture
                        .service
                        .run_document(black_box(fixture.request.clone()))
                        .expect("run coordination document");
                    black_box(response);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    kernel_perf,
    bench_append,
    bench_resolve,
    bench_compile,
    bench_runtime,
    bench_explain,
    bench_service
);
criterion_main!(kernel_perf);
