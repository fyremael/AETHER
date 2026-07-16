use aether_api::perf::{
    compare_perf_bundle_to_baseline, performance_bundle_for_suite, render_markdown_drift_report,
    PerfBaseline, PerfSuiteId, PerfVerdictPolicy, DEFAULT_HOST_MANIFEST_PATH,
};
use std::{fs, path::PathBuf, str::FromStr};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut suite = PerfSuiteId::CoreKernel;
    let mut host_manifest = PathBuf::from(DEFAULT_HOST_MANIFEST_PATH);
    let mut baseline_path =
        PathBuf::from("artifacts/performance/baselines/core_kernel/baseline.json");
    let mut bundle_path: Option<PathBuf> = None;
    let mut report_path: Option<PathBuf> = None;
    let mut verdict_policy_path = PathBuf::from("fixtures/performance/verdict-policy.json");

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--suite" => {
                let value = args.next().ok_or("--suite requires a value")?;
                suite = PerfSuiteId::from_str(&value)?;
            }
            "--host-manifest" => {
                let value = args.next().ok_or("--host-manifest requires a path")?;
                host_manifest = PathBuf::from(value);
            }
            "--baseline" => {
                let value = args.next().ok_or("--baseline requires a path")?;
                baseline_path = PathBuf::from(value);
            }
            "--bundle-path" => {
                let value = args.next().ok_or("--bundle-path requires a path")?;
                bundle_path = Some(PathBuf::from(value));
            }
            "--report-path" => {
                let value = args.next().ok_or("--report-path requires a path")?;
                report_path = Some(PathBuf::from(value));
            }
            "--verdict-policy" => {
                let value = args.next().ok_or("--verdict-policy requires a path")?;
                verdict_policy_path = PathBuf::from(value);
            }
            "--help" | "-h" => {
                println!(
                    "Usage: cargo run -p aether_api --example performance_drift_report --release -- --suite core_kernel --host-manifest {} --baseline fixtures/performance/baselines/core_kernel/dev-chad-windows-native.json [--verdict-policy fixtures/performance/verdict-policy.json] [--bundle-path artifacts/performance/latest.json] [--report-path artifacts/performance/latest-drift.md]",
                    DEFAULT_HOST_MANIFEST_PATH
                );
                return Ok(());
            }
            other => {
                return Err(format!("unrecognized argument: {other}").into());
            }
        }
    }

    let baseline: PerfBaseline = serde_json::from_str(&fs::read_to_string(baseline_path)?)?;
    let verdict_policy: PerfVerdictPolicy =
        serde_json::from_str(&fs::read_to_string(verdict_policy_path)?)?;
    verdict_policy.validate()?;
    let current = performance_bundle_for_suite(
        suite,
        verdict_policy.samples_per_workload,
        Some(host_manifest.as_path()),
    )?;
    let mut drift = compare_perf_bundle_to_baseline(
        &current,
        &baseline,
        &verdict_policy.budgets,
        current.generated_at.clone(),
    )?;
    drift.verdict_policy_version = Some(verdict_policy.policy_version.clone());
    drift.verdict_statistic = Some(verdict_policy.latency_statistic.clone());
    let markdown = render_markdown_drift_report(&drift);

    if let Some(path) = bundle_path {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, serde_json::to_string_pretty(&current)?)?;
    }
    if let Some(path) = report_path {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, &markdown)?;
    }

    println!("{markdown}");
    if !verdict_policy.pass_severities.contains(&drift.overall) {
        std::process::exit(2);
    }
    Ok(())
}
