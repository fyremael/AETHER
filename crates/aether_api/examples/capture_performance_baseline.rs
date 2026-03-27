use aether_api::perf::{
    baseline_from_bundle, performance_bundle_for_suite, PerfSuiteId, DEFAULT_HOST_MANIFEST_PATH,
};
use std::{fs, path::PathBuf, str::FromStr};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut suite = PerfSuiteId::CoreKernel;
    let mut host_manifest = PathBuf::from(DEFAULT_HOST_MANIFEST_PATH);
    let mut output_path: Option<PathBuf> = None;
    let mut label: Option<String> = None;

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
            "--output" => {
                let value = args.next().ok_or("--output requires a path")?;
                output_path = Some(PathBuf::from(value));
            }
            "--label" => {
                let value = args.next().ok_or("--label requires a value")?;
                label = Some(value);
            }
            "--help" | "-h" => {
                println!(
                    "Usage: cargo run -p aether_api --example capture_performance_baseline --release -- --suite core_kernel --host-manifest {} [--output fixtures/performance/baselines/core_kernel/dev-chad-windows-native.json] [--label accepted-baseline]",
                    DEFAULT_HOST_MANIFEST_PATH
                );
                return Ok(());
            }
            other => {
                return Err(format!("unrecognized argument: {other}").into());
            }
        }
    }

    let bundle = performance_bundle_for_suite(
        suite,
        aether_api::perf::DEFAULT_REPORT_SAMPLES,
        Some(host_manifest.as_path()),
    )?;
    let manifest = bundle
        .host_manifest
        .as_ref()
        .ok_or("baseline capture requires a host manifest")?;
    let output_path = output_path.unwrap_or_else(|| {
        PathBuf::from(format!(
            "artifacts/performance/baselines/{}/{}.json",
            suite, manifest.host_id
        ))
    });
    let baseline = baseline_from_bundle(
        label.unwrap_or_else(|| format!("accepted-{}-{}", suite, manifest.host_id)),
        &bundle,
    );

    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output_path, serde_json::to_string_pretty(&baseline)?)?;

    println!("AETHER performance baseline captured");
    println!("  suite: {}", suite);
    println!("  host: {}", manifest.host_id);
    println!("  path: {}", output_path.display());
    println!("  label: {}", baseline.label);
    println!("  generated_at: {}", baseline.generated_at);

    Ok(())
}
