use aether_api::perf::{
    performance_bundle_for_suite, render_markdown_bundle, PerfSuiteId, DEFAULT_HOST_MANIFEST_PATH,
};
use std::{fs, path::PathBuf, str::FromStr};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut suite = PerfSuiteId::FullStack;
    let mut host_manifest: Option<PathBuf> = None;
    let mut bundle_path: Option<PathBuf> = None;
    let mut report_path: Option<PathBuf> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--suite" => {
                let value = args.next().ok_or("--suite requires a value")?;
                suite = PerfSuiteId::from_str(&value)?;
            }
            "--host-manifest" => {
                let value = args.next().ok_or("--host-manifest requires a path")?;
                host_manifest = Some(PathBuf::from(value));
            }
            "--bundle-path" => {
                let value = args.next().ok_or("--bundle-path requires a path")?;
                bundle_path = Some(PathBuf::from(value));
            }
            "--report-path" => {
                let value = args.next().ok_or("--report-path requires a path")?;
                report_path = Some(PathBuf::from(value));
            }
            "--help" | "-h" => {
                println!(
                    "Usage: cargo run -p aether_api --example performance_report --release -- [--suite full_stack] [--host-manifest {}] [--bundle-path artifacts/performance/latest.json] [--report-path artifacts/performance/latest.md]",
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
        host_manifest.as_deref(),
    )?;
    let markdown = render_markdown_bundle(&bundle);

    if let Some(path) = bundle_path {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, serde_json::to_string_pretty(&bundle)?)?;
    }
    if let Some(path) = report_path {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, &markdown)?;
    }

    println!("{markdown}");
    Ok(())
}
