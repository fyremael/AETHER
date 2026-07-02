use aether_api::perf::{
    build_trend_index, render_markdown_trend_index, PerfBaseline, PerfRunBundle,
};
use std::{fs, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut output_json: Option<PathBuf> = None;
    let mut output_report: Option<PathBuf> = None;
    let mut baseline_paths = Vec::new();
    let mut bundle_paths = Vec::new();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output-json" => {
                let value = args.next().ok_or("--output-json requires a path")?;
                output_json = Some(PathBuf::from(value));
            }
            "--output-report" => {
                let value = args.next().ok_or("--output-report requires a path")?;
                output_report = Some(PathBuf::from(value));
            }
            "--baseline" => {
                let value = args.next().ok_or("--baseline requires a path")?;
                baseline_paths.push(PathBuf::from(value));
            }
            "--help" | "-h" => {
                println!(
                    "Usage: cargo run -p aether_api --example performance_trend_report --release -- [--output-json artifacts/performance/trends/latest.json] [--output-report artifacts/performance/trends/latest.md] [--baseline fixtures/performance/baselines/core_kernel/dev-chad-windows-native.json] <bundle> [<bundle> ...]"
                );
                return Ok(());
            }
            other => bundle_paths.push(PathBuf::from(other)),
        }
    }

    if bundle_paths.is_empty() {
        return Err("at least one run bundle path is required".into());
    }

    let mut bundles = Vec::new();
    for path in bundle_paths {
        let bundle: PerfRunBundle = serde_json::from_str(&fs::read_to_string(path)?)?;
        bundles.push(bundle);
    }

    let mut baselines = Vec::new();
    for path in baseline_paths {
        let baseline: PerfBaseline = serde_json::from_str(&fs::read_to_string(path)?)?;
        baselines.push(baseline);
    }

    let index = build_trend_index(&bundles, &baselines);
    let markdown = render_markdown_trend_index(&index);

    if let Some(path) = output_json {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, serde_json::to_string_pretty(&index)?)?;
    }
    if let Some(path) = output_report {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, &markdown)?;
    }

    println!("{markdown}");
    Ok(())
}
