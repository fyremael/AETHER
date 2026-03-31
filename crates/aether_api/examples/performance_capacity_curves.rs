use aether_api::perf::{
    build_capacity_input_bundle, render_markdown_capacity_input_bundle, DEFAULT_HOST_MANIFEST_PATH,
};
use std::{fs, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut host_manifest: Option<PathBuf> = None;
    let mut output_json: Option<PathBuf> = None;
    let mut output_report: Option<PathBuf> = None;
    let mut samples_per_point = 1usize;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--host-manifest" => {
                let value = args.next().ok_or("--host-manifest requires a path")?;
                host_manifest = Some(PathBuf::from(value));
            }
            "--output-json" => {
                let value = args.next().ok_or("--output-json requires a path")?;
                output_json = Some(PathBuf::from(value));
            }
            "--output-report" => {
                let value = args.next().ok_or("--output-report requires a path")?;
                output_report = Some(PathBuf::from(value));
            }
            "--samples" => {
                let value = args.next().ok_or("--samples requires an integer value")?;
                samples_per_point = value.parse::<usize>()?;
            }
            "--help" | "-h" => {
                println!(
                    "Usage: cargo run -p aether_api --example performance_capacity_curves --release -- [--host-manifest {}] [--samples 1] [--output-json artifacts/performance/capacity/inputs.json] [--output-report artifacts/performance/capacity/inputs.md]",
                    DEFAULT_HOST_MANIFEST_PATH
                );
                return Ok(());
            }
            other => return Err(format!("unrecognized argument: {other}").into()),
        }
    }

    let bundle = build_capacity_input_bundle(samples_per_point, host_manifest.as_deref())?;
    let markdown = render_markdown_capacity_input_bundle(&bundle);

    if let Some(path) = output_json {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, serde_json::to_string_pretty(&bundle)?)?;
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
