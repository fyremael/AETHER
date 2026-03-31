use aether_api::perf::{
    build_capacity_report, load_capacity_input_bundle, load_perturbation_summary,
    render_markdown_capacity_report, write_capacity_report, PerfCapacityArtifactPaths,
    PerfMatrixReport,
};
use std::{fs, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut perturbation_json: Option<PathBuf> = None;
    let mut matrix_json: Option<PathBuf> = None;
    let mut capacity_inputs_json: Option<PathBuf> = None;
    let mut output_json: Option<PathBuf> = None;
    let mut output_report: Option<PathBuf> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--perturbation-json" => {
                perturbation_json = Some(PathBuf::from(
                    args.next().ok_or("--perturbation-json requires a path")?,
                ));
            }
            "--matrix-json" => {
                matrix_json = Some(PathBuf::from(
                    args.next().ok_or("--matrix-json requires a path")?,
                ));
            }
            "--capacity-inputs-json" => {
                capacity_inputs_json = Some(PathBuf::from(
                    args.next()
                        .ok_or("--capacity-inputs-json requires a path")?,
                ));
            }
            "--output-json" => {
                output_json = Some(PathBuf::from(
                    args.next().ok_or("--output-json requires a path")?,
                ));
            }
            "--output-report" => {
                output_report = Some(PathBuf::from(
                    args.next().ok_or("--output-report requires a path")?,
                ));
            }
            "--help" | "-h" => {
                println!(
                    "Usage: cargo run -p aether_api --example performance_capacity_report --release -- --perturbation-json artifacts/performance/perturbation/latest.json --matrix-json artifacts/performance/matrix/latest.json [--capacity-inputs-json artifacts/performance/perturbation/runs/<timestamp>/capacity-curves.json] --output-json artifacts/performance/capacity/latest.json --output-report artifacts/performance/capacity/latest.md"
                );
                return Ok(());
            }
            other => return Err(format!("unrecognized argument: {other}").into()),
        }
    }

    let perturbation_path =
        perturbation_json.ok_or("--perturbation-json is required for capacity planning")?;
    let matrix_path = matrix_json.ok_or("--matrix-json is required for capacity planning")?;
    let perturbation = load_perturbation_summary(&perturbation_path)?;
    let matrix: PerfMatrixReport = serde_json::from_str(&fs::read_to_string(&matrix_path)?)?;
    let capacity_inputs_path = match capacity_inputs_json {
        Some(path) => path,
        None => perturbation
            .capacity_inputs
            .as_ref()
            .map(|pointer| PathBuf::from(&pointer.json_path))
            .ok_or(
                "--capacity-inputs-json is required when perturbation summary does not point to capacity inputs",
            )?,
    };
    let inputs = load_capacity_input_bundle(&capacity_inputs_path)?;

    let artifact_paths = PerfCapacityArtifactPaths {
        perturbation_json_path: perturbation_path.display().to_string(),
        matrix_json_path: matrix_path.display().to_string(),
        capacity_input_json_path: capacity_inputs_path.display().to_string(),
    };
    let report = build_capacity_report(&perturbation, &matrix, &inputs, artifact_paths)?;
    let markdown = render_markdown_capacity_report(&report);

    if let (Some(json_path), Some(report_path)) = (output_json.as_ref(), output_report.as_ref()) {
        write_capacity_report(&report, json_path, report_path)?;
    } else {
        if let Some(path) = output_json {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(path, serde_json::to_string_pretty(&report)?)?;
        }
        if let Some(path) = output_report {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(path, &markdown)?;
        }
    }

    println!("{markdown}");
    Ok(())
}
