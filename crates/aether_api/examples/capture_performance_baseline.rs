use aether_api::perf::{default_performance_report, PerfBaseline};
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("artifacts/performance/baseline.json"));
    let label = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "pilot-local-baseline".into());
    let baseline = PerfBaseline {
        label,
        generated_at: timestamp_string(),
        report: default_performance_report()?,
    };

    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output_path, serde_json::to_string_pretty(&baseline)?)?;

    println!("AETHER performance baseline captured");
    println!("  path: {}", output_path.display());
    println!("  label: {}", baseline.label);
    println!("  generated_at: {}", baseline.generated_at);

    Ok(())
}

fn timestamp_string() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_secs().to_string()
}
