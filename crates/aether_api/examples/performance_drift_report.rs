use aether_api::perf::{
    compare_perf_reports, default_performance_report, render_markdown_drift_report, DriftSeverity,
    PerfBaseline, PerfDriftBudget,
};
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let baseline_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("artifacts/performance/baseline.json"));
    let baseline: PerfBaseline = serde_json::from_str(&fs::read_to_string(baseline_path)?)?;
    let current = default_performance_report()?;
    let drift = compare_perf_reports(
        &current,
        &baseline,
        &PerfDriftBudget::default(),
        timestamp_string(),
    );

    println!("{}", render_markdown_drift_report(&drift));

    if drift.overall == DriftSeverity::Fail {
        std::process::exit(2);
    }

    Ok(())
}

fn timestamp_string() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_secs().to_string()
}
