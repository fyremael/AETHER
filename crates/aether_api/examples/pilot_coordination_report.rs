use aether_api::{
    build_coordination_pilot_report, coordination_pilot_seed_history,
    render_coordination_pilot_report_markdown, AppendRequest, HistoryRequest, KernelService,
    SqliteKernelService,
};
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("artifacts/pilot/coordination.sqlite"));
    let report_dir = PathBuf::from("artifacts/pilot/reports");
    let mut service = SqliteKernelService::open(&database_path)?;

    let seeded = if service.history(HistoryRequest)?.datoms.is_empty() {
        service.append(AppendRequest {
            datoms: coordination_pilot_seed_history(),
        })?;
        true
    } else {
        false
    };

    let report = build_coordination_pilot_report(&mut service)?;
    let markdown = render_coordination_pilot_report_markdown(&report);
    let json = serde_json::to_string_pretty(&report)?;

    fs::create_dir_all(&report_dir)?;
    let timestamp = timestamp_slug();
    let markdown_path = report_dir.join(format!("coordination-report-{timestamp}.md"));
    let json_path = report_dir.join(format!("coordination-report-{timestamp}.json"));
    let latest_markdown = report_dir.join("latest.md");
    let latest_json = report_dir.join("latest.json");

    fs::write(&markdown_path, &markdown)?;
    fs::write(&json_path, &json)?;
    fs::write(&latest_markdown, &markdown)?;
    fs::write(&latest_json, &json)?;

    println!("AETHER coordination pilot report");
    println!("  storage: {}", database_path.display());
    if seeded {
        println!("  seed data: appended default coordination pilot history");
    }
    println!("  markdown: {}", markdown_path.display());
    println!("  json: {}", json_path.display());
    println!("  latest markdown: {}", latest_markdown.display());
    println!("  latest json: {}", latest_json.display());

    Ok(())
}

fn timestamp_slug() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_secs().to_string()
}
