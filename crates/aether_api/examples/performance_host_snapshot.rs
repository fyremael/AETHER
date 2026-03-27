use aether_api::perf::collect_host_snapshot;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{}",
        serde_json::to_string_pretty(&collect_host_snapshot())?
    );
    Ok(())
}
