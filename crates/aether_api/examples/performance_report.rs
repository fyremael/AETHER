use aether_api::perf::{default_performance_report, render_markdown_report};

fn main() -> Result<(), aether_api::ApiError> {
    let report = default_performance_report()?;
    println!("{}", render_markdown_report(&report));
    Ok(())
}
