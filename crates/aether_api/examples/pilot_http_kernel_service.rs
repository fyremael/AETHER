use aether_api::{http_router, SqliteKernelService};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("artifacts/pilot/coordination.sqlite"));
    let service = SqliteKernelService::open(&database_path)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;

    println!("AETHER coordination pilot HTTP service");
    println!("  storage: {}", database_path.display());
    println!("  listening: http://127.0.0.1:3000");
    println!("  GET  /health");
    println!("  GET  /v1/history");
    println!("  POST /v1/append");
    println!("  POST /v1/state/current");
    println!("  POST /v1/state/as-of");
    println!("  POST /v1/documents/parse");
    println!("  POST /v1/documents/run");
    println!("  POST /v1/explain/tuple");

    axum::serve(listener, http_router(service)).await?;
    Ok(())
}
