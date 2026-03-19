use aether_api::{http_router, InMemoryKernelService};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("AETHER HTTP kernel service listening on http://127.0.0.1:3000");
    println!("  GET  /health");
    println!("  GET  /v1/history");
    println!("  POST /v1/append");
    println!("  POST /v1/state/current");
    println!("  POST /v1/state/as-of");
    println!("  POST /v1/documents/parse");
    println!("  POST /v1/documents/run");
    println!("  POST /v1/explain/tuple");

    axum::serve(listener, http_router(InMemoryKernelService::new())).await?;
    Ok(())
}
