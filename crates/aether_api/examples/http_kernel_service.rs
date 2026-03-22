use aether_api::{http_router, InMemoryKernelService};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("AETHER_BIND_ADDR").ok())
        .unwrap_or_else(|| "127.0.0.1:3000".into());
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    println!("AETHER HTTP kernel service listening on http://{bind_addr}");
    println!("  GET  /health");
    println!("  GET  /v1/history");
    println!("  POST /v1/append");
    println!("  POST /v1/state/current");
    println!("  POST /v1/state/as-of");
    println!("  POST /v1/documents/parse");
    println!("  POST /v1/documents/run");
    println!("  POST /v1/explain/tuple");
    println!("  POST /v1/sidecars/artifacts/register");
    println!("  POST /v1/sidecars/artifacts/get");
    println!("  POST /v1/sidecars/vectors/register");
    println!("  POST /v1/sidecars/vectors/search");

    axum::serve(listener, http_router(InMemoryKernelService::new())).await?;
    Ok(())
}
