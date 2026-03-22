use aether_api::{
    http_router_with_options, AuthScope, HttpAuthConfig, HttpKernelOptions, SqliteKernelService,
};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("artifacts/pilot/coordination.sqlite"));
    let bind_addr = std::env::var("AETHER_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let audit_path = database_path.with_extension("audit.jsonl");
    let operator_token =
        std::env::var("AETHER_PILOT_TOKEN").unwrap_or_else(|_| "pilot-operator-token".into());
    let service = SqliteKernelService::open(&database_path)?;
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    let auth = HttpAuthConfig::new().with_token(
        operator_token.clone(),
        "pilot-operator",
        [
            AuthScope::Append,
            AuthScope::Query,
            AuthScope::Explain,
            AuthScope::Ops,
        ],
    );

    println!("AETHER coordination pilot HTTP service");
    println!("  storage: {}", database_path.display());
    println!(
        "  sidecars: {}",
        aether_api::sidecar::sidecar_catalog_path_for_journal(&database_path).display()
    );
    println!("  audit log: {}", audit_path.display());
    println!("  listening: http://{bind_addr}");
    println!("  bearer token: {}", operator_token);
    println!("  GET  /health");
    println!("  GET  /v1/audit");
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

    let options = HttpKernelOptions::new()
        .with_auth(auth)
        .with_audit_log_path(audit_path);
    axum::serve(listener, http_router_with_options(service, options)).await?;
    Ok(())
}
