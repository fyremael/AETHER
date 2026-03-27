use aether_api::{
    default_audit_log_path, http_router_with_options, serve_pilot_http_service, AuthScope,
    HttpKernelOptions, PilotAuthConfig, PilotServiceConfig, PilotTokenConfig, SqliteKernelService,
};
use aether_ast::PolicyContext;
use std::path::PathBuf;

fn env_config_path() -> Option<PathBuf> {
    std::env::var_os("AETHER_PILOT_CONFIG").map(PathBuf::from)
}

fn developer_config() -> Result<PilotServiceConfig, Box<dyn std::error::Error>> {
    let database_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("artifacts/pilot/coordination.sqlite"));
    let bind_addr = std::env::var("AETHER_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let token_env =
        std::env::var("AETHER_PILOT_TOKEN_ENV").unwrap_or_else(|_| "AETHER_PILOT_TOKEN".into());
    if std::env::var_os(&token_env).is_none() {
        return Err(format!(
            "set {token_env} or AETHER_PILOT_CONFIG before running the durable pilot example"
        )
        .into());
    }

    Ok(PilotServiceConfig {
        bind_addr,
        database_path: database_path.clone(),
        audit_log_path: Some(default_audit_log_path(&database_path)),
        auth: PilotAuthConfig {
            tokens: vec![PilotTokenConfig {
                principal: "pilot-operator".into(),
                scopes: vec![
                    AuthScope::Append,
                    AuthScope::Query,
                    AuthScope::Explain,
                    AuthScope::Ops,
                ],
                policy_context: Some(PolicyContext {
                    capabilities: vec!["executor".into()],
                    visibilities: Vec::new(),
                }),
                token: None,
                token_env: Some(token_env),
                token_file: None,
                token_command: None,
            }],
        },
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(config_path) = env_config_path() {
        let config = PilotServiceConfig::load(&config_path)?.resolve(&config_path)?;
        println!("AETHER durable pilot HTTP service");
        println!("  config: {}", config_path.display());
        println!("  storage: {}", config.database_path.display());
        println!("  sidecars: {}", config.sidecar_path().display());
        println!("  audit log: {}", config.audit_log_path.display());
        println!("  listening: http://{}", config.bind_addr);
        return Ok(serve_pilot_http_service(config).await?);
    }

    let config = developer_config()?;
    let resolved = config.resolve(std::env::current_dir()?.join("aether-pilot-dev.json"))?;
    let service = SqliteKernelService::open(&resolved.database_path)?;
    let listener = tokio::net::TcpListener::bind(&resolved.bind_addr).await?;

    println!("AETHER durable pilot HTTP service");
    println!("  storage: {}", resolved.database_path.display());
    println!("  sidecars: {}", resolved.sidecar_path().display());
    println!("  audit log: {}", resolved.audit_log_path.display());
    println!("  listening: http://{}", resolved.bind_addr);
    println!("  principal sources:");
    for token in &resolved.token_summaries {
        println!("    - {} via {}", token.principal, token.source);
    }
    println!("  GET  /health");
    println!("  GET  /v1/audit");
    println!("  GET  /v1/history");
    println!("  POST /v1/append");
    println!("  POST /v1/state/current");
    println!("  POST /v1/state/as-of");
    println!("  POST /v1/documents/parse");
    println!("  POST /v1/documents/run");
    println!("  POST /v1/reports/pilot/coordination");
    println!("  POST /v1/explain/tuple");
    println!("  POST /v1/sidecars/artifacts/register");
    println!("  POST /v1/sidecars/artifacts/get");
    println!("  POST /v1/sidecars/vectors/register");
    println!("  POST /v1/sidecars/vectors/search");

    let options = HttpKernelOptions::new()
        .with_auth(resolved.auth.clone())
        .with_audit_log_path(resolved.audit_log_path);
    axum::serve(listener, http_router_with_options(service, options)).await?;
    Ok(())
}
