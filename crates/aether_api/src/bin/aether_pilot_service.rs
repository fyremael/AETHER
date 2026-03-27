use aether_api::{serve_pilot_http_service, PilotServiceConfig};
use std::{env, path::PathBuf};

fn parse_config_path() -> Result<PathBuf, String> {
    let mut args = env::args().skip(1);
    let mut config_path = None;

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--config" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--config requires a path".to_string())?;
                config_path = Some(PathBuf::from(value));
            }
            "--help" | "-h" => {
                println!(
                    "Usage: aether_pilot_service --config <path>\n       AETHER_PILOT_CONFIG=<path> aether_pilot_service"
                );
                std::process::exit(0);
            }
            other => {
                return Err(format!("unrecognized argument: {other}"));
            }
        }
    }

    config_path
        .or_else(|| env::var_os("AETHER_PILOT_CONFIG").map(PathBuf::from))
        .ok_or_else(|| {
            "missing pilot service config path; pass --config <path> or set AETHER_PILOT_CONFIG"
                .into()
        })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = parse_config_path()
        .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message))?;
    let config = PilotServiceConfig::load(&config_path)?.resolve(&config_path)?;

    println!("AETHER pilot HTTP service");
    println!("  config: {}", config_path.display());
    println!("  storage: {}", config.database_path.display());
    println!("  sidecars: {}", config.sidecar_path().display());
    println!("  audit log: {}", config.audit_log_path.display());
    println!("  listening: http://{}", config.bind_addr);
    println!("  configured principals:");
    for token in &config.token_summaries {
        let scopes = token
            .scopes
            .iter()
            .map(|scope| format!("{scope:?}").to_lowercase())
            .collect::<Vec<_>>()
            .join(", ");
        let policy = token
            .policy_context
            .as_ref()
            .map(|policy| {
                format!(
                    "capabilities={:?} visibilities={:?}",
                    policy.capabilities, policy.visibilities
                )
            })
            .unwrap_or_else(|| "public".into());
        println!(
            "    - {} [{}] via {} ({})",
            token.principal, scopes, token.source, policy
        );
    }
    println!("  GET  /health");
    println!("  GET  /v1/status");
    println!("  GET  /v1/audit");
    println!("  POST /v1/admin/auth/reload");
    println!("  GET  /v1/history");
    println!("  POST /v1/append");
    println!("  POST /v1/state/current");
    println!("  POST /v1/state/as-of");
    println!("  POST /v1/documents/parse");
    println!("  POST /v1/documents/run");
    println!("  POST /v1/reports/pilot/coordination-delta");
    println!("  POST /v1/explain/tuple");
    println!("  POST /v1/sidecars/artifacts/register");
    println!("  POST /v1/sidecars/artifacts/get");
    println!("  POST /v1/sidecars/vectors/register");
    println!("  POST /v1/sidecars/vectors/search");

    serve_pilot_http_service(config).await?;
    Ok(())
}
