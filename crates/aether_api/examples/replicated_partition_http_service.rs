use aether_api::{
    http_router_with_partitioned_options, AuthScope, AuthorityPartitionConfig, HttpAuthConfig,
    HttpKernelOptions, InMemoryKernelService, ReplicaConfig, ReplicaRole,
    ReplicatedAuthorityPartitionService, ServiceMode, ServiceStatusResponse, ServiceStatusStorage,
};
use aether_ast::{PartitionId, PolicyContext, ReplicaId};
use std::{env, net::SocketAddr, path::PathBuf};

fn parse_args() -> Result<(SocketAddr, PathBuf), String> {
    let mut args = env::args().skip(1);
    let mut bind_addr = SocketAddr::from(([127, 0, 0, 1], 3400));
    let mut root = PathBuf::from("artifacts/partitions/prototype");

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--bind" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--bind requires an address".to_string())?;
                bind_addr = value
                    .parse()
                    .map_err(|error| format!("invalid --bind value: {error}"))?;
            }
            "--root" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--root requires a path".to_string())?;
                root = PathBuf::from(value);
            }
            "--help" | "-h" => {
                println!(
                    "Usage: cargo run -p aether_api --example replicated_partition_http_service -- [--bind 127.0.0.1:3400] [--root artifacts/partitions/prototype]"
                );
                std::process::exit(0);
            }
            other => {
                return Err(format!("unrecognized argument: {other}"));
            }
        }
    }

    Ok((bind_addr, root))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (bind_addr, root) = parse_args()
        .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message))?;

    let partitioned = ReplicatedAuthorityPartitionService::open(
        &root,
        vec![
            AuthorityPartitionConfig {
                partition: PartitionId::new("readiness"),
                replicas: vec![
                    ReplicaConfig {
                        replica_id: ReplicaId::new(1),
                        database_path: PathBuf::from("readiness-leader.sqlite"),
                        role: ReplicaRole::Leader,
                    },
                    ReplicaConfig {
                        replica_id: ReplicaId::new(2),
                        database_path: PathBuf::from("readiness-follower.sqlite"),
                        role: ReplicaRole::Follower,
                    },
                ],
            },
            AuthorityPartitionConfig {
                partition: PartitionId::new("authority"),
                replicas: vec![
                    ReplicaConfig {
                        replica_id: ReplicaId::new(1),
                        database_path: PathBuf::from("authority-leader.sqlite"),
                        role: ReplicaRole::Leader,
                    },
                    ReplicaConfig {
                        replica_id: ReplicaId::new(2),
                        database_path: PathBuf::from("authority-follower.sqlite"),
                        role: ReplicaRole::Follower,
                    },
                ],
            },
        ],
    )?;

    let options = HttpKernelOptions::new()
        .with_auth(
            HttpAuthConfig::new()
                .with_token_context(
                    "pilot-operator-token",
                    "pilot-operator",
                    [
                        AuthScope::Append,
                        AuthScope::Query,
                        AuthScope::Explain,
                        AuthScope::Ops,
                    ],
                    PolicyContext {
                        capabilities: vec!["executor".into()],
                        visibilities: Vec::new(),
                    },
                )
                .with_token("pilot-query-token", "query-client", [AuthScope::Query]),
        )
        .with_service_status(ServiceStatusResponse {
            status: "ok".into(),
            build_version: env!("CARGO_PKG_VERSION").into(),
            config_version: "replicated-prototype".into(),
            schema_version: "v1".into(),
            bind_addr: Some(bind_addr.to_string()),
            service_mode: ServiceMode::Partitioned,
            storage: ServiceStatusStorage {
                database_path: None,
                sidecar_path: None,
                audit_log_path: None,
                partition_root: Some(root.clone()),
            },
            principals: Vec::new(),
            replicas: Vec::new(),
        });
    let router =
        http_router_with_partitioned_options(InMemoryKernelService::new(), partitioned, options);

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    println!("AETHER replicated partition prototype");
    println!("  root: {}", root.display());
    println!("  listening: http://{}", bind_addr);
    println!("  GET  /health");
    println!("  GET  /v1/status");
    println!("  GET  /v1/partitions/status");
    println!("  POST /v1/partitions/append");
    println!("  POST /v1/partitions/promote");
    println!("  POST /v1/federated/history");
    println!("  POST /v1/federated/run");
    println!("  POST /v1/federated/report");
    axum::serve(listener, router).await?;
    Ok(())
}
