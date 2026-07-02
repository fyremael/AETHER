use crate::{
    http_router_with_options, http_router_with_postgres_namespaces,
    http_router_with_sqlite_namespaces, sidecar::sidecar_catalog_path_for_journal, ApiError,
    AuthScope, HttpAccessToken, HttpAuthConfig, HttpKernelOptions, NamespaceId,
    NamespaceStatusSummary, PrincipalStatusSummary, ServiceMode, ServiceStatusResponse,
    ServiceStatusStorage, SqliteKernelService,
};
use aether_ast::PolicyContext;
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    path::{Component, Path, PathBuf},
    process::{Command, Stdio},
};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PilotServiceConfig {
    #[serde(default = "default_config_version")]
    pub config_version: String,
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    #[serde(default)]
    pub service_mode: ServiceMode,
    pub bind_addr: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<PilotStorageConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audit_log_path: Option<PathBuf>,
    pub auth: PilotAuthConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PilotStorageConfig {
    Sqlite {
        data_root: PathBuf,
    },
    Postgres {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        database_url_env: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        database_url_command: Option<Vec<String>>,
        #[serde(default)]
        schema: Option<String>,
        sidecar_path: PathBuf,
    },
}

impl PilotServiceConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, DeploymentError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|source| DeploymentError::ReadConfig {
            path: path.to_path_buf(),
            source,
        })?;
        serde_json::from_str(&contents).map_err(|source| DeploymentError::ParseConfig {
            path: path.to_path_buf(),
            source,
        })
    }

    pub fn resolve(
        self,
        config_path: impl AsRef<Path>,
    ) -> Result<ResolvedPilotServiceConfig, DeploymentError> {
        let config_path = config_path.as_ref();
        let config_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
        if self.bind_addr.trim().is_empty() {
            return Err(DeploymentError::Validation(
                "pilot service bind_addr must not be empty".into(),
            ));
        }

        let storage = resolve_storage_config(config_dir, self.database_path, self.storage)?;
        let audit_log_path = self
            .audit_log_path
            .map(|path| resolve_path(config_dir, &path))
            .unwrap_or_else(|| storage.default_audit_log_path());

        let resolved_tokens = self.auth.resolve(config_dir)?;
        let auth = resolved_tokens
            .iter()
            .fold(HttpAuthConfig::new(), |mut auth, token| {
                auth.tokens.push(HttpAccessToken {
                    token: token.token.clone(),
                    token_id: token.token_id.clone(),
                    principal: token.principal.clone(),
                    principal_id: token.principal_id.clone(),
                    scopes: token.scopes.clone(),
                    namespaces: token.namespaces.clone(),
                    policy_context: token.policy_context.clone(),
                    source: token.source.clone(),
                    revoked: token.revoked,
                });
                auth
            });

        Ok(ResolvedPilotServiceConfig {
            config_path: config_path.to_path_buf(),
            config_version: self.config_version,
            schema_version: self.schema_version,
            service_mode: self.service_mode,
            bind_addr: self.bind_addr,
            database_path: storage.legacy_database_path(),
            storage,
            audit_log_path,
            auth,
            token_summaries: resolved_tokens
                .into_iter()
                .map(|token| ResolvedPilotTokenSummary {
                    principal: token.principal,
                    principal_id: token.principal_id,
                    token_id: token.token_id,
                    scopes: token.scopes,
                    namespaces: token.namespaces,
                    policy_context: token.policy_context,
                    source: token.source,
                    revoked: token.revoked,
                })
                .collect(),
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PilotAuthConfig {
    pub tokens: Vec<PilotTokenConfig>,
    #[serde(default)]
    pub revoked_token_ids: Vec<String>,
    #[serde(default)]
    pub revoked_principal_ids: Vec<String>,
}

impl PilotAuthConfig {
    fn resolve(&self, config_dir: &Path) -> Result<Vec<ResolvedPilotToken>, DeploymentError> {
        if self.tokens.is_empty() {
            return Err(DeploymentError::Validation(
                "pilot service auth.tokens must contain at least one token".into(),
            ));
        }
        let revoked_token_ids = self
            .revoked_token_ids
            .iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect::<std::collections::BTreeSet<_>>();
        let revoked_principal_ids = self
            .revoked_principal_ids
            .iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect::<std::collections::BTreeSet<_>>();
        let resolved = self
            .tokens
            .iter()
            .map(|token| token.resolve(config_dir, &revoked_token_ids, &revoked_principal_ids))
            .collect::<Result<Vec<_>, _>>()?;
        let mut seen = std::collections::BTreeSet::new();
        for token in &resolved {
            if !seen.insert(token.token_id.clone()) {
                return Err(DeploymentError::Validation(format!(
                    "pilot auth token_id {} is duplicated",
                    token.token_id
                )));
            }
        }
        Ok(resolved)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PilotTokenConfig {
    pub principal: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_id: Option<String>,
    pub scopes: Vec<AuthScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_command: Option<Vec<String>>,
    #[serde(default)]
    pub namespaces: Vec<NamespaceId>,
    #[serde(default)]
    pub revoked: bool,
}

impl PilotTokenConfig {
    fn resolve(
        &self,
        config_dir: &Path,
        revoked_token_ids: &std::collections::BTreeSet<String>,
        revoked_principal_ids: &std::collections::BTreeSet<String>,
    ) -> Result<ResolvedPilotToken, DeploymentError> {
        if self.principal.trim().is_empty() {
            return Err(DeploymentError::Validation(
                "pilot auth principal must not be empty".into(),
            ));
        }
        if self.scopes.is_empty() {
            return Err(DeploymentError::Validation(format!(
                "pilot auth principal {} must declare at least one scope",
                self.principal
            )));
        }
        let namespaces = if self.namespaces.is_empty() {
            vec![NamespaceId::default()]
        } else {
            self.namespaces.clone()
        };
        let principal_id = self
            .principal_id
            .clone()
            .unwrap_or_else(|| format!("principal:{}", self.principal.trim()))
            .trim()
            .to_string();
        if principal_id.is_empty() {
            return Err(DeploymentError::Validation(format!(
                "pilot auth principal {} resolved an empty principal_id",
                self.principal
            )));
        }
        let token_id = self
            .token_id
            .clone()
            .unwrap_or_else(|| format!("token:{}", self.principal.trim()))
            .trim()
            .to_string();
        if token_id.is_empty() {
            return Err(DeploymentError::Validation(format!(
                "pilot auth principal {} resolved an empty token_id",
                self.principal
            )));
        }

        let mut sources = 0;
        if self.token.is_some() {
            sources += 1;
        }
        if self.token_env.is_some() {
            sources += 1;
        }
        if self.token_file.is_some() {
            sources += 1;
        }
        if self.token_command.is_some() {
            sources += 1;
        }
        if sources != 1 {
            return Err(DeploymentError::Validation(format!(
                "pilot auth principal {} must declare exactly one token source (token, token_env, token_file, or token_command)",
                self.principal
            )));
        }

        let (token, source) = if let Some(token) = &self.token {
            let token = token.trim();
            if token.is_empty() {
                return Err(DeploymentError::Validation(format!(
                    "pilot auth principal {} has an empty inline token",
                    self.principal
                )));
            }
            (token.to_string(), "inline".to_string())
        } else if let Some(token_env) = &self.token_env {
            let token = env::var(token_env)
                .map_err(|_| DeploymentError::MissingTokenEnv(token_env.clone()))?;
            let token = token.trim().to_string();
            if token.is_empty() {
                return Err(DeploymentError::Validation(format!(
                    "environment token {} for principal {} is empty",
                    token_env, self.principal
                )));
            }
            (token, format!("env:{token_env}"))
        } else if let Some(token_file_path) = &self.token_file {
            let token_file = resolve_path(config_dir, token_file_path);
            let token = fs::read_to_string(token_file.clone()).map_err(|source| {
                DeploymentError::ReadTokenFile {
                    path: token_file.clone(),
                    source,
                }
            })?;
            let token = token.trim().to_string();
            if token.is_empty() {
                return Err(DeploymentError::Validation(format!(
                    "token file {} for principal {} is empty",
                    token_file.display(),
                    self.principal
                )));
            }
            (token, format!("file:{}", token_file.display()))
        } else {
            let token_command = self
                .token_command
                .as_ref()
                .expect("token_command source already validated");
            let (token, source) =
                resolve_token_command(config_dir, token_command, &self.principal)?;
            (token, source)
        };

        Ok(ResolvedPilotToken {
            principal: self.principal.clone(),
            principal_id: principal_id.clone(),
            token_id: token_id.clone(),
            scopes: self.scopes.clone(),
            namespaces,
            policy_context: normalize_policy_context(self.policy_context.clone()),
            token,
            source,
            revoked: self.revoked
                || revoked_token_ids.contains(&token_id)
                || revoked_principal_ids.contains(&principal_id),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedPilotServiceConfig {
    pub config_path: PathBuf,
    pub config_version: String,
    pub schema_version: String,
    pub service_mode: ServiceMode,
    pub bind_addr: String,
    pub database_path: Option<PathBuf>,
    pub storage: ResolvedPilotStorage,
    pub audit_log_path: PathBuf,
    pub auth: HttpAuthConfig,
    pub token_summaries: Vec<ResolvedPilotTokenSummary>,
}

impl ResolvedPilotServiceConfig {
    pub fn sidecar_path(&self) -> PathBuf {
        self.storage.sidecar_path()
    }

    pub fn service_status(&self) -> ServiceStatusResponse {
        let principals = self
            .token_summaries
            .iter()
            .map(|summary| summary.status_summary())
            .collect::<Vec<_>>();
        ServiceStatusResponse {
            status: "ok".into(),
            build_version: env!("CARGO_PKG_VERSION").into(),
            config_version: self.config_version.clone(),
            schema_version: self.schema_version.clone(),
            bind_addr: Some(self.bind_addr.clone()),
            service_mode: self.service_mode.clone(),
            storage: self.storage.status_storage(self.audit_log_path.clone()),
            active_namespace_count: 0,
            namespaces: namespace_status_from_principals(&principals),
            principals,
            replicas: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolvedPilotStorage {
    LegacySqlite {
        database_path: PathBuf,
        sidecar_path: PathBuf,
    },
    SqliteNamespaces {
        data_root: PathBuf,
    },
    PostgresNamespaces {
        database_url: String,
        schema: String,
        sidecar_path: PathBuf,
    },
}

impl ResolvedPilotStorage {
    pub fn legacy_database_path(&self) -> Option<PathBuf> {
        match self {
            Self::LegacySqlite { database_path, .. } => Some(database_path.clone()),
            Self::SqliteNamespaces { .. } | Self::PostgresNamespaces { .. } => None,
        }
    }

    pub fn sidecar_path(&self) -> PathBuf {
        match self {
            Self::LegacySqlite { sidecar_path, .. } => sidecar_path.clone(),
            Self::SqliteNamespaces { data_root } => data_root.join("sidecars.sqlite"),
            Self::PostgresNamespaces { sidecar_path, .. } => sidecar_path.clone(),
        }
    }

    pub fn storage_label(&self) -> String {
        match self {
            Self::LegacySqlite { database_path, .. } => database_path.display().to_string(),
            Self::SqliteNamespaces { data_root } => {
                format!("sqlite namespaces under {}", data_root.display())
            }
            Self::PostgresNamespaces { schema, .. } => {
                format!("postgres journal schema {schema}")
            }
        }
    }

    fn default_audit_log_path(&self) -> PathBuf {
        match self {
            Self::LegacySqlite { database_path, .. } => default_audit_log_path(database_path),
            Self::SqliteNamespaces { data_root } => data_root.join("audit.jsonl"),
            Self::PostgresNamespaces { sidecar_path, .. } => {
                sidecar_path.with_extension("audit.jsonl")
            }
        }
    }

    fn status_storage(&self, audit_log_path: PathBuf) -> ServiceStatusStorage {
        match self {
            Self::LegacySqlite {
                database_path,
                sidecar_path,
            } => ServiceStatusStorage {
                backend: "sqlite".into(),
                database_path: Some(database_path.clone()),
                data_root: None,
                postgres_schema: None,
                postgres_url_configured: false,
                sidecar_mode: "sqlite_local".into(),
                sidecar_path: Some(sidecar_path.clone()),
                audit_log_path: Some(audit_log_path),
                partition_root: None,
            },
            Self::SqliteNamespaces { data_root } => ServiceStatusStorage {
                backend: "sqlite".into(),
                database_path: None,
                data_root: Some(data_root.clone()),
                postgres_schema: None,
                postgres_url_configured: false,
                sidecar_mode: "sqlite_local_per_namespace".into(),
                sidecar_path: None,
                audit_log_path: Some(audit_log_path),
                partition_root: None,
            },
            Self::PostgresNamespaces {
                schema,
                sidecar_path,
                ..
            } => ServiceStatusStorage {
                backend: "postgres".into(),
                database_path: None,
                data_root: None,
                postgres_schema: Some(schema.clone()),
                postgres_url_configured: true,
                sidecar_mode: "sqlite_local".into(),
                sidecar_path: Some(sidecar_path.clone()),
                audit_log_path: Some(audit_log_path),
                partition_root: None,
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedPilotTokenSummary {
    pub principal: String,
    pub principal_id: String,
    pub token_id: String,
    pub scopes: Vec<AuthScope>,
    pub namespaces: Vec<NamespaceId>,
    pub policy_context: Option<PolicyContext>,
    pub source: String,
    pub revoked: bool,
}

impl ResolvedPilotTokenSummary {
    pub fn status_summary(&self) -> PrincipalStatusSummary {
        PrincipalStatusSummary {
            principal: self.principal.clone(),
            principal_id: self.principal_id.clone(),
            token_id: self.token_id.clone(),
            scopes: self
                .scopes
                .iter()
                .map(|scope| format!("{scope:?}").to_lowercase())
                .collect(),
            namespaces: self.namespaces.iter().map(ToString::to_string).collect(),
            policy_context: self.policy_context.clone(),
            source: self.source.clone(),
            revoked: self.revoked,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolvedPilotToken {
    principal: String,
    principal_id: String,
    token_id: String,
    scopes: Vec<AuthScope>,
    namespaces: Vec<NamespaceId>,
    policy_context: Option<PolicyContext>,
    token: String,
    source: String,
    revoked: bool,
}

fn namespace_status_from_principals(
    principals: &[PrincipalStatusSummary],
) -> Vec<NamespaceStatusSummary> {
    let mut namespaces = std::collections::BTreeMap::<String, Vec<String>>::new();
    for principal in principals {
        for namespace in &principal.namespaces {
            namespaces
                .entry(namespace.clone())
                .or_default()
                .push(principal.principal.clone());
        }
    }
    namespaces
        .into_iter()
        .map(|(namespace, mut principals)| {
            principals.sort();
            principals.dedup();
            NamespaceStatusSummary {
                namespace,
                principals,
            }
        })
        .collect()
}

fn resolve_storage_config(
    config_dir: &Path,
    legacy_database_path: Option<PathBuf>,
    storage: Option<PilotStorageConfig>,
) -> Result<ResolvedPilotStorage, DeploymentError> {
    match storage {
        Some(PilotStorageConfig::Sqlite { data_root }) => {
            Ok(ResolvedPilotStorage::SqliteNamespaces {
                data_root: resolve_path(config_dir, &data_root),
            })
        }
        Some(PilotStorageConfig::Postgres {
            database_url_env,
            database_url_command,
            schema,
            sidecar_path,
        }) => {
            let database_url =
                resolve_database_url(config_dir, database_url_env, database_url_command)?;
            Ok(ResolvedPilotStorage::PostgresNamespaces {
                database_url,
                schema: schema.unwrap_or_else(|| "aether".into()),
                sidecar_path: resolve_path(config_dir, &sidecar_path),
            })
        }
        None => {
            let database_path = legacy_database_path.ok_or_else(|| {
                DeploymentError::Validation(
                    "pilot service config must declare either database_path or storage".into(),
                )
            })?;
            let database_path = resolve_path(config_dir, &database_path);
            let sidecar_path = sidecar_catalog_path_for_journal(&database_path);
            Ok(ResolvedPilotStorage::LegacySqlite {
                database_path,
                sidecar_path,
            })
        }
    }
}

fn resolve_database_url(
    config_dir: &Path,
    database_url_env: Option<String>,
    database_url_command: Option<Vec<String>>,
) -> Result<String, DeploymentError> {
    let source_count =
        usize::from(database_url_env.is_some()) + usize::from(database_url_command.is_some());
    if source_count != 1 {
        return Err(DeploymentError::Validation(
            "postgres storage must declare exactly one database_url_env or database_url_command"
                .into(),
        ));
    }
    if let Some(env_name) = database_url_env {
        let value = env::var(&env_name)
            .map_err(|_| DeploymentError::MissingDatabaseUrlEnv(env_name.clone()))?;
        let value = value.trim().to_string();
        if value.is_empty() {
            return Err(DeploymentError::Validation(format!(
                "postgres database URL environment variable {env_name} is empty"
            )));
        }
        Ok(value)
    } else {
        let command = database_url_command.expect("database_url_command source already validated");
        resolve_database_url_command(config_dir, &command)
    }
}

fn resolve_database_url_command(
    config_dir: &Path,
    database_url_command: &[String],
) -> Result<String, DeploymentError> {
    let (program, args) = database_url_command.split_first().ok_or_else(|| {
        DeploymentError::Validation("postgres database_url_command must not be empty".into())
    })?;
    let command_path = resolve_command_path(config_dir, program);
    let output = Command::new(command_path)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|source| DeploymentError::RunDatabaseUrlCommand {
            command: display_command(program, args),
            source,
        })?;
    if !output.status.success() {
        return Err(DeploymentError::DatabaseUrlCommandFailed {
            command: display_command(program, args),
            exit_code: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    let database_url = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if database_url.is_empty() {
        return Err(DeploymentError::Validation(format!(
            "postgres database_url_command {} returned an empty URL",
            display_command(program, args)
        )));
    }
    Ok(database_url)
}

#[derive(Debug, Error)]
pub enum DeploymentError {
    #[error("failed to read pilot service config {path}: {source}")]
    ReadConfig {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse pilot service config {path}: {source}")]
    ParseConfig {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("missing required token environment variable {0}")]
    MissingTokenEnv(String),
    #[error("missing required postgres database URL environment variable {0}")]
    MissingDatabaseUrlEnv(String),
    #[error("failed to read token file {path}: {source}")]
    ReadTokenFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to launch token command {command}: {source}")]
    RunTokenCommand {
        command: String,
        #[source]
        source: std::io::Error,
    },
    #[error("token command {command} for principal {principal} exited with code {exit_code:?}: {stderr}")]
    TokenCommandFailed {
        principal: String,
        command: String,
        exit_code: Option<i32>,
        stderr: String,
    },
    #[error("failed to launch postgres database URL command {command}: {source}")]
    RunDatabaseUrlCommand {
        command: String,
        #[source]
        source: std::io::Error,
    },
    #[error("postgres database URL command {command} exited with code {exit_code:?}: {stderr}")]
    DatabaseUrlCommandFailed {
        command: String,
        exit_code: Option<i32>,
        stderr: String,
    },
    #[error("invalid pilot deployment configuration: {0}")]
    Validation(String),
    #[error(transparent)]
    Api(#[from] ApiError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub fn default_audit_log_path(database_path: &Path) -> PathBuf {
    database_path.with_extension("audit.jsonl")
}

pub async fn serve_pilot_http_service(
    resolved: ResolvedPilotServiceConfig,
) -> Result<(), DeploymentError> {
    let listener = tokio::net::TcpListener::bind(&resolved.bind_addr).await?;
    let options = HttpKernelOptions::new()
        .with_auth(resolved.auth.clone())
        .with_audit_log_path(resolved.audit_log_path.clone())
        .with_service_status(resolved.service_status())
        .with_auth_reload_config_path(resolved.config_path.clone());
    match &resolved.storage {
        ResolvedPilotStorage::LegacySqlite { database_path, .. } => {
            let service = SqliteKernelService::open(database_path)?;
            axum::serve(listener, http_router_with_options(service, options)).await?;
        }
        ResolvedPilotStorage::SqliteNamespaces { data_root } => {
            axum::serve(
                listener,
                http_router_with_sqlite_namespaces(data_root.clone(), options),
            )
            .await?;
        }
        ResolvedPilotStorage::PostgresNamespaces {
            database_url,
            schema,
            sidecar_path,
        } => {
            axum::serve(
                listener,
                http_router_with_postgres_namespaces(
                    database_url.clone(),
                    schema.clone(),
                    sidecar_path.clone(),
                    options,
                ),
            )
            .await?;
        }
    }
    Ok(())
}

fn default_config_version() -> String {
    "pilot-v1".into()
}

fn default_schema_version() -> String {
    "v1".into()
}

fn resolve_path(base_dir: &Path, path: &Path) -> PathBuf {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    };
    normalize_path(&joined)
}

fn resolve_token_command(
    config_dir: &Path,
    token_command: &[String],
    principal: &str,
) -> Result<(String, String), DeploymentError> {
    let (program, args) = token_command.split_first().ok_or_else(|| {
        DeploymentError::Validation(format!(
            "pilot auth principal {principal} has an empty token_command"
        ))
    })?;
    let command_path = resolve_command_path(config_dir, program);
    let output = Command::new(command_path)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|source| DeploymentError::RunTokenCommand {
            command: display_command(program, args),
            source,
        })?;
    if !output.status.success() {
        return Err(DeploymentError::TokenCommandFailed {
            principal: principal.to_string(),
            command: display_command(program, args),
            exit_code: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }

    let token = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if token.is_empty() {
        return Err(DeploymentError::Validation(format!(
            "token command {} for principal {} returned an empty token",
            display_command(program, args),
            principal
        )));
    }

    Ok((token, format!("command:{program}")))
}

fn resolve_command_path(config_dir: &Path, program: &str) -> PathBuf {
    let program_path = Path::new(program);
    if program_path.is_absolute()
        || program_path.parent().is_some()
        || program.starts_with('.')
        || program.contains('/')
        || program.contains('\\')
    {
        resolve_path(config_dir, program_path)
    } else {
        program_path.to_path_buf()
    }
}

fn display_command(program: &str, args: &[String]) -> String {
    std::iter::once(program.to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

fn normalize_policy_context(policy_context: Option<PolicyContext>) -> Option<PolicyContext> {
    match policy_context {
        Some(policy_context) if policy_context.is_empty() => None,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        default_audit_log_path, DeploymentError, PilotAuthConfig, PilotServiceConfig,
        PilotStorageConfig, PilotTokenConfig,
    };
    use crate::{AuthScope, NamespaceId, ServiceMode};
    use aether_ast::PolicyContext;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn resolves_token_file_relative_to_config_path() {
        let root = unique_temp_dir("pilot-config");
        let config_dir = root.join("config");
        fs::create_dir_all(&config_dir).expect("create config dir");
        let token_path = config_dir.join("pilot.token");
        fs::write(&token_path, "secret-token\n").expect("write token");

        let config = PilotServiceConfig {
            config_version: "pilot-v1".into(),
            schema_version: "v1".into(),
            service_mode: ServiceMode::SingleNode,
            bind_addr: "127.0.0.1:3000".into(),
            database_path: Some(PathBuf::from("../data/coordination.sqlite")),
            storage: None,
            audit_log_path: None,
            auth: PilotAuthConfig {
                revoked_token_ids: Vec::new(),
                revoked_principal_ids: Vec::new(),
                tokens: vec![PilotTokenConfig {
                    principal: "pilot-operator".into(),
                    principal_id: Some("principal:pilot-operator".into()),
                    token_id: Some("token:pilot-operator".into()),
                    scopes: vec![AuthScope::Query, AuthScope::Explain],
                    policy_context: Some(PolicyContext {
                        capabilities: vec!["executor".into()],
                        visibilities: Vec::new(),
                    }),
                    token: None,
                    token_env: None,
                    token_file: Some(PathBuf::from("pilot.token")),
                    token_command: None,
                    namespaces: Vec::new(),
                    revoked: false,
                }],
            },
        };

        let resolved = config
            .resolve(config_dir.join("pilot-service.json"))
            .expect("resolve config");

        assert_eq!(
            resolved.database_path,
            Some(root.join("data").join("coordination.sqlite"))
        );
        assert_eq!(
            resolved.audit_log_path,
            default_audit_log_path(&root.join("data").join("coordination.sqlite"))
        );
        assert_eq!(resolved.auth.tokens.len(), 1);
        assert_eq!(resolved.auth.tokens[0].token, "secret-token");
        assert_eq!(
            resolved.token_summaries[0].source,
            format!("file:{}", token_path.display())
        );
    }

    #[test]
    fn resolves_v2_sqlite_storage_config_with_default_namespace_binding() {
        let root = unique_temp_dir("pilot-sqlite-v2");
        let config_path = root.join("config").join("pilot-service.json");
        let config = PilotServiceConfig {
            config_version: "pilot-v2".into(),
            schema_version: "v2".into(),
            service_mode: ServiceMode::SingleNode,
            bind_addr: "127.0.0.1:3000".into(),
            database_path: None,
            storage: Some(PilotStorageConfig::Sqlite {
                data_root: PathBuf::from("../data"),
            }),
            audit_log_path: None,
            auth: PilotAuthConfig {
                revoked_token_ids: Vec::new(),
                revoked_principal_ids: Vec::new(),
                tokens: vec![PilotTokenConfig {
                    principal: "pilot-operator".into(),
                    principal_id: Some("principal:pilot-operator".into()),
                    token_id: Some("token:pilot-operator".into()),
                    scopes: vec![AuthScope::Append, AuthScope::Query, AuthScope::Ops],
                    policy_context: None,
                    token: Some("inline".into()),
                    token_env: None,
                    token_file: None,
                    token_command: None,
                    namespaces: Vec::new(),
                    revoked: false,
                }],
            },
        };

        let resolved = config.resolve(config_path).expect("resolve sqlite v2");
        let status = resolved.service_status();
        assert_eq!(resolved.database_path, None);
        assert_eq!(status.storage.backend, "sqlite");
        assert_eq!(status.storage.data_root, Some(root.join("data")));
        assert_eq!(
            status.storage.audit_log_path,
            Some(root.join("data").join("audit.jsonl"))
        );
        assert_eq!(status.principals[0].namespaces, vec!["default"]);
    }

    #[test]
    fn accepts_legacy_pilot_json_database_path_config() {
        let config: PilotServiceConfig = serde_json::from_str(
            r#"{
              "config_version": "pilot-v1",
              "schema_version": "v1",
              "service_mode": "single_node",
              "bind_addr": "127.0.0.1:3000",
              "database_path": "../data/coordination.sqlite",
              "auth": {
                "tokens": [
                  {
                    "principal": "pilot-operator",
                    "scopes": ["append", "query", "ops"],
                    "token": "inline"
                  }
                ]
              }
            }"#,
        )
        .expect("parse legacy pilot config");

        assert_eq!(
            config.database_path,
            Some(PathBuf::from("../data/coordination.sqlite"))
        );
        assert_eq!(config.storage, None);
    }

    #[test]
    fn resolves_v2_postgres_storage_config_without_leaking_database_url() {
        let root = unique_temp_dir("pilot-postgres-v2");
        let config_path = root.join("config").join("pilot-service.json");
        let env_name = format!(
            "AETHER_TEST_DATABASE_URL_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        );
        std::env::set_var(&env_name, "postgres://aether:secret@example.invalid/aether");
        let config = PilotServiceConfig {
            config_version: "pilot-v2".into(),
            schema_version: "v2".into(),
            service_mode: ServiceMode::SingleNode,
            bind_addr: "127.0.0.1:3000".into(),
            database_path: None,
            storage: Some(PilotStorageConfig::Postgres {
                database_url_env: Some(env_name.clone()),
                database_url_command: None,
                schema: Some("partner".into()),
                sidecar_path: PathBuf::from("../data/sidecars.sqlite"),
            }),
            audit_log_path: None,
            auth: PilotAuthConfig {
                revoked_token_ids: Vec::new(),
                revoked_principal_ids: Vec::new(),
                tokens: vec![PilotTokenConfig {
                    principal: "acme-operator".into(),
                    principal_id: Some("principal:acme".into()),
                    token_id: Some("token:acme".into()),
                    scopes: vec![AuthScope::Append, AuthScope::Query, AuthScope::Ops],
                    policy_context: None,
                    token: Some("inline".into()),
                    token_env: None,
                    token_file: None,
                    token_command: None,
                    namespaces: vec![NamespaceId::new("acme").expect("valid namespace")],
                    revoked: false,
                }],
            },
        };

        let resolved = config.resolve(config_path).expect("resolve postgres v2");
        std::env::remove_var(env_name);
        let status = resolved.service_status();
        assert_eq!(resolved.database_path, None);
        assert_eq!(status.storage.backend, "postgres");
        assert_eq!(status.storage.postgres_schema.as_deref(), Some("partner"));
        assert!(status.storage.postgres_url_configured);
        assert_eq!(
            status.storage.sidecar_path,
            Some(root.join("data").join("sidecars.sqlite"))
        );
        let serialized = serde_json::to_string(&status).expect("serialize status");
        assert!(!serialized.contains("secret@example"));
        assert_eq!(status.principals[0].namespaces, vec!["acme"]);
    }

    #[test]
    fn rejects_missing_or_ambiguous_token_sources() {
        let config = PilotServiceConfig {
            config_version: "pilot-v1".into(),
            schema_version: "v1".into(),
            service_mode: ServiceMode::SingleNode,
            bind_addr: "127.0.0.1:3000".into(),
            database_path: Some(PathBuf::from("coordination.sqlite")),
            storage: None,
            audit_log_path: None,
            auth: PilotAuthConfig {
                revoked_token_ids: Vec::new(),
                revoked_principal_ids: Vec::new(),
                tokens: vec![PilotTokenConfig {
                    principal: "pilot-operator".into(),
                    principal_id: Some("principal:pilot-operator".into()),
                    token_id: Some("token:pilot-operator".into()),
                    scopes: vec![AuthScope::Query],
                    policy_context: None,
                    token: Some("inline".into()),
                    token_env: Some("AETHER_TOKEN".into()),
                    token_file: None,
                    token_command: None,
                    namespaces: Vec::new(),
                    revoked: false,
                }],
            },
        };

        let error = config
            .resolve(PathBuf::from("pilot-service.json"))
            .expect_err("ambiguous token source should fail");
        assert!(matches!(
            error,
            DeploymentError::Validation(message)
                if message.contains("exactly one token source")
        ));
    }

    #[test]
    fn resolves_token_from_command() {
        let command = token_command_fixture("command-token");
        let config = PilotServiceConfig {
            config_version: "pilot-v1".into(),
            schema_version: "v1".into(),
            service_mode: ServiceMode::SingleNode,
            bind_addr: "127.0.0.1:3000".into(),
            database_path: Some(PathBuf::from("coordination.sqlite")),
            storage: None,
            audit_log_path: None,
            auth: PilotAuthConfig {
                revoked_token_ids: Vec::new(),
                revoked_principal_ids: Vec::new(),
                tokens: vec![PilotTokenConfig {
                    principal: "pilot-operator".into(),
                    principal_id: Some("principal:pilot-operator".into()),
                    token_id: Some("token:pilot-operator".into()),
                    scopes: vec![AuthScope::Query],
                    policy_context: None,
                    token: None,
                    token_env: None,
                    token_file: None,
                    token_command: Some(command),
                    namespaces: Vec::new(),
                    revoked: false,
                }],
            },
        };

        let resolved = config
            .resolve(PathBuf::from("pilot-service.json"))
            .expect("resolve command token");
        assert_eq!(resolved.auth.tokens[0].token, "command-token");
        assert!(resolved.token_summaries[0].source.starts_with("command:"));
    }

    #[test]
    fn rejects_empty_token_command_output() {
        let config = PilotServiceConfig {
            config_version: "pilot-v1".into(),
            schema_version: "v1".into(),
            service_mode: ServiceMode::SingleNode,
            bind_addr: "127.0.0.1:3000".into(),
            database_path: Some(PathBuf::from("coordination.sqlite")),
            storage: None,
            audit_log_path: None,
            auth: PilotAuthConfig {
                revoked_token_ids: Vec::new(),
                revoked_principal_ids: Vec::new(),
                tokens: vec![PilotTokenConfig {
                    principal: "pilot-operator".into(),
                    principal_id: Some("principal:pilot-operator".into()),
                    token_id: Some("token:pilot-operator".into()),
                    scopes: vec![AuthScope::Query],
                    policy_context: None,
                    token: None,
                    token_env: None,
                    token_file: None,
                    token_command: Some(empty_output_command_fixture()),
                    namespaces: Vec::new(),
                    revoked: false,
                }],
            },
        };

        let error = config
            .resolve(PathBuf::from("pilot-service.json"))
            .expect_err("empty command output should fail");
        assert!(matches!(
            error,
            DeploymentError::Validation(message)
                if message.contains("returned an empty token")
        ));
    }

    #[test]
    fn rejects_missing_token_file_path() {
        let root = unique_temp_dir("pilot-missing-token");
        let config_dir = root.join("config");
        fs::create_dir_all(&config_dir).expect("create config dir");

        let config = PilotServiceConfig {
            config_version: "pilot-v1".into(),
            schema_version: "v1".into(),
            service_mode: ServiceMode::SingleNode,
            bind_addr: "127.0.0.1:3000".into(),
            database_path: Some(PathBuf::from("coordination.sqlite")),
            storage: None,
            audit_log_path: None,
            auth: PilotAuthConfig {
                revoked_token_ids: Vec::new(),
                revoked_principal_ids: Vec::new(),
                tokens: vec![PilotTokenConfig {
                    principal: "pilot-operator".into(),
                    principal_id: Some("principal:pilot-operator".into()),
                    token_id: Some("token:pilot-operator".into()),
                    scopes: vec![AuthScope::Query],
                    policy_context: None,
                    token: None,
                    token_env: None,
                    token_file: Some(PathBuf::from("missing.token")),
                    token_command: None,
                    namespaces: Vec::new(),
                    revoked: false,
                }],
            },
        };

        let error = config
            .resolve(config_dir.join("pilot-service.json"))
            .expect_err("missing token file should fail");
        assert!(matches!(
            error,
            DeploymentError::ReadTokenFile { path, .. }
                if path.ends_with("missing.token")
        ));
    }

    #[test]
    fn rejects_failed_token_command() {
        let config = PilotServiceConfig {
            config_version: "pilot-v1".into(),
            schema_version: "v1".into(),
            service_mode: ServiceMode::SingleNode,
            bind_addr: "127.0.0.1:3000".into(),
            database_path: Some(PathBuf::from("coordination.sqlite")),
            storage: None,
            audit_log_path: None,
            auth: PilotAuthConfig {
                revoked_token_ids: Vec::new(),
                revoked_principal_ids: Vec::new(),
                tokens: vec![PilotTokenConfig {
                    principal: "pilot-operator".into(),
                    principal_id: Some("principal:pilot-operator".into()),
                    token_id: Some("token:pilot-operator".into()),
                    scopes: vec![AuthScope::Query],
                    policy_context: None,
                    token: None,
                    token_env: None,
                    token_file: None,
                    token_command: Some(failing_command_fixture()),
                    namespaces: Vec::new(),
                    revoked: false,
                }],
            },
        };

        let error = config
            .resolve(PathBuf::from("pilot-service.json"))
            .expect_err("failed token command should fail");
        assert!(matches!(
            error,
            DeploymentError::TokenCommandFailed {
                principal,
                stderr,
                ..
            } if principal == "pilot-operator" && stderr.contains("hard failure")
        ));
    }

    fn token_command_fixture(token: &str) -> Vec<String> {
        if cfg!(windows) {
            vec![
                "powershell".into(),
                "-NoProfile".into(),
                "-Command".into(),
                format!("Write-Output '{token}'"),
            ]
        } else {
            vec![
                "sh".into(),
                "-c".into(),
                format!("printf '%s\\n' '{token}'"),
            ]
        }
    }

    fn empty_output_command_fixture() -> Vec<String> {
        if cfg!(windows) {
            vec![
                "powershell".into(),
                "-NoProfile".into(),
                "-Command".into(),
                "Write-Output ''".into(),
            ]
        } else {
            vec!["sh".into(), "-c".into(), "printf ''".into()]
        }
    }

    fn failing_command_fixture() -> Vec<String> {
        if cfg!(windows) {
            vec![
                "powershell".into(),
                "-NoProfile".into(),
                "-Command".into(),
                "Write-Error 'hard failure'; exit 9".into(),
            ]
        } else {
            vec![
                "sh".into(),
                "-c".into(),
                "printf '%s\\n' 'hard failure' >&2; exit 9".into(),
            ]
        }
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("aether-{prefix}-{unique}"))
    }
}
