use crate::{
    http_router_with_options, sidecar::sidecar_catalog_path_for_journal, ApiError, AuthScope,
    HttpAccessToken, HttpAuthConfig, HttpKernelOptions, PrincipalStatusSummary, ServiceMode,
    ServiceStatusResponse, ServiceStatusStorage, SqliteKernelService,
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
    pub database_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audit_log_path: Option<PathBuf>,
    pub auth: PilotAuthConfig,
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

        let database_path = resolve_path(config_dir, &self.database_path);
        let audit_log_path = self
            .audit_log_path
            .map(|path| resolve_path(config_dir, &path))
            .unwrap_or_else(|| default_audit_log_path(&database_path));

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
            database_path,
            audit_log_path,
            auth,
            token_summaries: resolved_tokens
                .into_iter()
                .map(|token| ResolvedPilotTokenSummary {
                    principal: token.principal,
                    principal_id: token.principal_id,
                    token_id: token.token_id,
                    scopes: token.scopes,
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
            let token = fs::read_to_string(&token_file).map_err(|source| {
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
    pub database_path: PathBuf,
    pub audit_log_path: PathBuf,
    pub auth: HttpAuthConfig,
    pub token_summaries: Vec<ResolvedPilotTokenSummary>,
}

impl ResolvedPilotServiceConfig {
    pub fn sidecar_path(&self) -> PathBuf {
        sidecar_catalog_path_for_journal(&self.database_path)
    }

    pub fn service_status(&self) -> ServiceStatusResponse {
        ServiceStatusResponse {
            status: "ok".into(),
            build_version: env!("CARGO_PKG_VERSION").into(),
            config_version: self.config_version.clone(),
            schema_version: self.schema_version.clone(),
            bind_addr: Some(self.bind_addr.clone()),
            service_mode: self.service_mode.clone(),
            storage: ServiceStatusStorage {
                database_path: Some(self.database_path.clone()),
                sidecar_path: Some(self.sidecar_path()),
                audit_log_path: Some(self.audit_log_path.clone()),
                partition_root: None,
            },
            principals: self
                .token_summaries
                .iter()
                .map(|summary| summary.status_summary())
                .collect(),
            replicas: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedPilotTokenSummary {
    pub principal: String,
    pub principal_id: String,
    pub token_id: String,
    pub scopes: Vec<AuthScope>,
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
    policy_context: Option<PolicyContext>,
    token: String,
    source: String,
    revoked: bool,
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
    let service = SqliteKernelService::open(&resolved.database_path)?;
    let listener = tokio::net::TcpListener::bind(&resolved.bind_addr).await?;
    let options = HttpKernelOptions::new()
        .with_auth(resolved.auth.clone())
        .with_audit_log_path(resolved.audit_log_path.clone())
        .with_service_status(resolved.service_status())
        .with_auth_reload_config_path(resolved.config_path.clone());
    axum::serve(listener, http_router_with_options(service, options)).await?;
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
        PilotTokenConfig,
    };
    use crate::{AuthScope, ServiceMode};
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
            database_path: PathBuf::from("../data/coordination.sqlite"),
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
                    revoked: false,
                }],
            },
        };

        let resolved = config
            .resolve(config_dir.join("pilot-service.json"))
            .expect("resolve config");

        assert_eq!(
            resolved.database_path,
            root.join("data").join("coordination.sqlite")
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
    fn rejects_missing_or_ambiguous_token_sources() {
        let config = PilotServiceConfig {
            config_version: "pilot-v1".into(),
            schema_version: "v1".into(),
            service_mode: ServiceMode::SingleNode,
            bind_addr: "127.0.0.1:3000".into(),
            database_path: PathBuf::from("coordination.sqlite"),
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
            database_path: PathBuf::from("coordination.sqlite"),
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
            database_path: PathBuf::from("coordination.sqlite"),
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

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("aether-{prefix}-{unique}"))
    }
}
