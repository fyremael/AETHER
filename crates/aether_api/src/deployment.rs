use crate::{
    http_router_with_options, sidecar::sidecar_catalog_path_for_journal, ApiError, AuthScope,
    HttpAccessToken, HttpAuthConfig, HttpKernelOptions, SqliteKernelService,
};
use aether_ast::PolicyContext;
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    path::{Component, Path, PathBuf},
};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PilotServiceConfig {
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
                    principal: token.principal.clone(),
                    scopes: token.scopes.clone(),
                    policy_context: token.policy_context.clone(),
                });
                auth
            });

        Ok(ResolvedPilotServiceConfig {
            bind_addr: self.bind_addr,
            database_path,
            audit_log_path,
            auth,
            token_summaries: resolved_tokens
                .into_iter()
                .map(|token| ResolvedPilotTokenSummary {
                    principal: token.principal,
                    scopes: token.scopes,
                    policy_context: token.policy_context,
                    source: token.source,
                })
                .collect(),
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PilotAuthConfig {
    pub tokens: Vec<PilotTokenConfig>,
}

impl PilotAuthConfig {
    fn resolve(&self, config_dir: &Path) -> Result<Vec<ResolvedPilotToken>, DeploymentError> {
        if self.tokens.is_empty() {
            return Err(DeploymentError::Validation(
                "pilot service auth.tokens must contain at least one token".into(),
            ));
        }
        self.tokens
            .iter()
            .map(|token| token.resolve(config_dir))
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PilotTokenConfig {
    pub principal: String,
    pub scopes: Vec<AuthScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_file: Option<PathBuf>,
}

impl PilotTokenConfig {
    fn resolve(&self, config_dir: &Path) -> Result<ResolvedPilotToken, DeploymentError> {
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
        if sources != 1 {
            return Err(DeploymentError::Validation(format!(
                "pilot auth principal {} must declare exactly one token source (token, token_env, or token_file)",
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
        } else {
            let token_file = resolve_path(
                config_dir,
                self.token_file
                    .as_ref()
                    .expect("token_file source already validated"),
            );
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
        };

        Ok(ResolvedPilotToken {
            principal: self.principal.clone(),
            scopes: self.scopes.clone(),
            policy_context: normalize_policy_context(self.policy_context.clone()),
            token,
            source,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedPilotServiceConfig {
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedPilotTokenSummary {
    pub principal: String,
    pub scopes: Vec<AuthScope>,
    pub policy_context: Option<PolicyContext>,
    pub source: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolvedPilotToken {
    principal: String,
    scopes: Vec<AuthScope>,
    policy_context: Option<PolicyContext>,
    token: String,
    source: String,
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
        .with_audit_log_path(resolved.audit_log_path.clone());
    axum::serve(listener, http_router_with_options(service, options)).await?;
    Ok(())
}

fn resolve_path(base_dir: &Path, path: &Path) -> PathBuf {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    };
    normalize_path(&joined)
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
    use crate::AuthScope;
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
            bind_addr: "127.0.0.1:3000".into(),
            database_path: PathBuf::from("../data/coordination.sqlite"),
            audit_log_path: None,
            auth: PilotAuthConfig {
                tokens: vec![PilotTokenConfig {
                    principal: "pilot-operator".into(),
                    scopes: vec![AuthScope::Query, AuthScope::Explain],
                    policy_context: Some(PolicyContext {
                        capabilities: vec!["executor".into()],
                        visibilities: Vec::new(),
                    }),
                    token: None,
                    token_env: None,
                    token_file: Some(PathBuf::from("pilot.token")),
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
            bind_addr: "127.0.0.1:3000".into(),
            database_path: PathBuf::from("coordination.sqlite"),
            audit_log_path: None,
            auth: PilotAuthConfig {
                tokens: vec![PilotTokenConfig {
                    principal: "pilot-operator".into(),
                    scopes: vec![AuthScope::Query],
                    policy_context: None,
                    token: Some("inline".into()),
                    token_env: Some("AETHER_TOKEN".into()),
                    token_file: None,
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

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("aether-{prefix}-{unique}"))
    }
}
