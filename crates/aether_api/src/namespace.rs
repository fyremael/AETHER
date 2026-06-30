use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NamespaceId(String);

impl NamespaceId {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err("namespace must not be empty".into());
        }
        if trimmed.len() > 128 {
            return Err("namespace must be at most 128 characters".into());
        }
        if !trimmed
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(
                "namespace may only contain ASCII letters, numbers, '.', '_', or '-'".into(),
            );
        }
        Ok(Self(trimmed.to_string()))
    }

    pub fn default_namespace() -> Self {
        Self("default".into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for NamespaceId {
    fn default() -> Self {
        Self::default_namespace()
    }
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<NamespaceId> for String {
    fn from(value: NamespaceId) -> Self {
        value.0
    }
}
