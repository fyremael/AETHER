use aether_ast::{Datom, ElementId};
use native_tls::{Certificate, Identity, Protocol, TlsConnector};
use postgres::{config::Host, Client, Config as PostgresConfig, NoTls};
use postgres_native_tls::MakeTlsConnector;
use rusqlite::{params, Connection, ErrorCode, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    cell::RefCell,
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};
use thiserror::Error;

pub trait Journal {
    fn append(&mut self, datoms: &[Datom]) -> Result<(), JournalError>;
    fn history(&self) -> Result<Vec<Datom>, JournalError>;
    fn prefix(&self, at: &ElementId) -> Result<Vec<Datom>, JournalError>;
    fn cut(&self) -> Result<JournalCutRef, JournalError> {
        journal_cut(&self.history()?)
    }
    fn append_if_cut(
        &mut self,
        expected: &JournalCutRef,
        datoms: &[Datom],
        draft: &AppendReceiptDraft,
    ) -> Result<ConditionalAppend, JournalError>;
    fn append_receipts(&self) -> Result<Vec<StoredAppendReceipt>, JournalError>;
    fn schema_revisions(&self) -> Result<Vec<StoredSchemaRevision>, JournalError>;
    fn active_schema_revision(&self) -> Result<Option<StoredSchemaRevision>, JournalError>;
    fn history_certifications(&self) -> Result<Vec<StoredHistoryCertification>, JournalError>;
    fn seal_history_certification(
        &mut self,
        certification: &StoredHistoryCertification,
    ) -> Result<(), JournalError>;
    fn register_schema_revision(
        &mut self,
        revision: &StoredSchemaRevision,
    ) -> Result<(), JournalError>;
    fn activate_schema_revision(
        &mut self,
        expected_active_digest: Option<&str>,
        schema_digest: &str,
        expected_cut: &JournalCutRef,
    ) -> Result<StoredSchemaRevision, JournalError>;
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct JournalCutRef {
    pub last_element: Option<ElementId>,
    pub entry_count: u64,
    pub prefix_digest: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendReceiptDraft {
    pub batch_id: String,
    pub schema_version: String,
    pub schema_digest: String,
    pub batch_digest: String,
    pub principal: String,
    pub admission_engine_version: String,
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub schema_ref_was_implicit: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredAppendReceipt {
    pub draft: AppendReceiptDraft,
    pub prior_cut: JournalCutRef,
    pub committed_cut: JournalCutRef,
    pub appended: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConditionalAppend {
    pub receipt: StoredAppendReceipt,
    pub idempotent_replay: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredSchemaRevision {
    pub version: String,
    pub digest: String,
    pub schema_json: String,
    pub predecessor_digest: Option<String>,
    pub predecessor_version: Option<String>,
    pub compatibility: String,
    pub status: String,
}

/// The only PostgreSQL transport modes accepted by the authoritative journal.
///
/// `VerifyFull` is the production default. `DevelopmentPlaintext` is an
/// explicit escape hatch restricted to literal loopback endpoints and Unix
/// sockets; it never falls back from a failed TLS handshake.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresTlsMode {
    #[default]
    VerifyFull,
    VerifyCa,
    DevelopmentPlaintext,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PostgresTlsConfig {
    #[serde(default)]
    pub mode: PostgresTlsMode,
    #[serde(default)]
    pub ca_certificate_paths: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_certificate_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_private_key_path: Option<PathBuf>,
    #[serde(default)]
    pub disable_system_roots: bool,
}

impl PostgresTlsConfig {
    pub fn development_plaintext() -> Self {
        Self {
            mode: PostgresTlsMode::DevelopmentPlaintext,
            ..Self::default()
        }
    }

    pub fn validate(&self, database_url: &str) -> Result<(), JournalError> {
        let config = PostgresConfig::from_str(database_url).map_err(JournalError::Postgres)?;
        let client_pair_complete =
            self.client_certificate_path.is_some() == self.client_private_key_path.is_some();
        if !client_pair_complete {
            return Err(JournalError::InvalidTlsConfiguration(
                "client_certificate_path and client_private_key_path must be configured together"
                    .into(),
            ));
        }
        match self.mode {
            PostgresTlsMode::DevelopmentPlaintext => {
                if !self.ca_certificate_paths.is_empty()
                    || self.client_certificate_path.is_some()
                    || self.disable_system_roots
                {
                    return Err(JournalError::InvalidTlsConfiguration(
                        "development_plaintext cannot declare CA, client identity, or root-store options"
                            .into(),
                    ));
                }
                if !postgres_hosts_are_loopback(&config) {
                    return Err(JournalError::PlaintextPostgresForbidden);
                }
            }
            PostgresTlsMode::VerifyFull | PostgresTlsMode::VerifyCa => {
                if self.disable_system_roots && self.ca_certificate_paths.is_empty() {
                    return Err(JournalError::InvalidTlsConfiguration(
                        "TLS with system roots disabled requires at least one CA certificate"
                            .into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

fn postgres_hosts_are_loopback(config: &PostgresConfig) -> bool {
    let hosts = config.get_hosts();
    hosts.is_empty()
        || hosts.iter().all(|host| match host {
            Host::Tcp(host) => {
                host.eq_ignore_ascii_case("localhost")
                    || host
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|address| address.is_loopback())
            }
            #[cfg(unix)]
            Host::Unix(_) => true,
        })
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredHistoryCertification {
    pub schema_version: String,
    pub schema_digest: String,
    pub cut: JournalCutRef,
    pub status: String,
    pub validation_engine_version: String,
    #[serde(default)]
    pub diagnostics: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_manifest_json: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct JournalSnapshot {
    pub entries: Vec<Datom>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct InMemoryJournal {
    entries: Vec<Datom>,
    receipts: Vec<StoredAppendReceipt>,
    schema_revisions: Vec<StoredSchemaRevision>,
    history_certifications: Vec<StoredHistoryCertification>,
    active_schema_digest: Option<String>,
}

impl InMemoryJournal {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Journal for InMemoryJournal {
    fn append(&mut self, datoms: &[Datom]) -> Result<(), JournalError> {
        let mut batch_ids = BTreeSet::new();
        for datom in datoms {
            if self
                .entries
                .iter()
                .any(|existing| existing.element == datom.element)
                || !batch_ids.insert(datom.element)
            {
                return Err(JournalError::DuplicateElementId(datom.element));
            }
        }

        self.entries.extend(datoms.iter().cloned());
        Ok(())
    }

    fn history(&self) -> Result<Vec<Datom>, JournalError> {
        Ok(self.entries.clone())
    }

    fn prefix(&self, at: &ElementId) -> Result<Vec<Datom>, JournalError> {
        let end = self
            .entries
            .iter()
            .position(|datom| datom.element == *at)
            .ok_or(JournalError::UnknownElementId(*at))?;
        Ok(self.entries[..=end].to_vec())
    }

    fn append_if_cut(
        &mut self,
        expected: &JournalCutRef,
        datoms: &[Datom],
        draft: &AppendReceiptDraft,
    ) -> Result<ConditionalAppend, JournalError> {
        if let Some(existing) = find_idempotent_receipt(&self.receipts, draft)? {
            return Ok(ConditionalAppend {
                receipt: existing,
                idempotent_replay: true,
            });
        }
        if self.active_schema_digest.as_deref() != Some(draft.schema_digest.as_str()) {
            return Err(JournalError::ActiveSchemaChanged {
                expected: draft.schema_digest.clone(),
                actual: self.active_schema_digest.clone(),
            });
        }
        let prior_cut = journal_cut(&self.entries)?;
        if &prior_cut != expected {
            return Err(JournalError::StaleCut {
                expected: expected.clone(),
                actual: prior_cut,
            });
        }
        validate_unique_elements(&self.entries, datoms)?;
        let mut committed = self.entries.clone();
        committed.extend_from_slice(datoms);
        let committed_cut = journal_cut(&committed)?;
        let receipt = StoredAppendReceipt {
            draft: draft.clone(),
            prior_cut,
            committed_cut,
            appended: datoms.len(),
        };
        self.entries = committed;
        self.receipts.push(receipt.clone());
        Ok(ConditionalAppend {
            receipt,
            idempotent_replay: false,
        })
    }

    fn append_receipts(&self) -> Result<Vec<StoredAppendReceipt>, JournalError> {
        Ok(self.receipts.clone())
    }

    fn schema_revisions(&self) -> Result<Vec<StoredSchemaRevision>, JournalError> {
        Ok(self.schema_revisions.clone())
    }

    fn active_schema_revision(&self) -> Result<Option<StoredSchemaRevision>, JournalError> {
        Ok(self.active_schema_digest.as_ref().and_then(|digest| {
            self.schema_revisions
                .iter()
                .find(|revision| &revision.digest == digest)
                .cloned()
        }))
    }

    fn history_certifications(&self) -> Result<Vec<StoredHistoryCertification>, JournalError> {
        Ok(self.history_certifications.clone())
    }

    fn seal_history_certification(
        &mut self,
        certification: &StoredHistoryCertification,
    ) -> Result<(), JournalError> {
        seal_history_certification_record(&mut self.history_certifications, certification)
    }

    fn register_schema_revision(
        &mut self,
        revision: &StoredSchemaRevision,
    ) -> Result<(), JournalError> {
        register_schema_record(&mut self.schema_revisions, revision)
    }

    fn activate_schema_revision(
        &mut self,
        expected_active_digest: Option<&str>,
        schema_digest: &str,
        expected_cut: &JournalCutRef,
    ) -> Result<StoredSchemaRevision, JournalError> {
        if self.active_schema_digest.as_deref() != expected_active_digest {
            return Err(JournalError::StaleSchemaActivation);
        }
        let actual_cut = self.cut()?;
        if &actual_cut != expected_cut {
            return Err(JournalError::StaleCut {
                expected: expected_cut.clone(),
                actual: actual_cut,
            });
        }
        let revision = self
            .schema_revisions
            .iter()
            .find(|revision| revision.digest == schema_digest)
            .cloned()
            .ok_or_else(|| JournalError::UnknownSchemaDigest(schema_digest.into()))?;
        self.active_schema_digest = Some(schema_digest.into());
        Ok(revision)
    }
}

#[derive(Debug)]
pub struct SqliteJournal {
    connection: Connection,
    path: PathBuf,
}

pub struct PostgresJournal {
    client: RefCell<Client>,
    schema: String,
    namespace: String,
}

const POSTGRES_SCHEMA_INIT_LOCK: i64 = 0x4145_5448_4552;

impl std::fmt::Debug for PostgresJournal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresJournal")
            .field("schema", &self.schema)
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl SqliteJournal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, JournalError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let connection = Connection::open(&path)?;
        configure_connection(&connection)?;
        initialize_schema(&connection)?;

        Ok(Self { connection, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Journal for SqliteJournal {
    fn append(&mut self, datoms: &[Datom]) -> Result<(), JournalError> {
        let mut batch_ids = BTreeSet::new();
        for datom in datoms {
            if !batch_ids.insert(datom.element) {
                return Err(JournalError::DuplicateElementId(datom.element));
            }
        }

        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        for datom in datoms {
            let element = element_key(&datom.element);
            let encoded = serde_json::to_string(datom)?;
            let inserted = transaction.execute(
                "INSERT INTO journal_entries (element, datom_json) VALUES (?1, ?2)",
                params![element, encoded],
            );
            if let Err(error) = inserted {
                return Err(map_insert_error(error, datom.element));
            }
        }
        transaction.commit()?;
        Ok(())
    }

    fn history(&self) -> Result<Vec<Datom>, JournalError> {
        read_datoms(
            &self.connection,
            "SELECT datom_json FROM journal_entries ORDER BY seq ASC",
            params![],
        )
    }

    fn prefix(&self, at: &ElementId) -> Result<Vec<Datom>, JournalError> {
        let Some(seq) = self
            .connection
            .query_row(
                "SELECT seq FROM journal_entries WHERE element = ?1",
                params![element_key(at)],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
        else {
            return Err(JournalError::UnknownElementId(*at));
        };

        read_datoms(
            &self.connection,
            "SELECT datom_json FROM journal_entries WHERE seq <= ?1 ORDER BY seq ASC",
            params![seq],
        )
    }

    fn append_if_cut(
        &mut self,
        expected: &JournalCutRef,
        datoms: &[Datom],
        draft: &AppendReceiptDraft,
    ) -> Result<ConditionalAppend, JournalError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(key) = &draft.idempotency_key {
            let existing = transaction
                .query_row(
                    "SELECT receipt_json FROM append_receipts WHERE idempotency_key = ?1",
                    params![key],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(existing) = existing {
                let receipt: StoredAppendReceipt = serde_json::from_str(&existing)?;
                ensure_idempotent_digest(&receipt, draft)?;
                return Ok(ConditionalAppend {
                    receipt,
                    idempotent_replay: true,
                });
            }
        }
        let active_schema = transaction
            .query_row(
                "SELECT active_digest FROM schema_state WHERE singleton = 1",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        if active_schema.as_deref() != Some(draft.schema_digest.as_str()) {
            return Err(JournalError::ActiveSchemaChanged {
                expected: draft.schema_digest.clone(),
                actual: active_schema,
            });
        }
        let prior = read_datoms(
            &transaction,
            "SELECT datom_json FROM journal_entries ORDER BY seq ASC",
            params![],
        )?;
        let prior_cut = journal_cut(&prior)?;
        if &prior_cut != expected {
            return Err(JournalError::StaleCut {
                expected: expected.clone(),
                actual: prior_cut,
            });
        }
        validate_unique_elements(&prior, datoms)?;
        for datom in datoms {
            transaction.execute(
                "INSERT INTO journal_entries (element, datom_json) VALUES (?1, ?2)",
                params![element_key(&datom.element), serde_json::to_string(datom)?],
            )?;
        }
        let mut committed = prior;
        committed.extend_from_slice(datoms);
        let receipt = StoredAppendReceipt {
            draft: draft.clone(),
            prior_cut,
            committed_cut: journal_cut(&committed)?,
            appended: datoms.len(),
        };
        transaction.execute(
            "INSERT INTO append_receipts (batch_id, idempotency_key, receipt_json)
             VALUES (?1, ?2, ?3)",
            params![
                &draft.batch_id,
                &draft.idempotency_key,
                serde_json::to_string(&receipt)?,
            ],
        )?;
        transaction.commit()?;
        Ok(ConditionalAppend {
            receipt,
            idempotent_replay: false,
        })
    }

    #[allow(clippy::let_and_return)]
    fn append_receipts(&self) -> Result<Vec<StoredAppendReceipt>, JournalError> {
        let mut statement = self
            .connection
            .prepare("SELECT receipt_json FROM append_receipts ORDER BY seq ASC")?;
        let receipts = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .map(|row| Ok(serde_json::from_str(&row?)?))
            .collect();
        receipts
    }

    #[allow(clippy::let_and_return)]
    fn schema_revisions(&self) -> Result<Vec<StoredSchemaRevision>, JournalError> {
        let mut statement = self
            .connection
            .prepare("SELECT revision_json FROM schema_revisions ORDER BY seq ASC")?;
        let revisions = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .map(|row| Ok(serde_json::from_str(&row?)?))
            .collect();
        revisions
    }

    fn active_schema_revision(&self) -> Result<Option<StoredSchemaRevision>, JournalError> {
        let encoded = self
            .connection
            .query_row(
                "SELECT revision_json FROM schema_revisions
                 WHERE digest = (SELECT active_digest FROM schema_state WHERE singleton = 1)",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        encoded
            .map(|encoded| serde_json::from_str(&encoded))
            .transpose()
            .map_err(JournalError::from)
    }

    #[allow(clippy::let_and_return)]
    fn history_certifications(&self) -> Result<Vec<StoredHistoryCertification>, JournalError> {
        let mut statement = self
            .connection
            .prepare("SELECT certification_json FROM history_certifications ORDER BY seq ASC")?;
        let certifications = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .map(|row| Ok(serde_json::from_str(&row?)?))
            .collect();
        certifications
    }

    fn seal_history_certification(
        &mut self,
        certification: &StoredHistoryCertification,
    ) -> Result<(), JournalError> {
        let encoded = serde_json::to_string(certification)?;
        let inserted = self.connection.execute(
            "INSERT OR IGNORE INTO history_certifications
             (schema_digest, prefix_digest, certification_json) VALUES (?1, ?2, ?3)",
            params![
                certification.schema_digest,
                certification.cut.prefix_digest,
                encoded,
            ],
        )?;
        if inserted == 0 {
            let existing = self.connection.query_row(
                "SELECT certification_json FROM history_certifications
                 WHERE schema_digest = ?1 AND prefix_digest = ?2",
                params![certification.schema_digest, certification.cut.prefix_digest,],
                |row| row.get::<_, String>(0),
            )?;
            if serde_json::from_str::<StoredHistoryCertification>(&existing)? != *certification {
                return Err(JournalError::HistoryCertificationCollision);
            }
        }
        Ok(())
    }

    fn register_schema_revision(
        &mut self,
        revision: &StoredSchemaRevision,
    ) -> Result<(), JournalError> {
        let inserted = self.connection.execute(
            "INSERT OR IGNORE INTO schema_revisions (digest, revision_json) VALUES (?1, ?2)",
            params![revision.digest, serde_json::to_string(revision)?],
        )?;
        if inserted == 0 {
            let existing = self.connection.query_row(
                "SELECT revision_json FROM schema_revisions WHERE digest = ?1",
                params![revision.digest],
                |row| row.get::<_, String>(0),
            )?;
            if serde_json::from_str::<StoredSchemaRevision>(&existing)? != *revision {
                return Err(JournalError::SchemaDigestCollision(revision.digest.clone()));
            }
        }
        Ok(())
    }

    fn activate_schema_revision(
        &mut self,
        expected_active_digest: Option<&str>,
        schema_digest: &str,
        expected_cut: &JournalCutRef,
    ) -> Result<StoredSchemaRevision, JournalError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active = transaction
            .query_row(
                "SELECT active_digest FROM schema_state WHERE singleton = 1",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        if active.as_deref() != expected_active_digest {
            return Err(JournalError::StaleSchemaActivation);
        }
        let history = read_datoms(
            &transaction,
            "SELECT datom_json FROM journal_entries ORDER BY seq ASC",
            params![],
        )?;
        let actual_cut = journal_cut(&history)?;
        if &actual_cut != expected_cut {
            return Err(JournalError::StaleCut {
                expected: expected_cut.clone(),
                actual: actual_cut,
            });
        }
        let encoded = transaction
            .query_row(
                "SELECT revision_json FROM schema_revisions WHERE digest = ?1",
                params![schema_digest],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| JournalError::UnknownSchemaDigest(schema_digest.into()))?;
        transaction.execute(
            "INSERT INTO schema_state (singleton, active_digest) VALUES (1, ?1)
             ON CONFLICT(singleton) DO UPDATE SET active_digest = excluded.active_digest",
            params![schema_digest],
        )?;
        transaction.commit()?;
        Ok(serde_json::from_str(&encoded)?)
    }
}

impl PostgresJournal {
    /// Opens a production PostgreSQL journal using certificate and hostname
    /// verification against the platform trust store.
    pub fn open(
        database_url: &str,
        schema: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, JournalError> {
        Self::open_with_tls(
            database_url,
            schema,
            namespace,
            &PostgresTlsConfig::default(),
        )
    }

    pub fn open_with_tls(
        database_url: &str,
        schema: impl Into<String>,
        namespace: impl Into<String>,
        tls: &PostgresTlsConfig,
    ) -> Result<Self, JournalError> {
        let schema = validate_pg_identifier(&schema.into())?;
        let namespace = namespace.into();
        tls.validate(database_url)?;
        let mut config = PostgresConfig::from_str(database_url)?;
        let mut client = match tls.mode {
            PostgresTlsMode::DevelopmentPlaintext => {
                config.ssl_mode(postgres::config::SslMode::Disable);
                config.connect(NoTls)?
            }
            PostgresTlsMode::VerifyFull | PostgresTlsMode::VerifyCa => {
                config.ssl_mode(postgres::config::SslMode::Require);
                let connector = postgres_tls_connector(tls)?;
                config.connect(connector)?
            }
        };
        initialize_postgres_schema(&mut client, &schema)?;
        Ok(Self {
            client: RefCell::new(client),
            schema,
            namespace,
        })
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    fn table(&self, table: &str) -> String {
        format!(
            "{}.{}",
            quote_pg_identifier(&self.schema),
            quote_pg_identifier(table)
        )
    }
}

fn postgres_tls_connector(tls: &PostgresTlsConfig) -> Result<MakeTlsConnector, JournalError> {
    let mut builder = TlsConnector::builder();
    builder.min_protocol_version(Some(Protocol::Tlsv12));
    builder.disable_built_in_roots(tls.disable_system_roots);
    if matches!(tls.mode, PostgresTlsMode::VerifyCa) {
        // verify_ca intentionally verifies certificate validity and trust while
        // omitting hostname matching. This is never selected automatically.
        builder.danger_accept_invalid_hostnames(true);
    }
    for path in &tls.ca_certificate_paths {
        let pem = fs::read(path).map_err(|source| JournalError::TlsFile {
            kind: "CA certificate",
            path: path.clone(),
            source,
        })?;
        let certificate =
            Certificate::from_pem(&pem).map_err(|source| JournalError::InvalidTlsMaterial {
                kind: "CA certificate",
                source,
            })?;
        builder.add_root_certificate(certificate);
    }
    if let (Some(certificate_path), Some(private_key_path)) =
        (&tls.client_certificate_path, &tls.client_private_key_path)
    {
        let certificate = fs::read(certificate_path).map_err(|source| JournalError::TlsFile {
            kind: "client certificate",
            path: certificate_path.clone(),
            source,
        })?;
        let private_key = fs::read(private_key_path).map_err(|source| JournalError::TlsFile {
            kind: "client private key",
            path: PathBuf::from("<redacted>"),
            source,
        })?;
        let identity = Identity::from_pkcs8(&certificate, &private_key).map_err(|source| {
            JournalError::InvalidTlsMaterial {
                kind: "client identity",
                source,
            }
        })?;
        builder.identity(identity);
    }
    let connector = builder
        .build()
        .map_err(|source| JournalError::InvalidTlsMaterial {
            kind: "TLS connector",
            source,
        })?;
    Ok(MakeTlsConnector::new(connector))
}

impl Journal for PostgresJournal {
    fn append(&mut self, datoms: &[Datom]) -> Result<(), JournalError> {
        let mut batch_ids = BTreeSet::new();
        for datom in datoms {
            if !batch_ids.insert(datom.element) {
                return Err(JournalError::DuplicateElementId(datom.element));
            }
        }

        let journal_table = self.table("journal_entries");
        let lock_table = self.table("namespace_locks");
        let client = self.client.get_mut();
        let mut transaction = client.transaction()?;
        transaction.execute(
            &format!("INSERT INTO {lock_table} (namespace) VALUES ($1) ON CONFLICT DO NOTHING"),
            &[&self.namespace],
        )?;
        transaction.query_one(
            &format!("SELECT namespace FROM {lock_table} WHERE namespace = $1 FOR UPDATE"),
            &[&self.namespace],
        )?;
        for datom in datoms {
            let encoded = serde_json::to_string(datom)?;
            let inserted = transaction.execute(
                &format!(
                    "INSERT INTO {journal_table} (namespace, element, datom_json) VALUES ($1, $2, $3::text::jsonb)"
                ),
                &[&self.namespace, &element_key(&datom.element), &encoded],
            );
            if let Err(error) = inserted {
                return Err(map_postgres_insert_error(error, datom.element));
            }
        }
        transaction.commit()?;
        Ok(())
    }

    fn history(&self) -> Result<Vec<Datom>, JournalError> {
        let journal_table = self.table("journal_entries");
        let mut client = self.client.borrow_mut();
        read_postgres_datoms(
            &mut *client,
            &format!(
                "SELECT datom_json::text FROM {journal_table} WHERE namespace = $1 ORDER BY seq ASC"
            ),
            &[&self.namespace],
        )
    }

    fn prefix(&self, at: &ElementId) -> Result<Vec<Datom>, JournalError> {
        let journal_table = self.table("journal_entries");
        let mut client = self.client.borrow_mut();
        let row = client.query_opt(
            &format!("SELECT seq FROM {journal_table} WHERE namespace = $1 AND element = $2"),
            &[&self.namespace, &element_key(at)],
        )?;
        let Some(row) = row else {
            return Err(JournalError::UnknownElementId(*at));
        };
        let seq: i64 = row.get(0);
        read_postgres_datoms(
            &mut *client,
            &format!(
                "SELECT datom_json::text FROM {journal_table} WHERE namespace = $1 AND seq <= $2 ORDER BY seq ASC"
            ),
            &[&self.namespace, &seq],
        )
    }

    fn append_if_cut(
        &mut self,
        expected: &JournalCutRef,
        datoms: &[Datom],
        draft: &AppendReceiptDraft,
    ) -> Result<ConditionalAppend, JournalError> {
        let journal_table = self.table("journal_entries");
        let lock_table = self.table("namespace_locks");
        let receipt_table = self.table("append_receipts");
        let state_table = self.table("schema_state");
        let client = self.client.get_mut();
        let mut transaction = client.transaction()?;
        transaction.execute(
            &format!("INSERT INTO {lock_table} (namespace) VALUES ($1) ON CONFLICT DO NOTHING"),
            &[&self.namespace],
        )?;
        transaction.query_one(
            &format!("SELECT namespace FROM {lock_table} WHERE namespace = $1 FOR UPDATE"),
            &[&self.namespace],
        )?;
        if let Some(key) = &draft.idempotency_key {
            if let Some(row) = transaction.query_opt(
                &format!(
                    "SELECT receipt_json::text FROM {receipt_table} WHERE namespace = $1 AND idempotency_key = $2"
                ),
                &[&self.namespace, key],
            )? {
                let encoded: String = row.get(0);
                let receipt: StoredAppendReceipt = serde_json::from_str(&encoded)?;
                ensure_idempotent_digest(&receipt, draft)?;
                return Ok(ConditionalAppend {
                    receipt,
                    idempotent_replay: true,
                });
            }
        }
        let active_schema = transaction
            .query_opt(
                &format!("SELECT active_digest FROM {state_table} WHERE namespace = $1"),
                &[&self.namespace],
            )?
            .and_then(|row| row.get::<_, Option<String>>(0));
        if active_schema.as_deref() != Some(draft.schema_digest.as_str()) {
            return Err(JournalError::ActiveSchemaChanged {
                expected: draft.schema_digest.clone(),
                actual: active_schema,
            });
        }
        let prior = read_postgres_datoms(
            &mut transaction,
            &format!(
                "SELECT datom_json::text FROM {journal_table} WHERE namespace = $1 ORDER BY seq ASC"
            ),
            &[&self.namespace],
        )?;
        let prior_cut = journal_cut(&prior)?;
        if &prior_cut != expected {
            return Err(JournalError::StaleCut {
                expected: expected.clone(),
                actual: prior_cut,
            });
        }
        validate_unique_elements(&prior, datoms)?;
        for datom in datoms {
            let encoded = serde_json::to_string(datom)?;
            transaction.execute(
                &format!(
                    "INSERT INTO {journal_table} (namespace, element, datom_json) VALUES ($1, $2, $3::text::jsonb)"
                ),
                &[&self.namespace, &element_key(&datom.element), &encoded],
            )?;
        }
        let mut committed = prior;
        committed.extend_from_slice(datoms);
        let receipt = StoredAppendReceipt {
            draft: draft.clone(),
            prior_cut,
            committed_cut: journal_cut(&committed)?,
            appended: datoms.len(),
        };
        let receipt_json = serde_json::to_string(&receipt)?;
        transaction.execute(
            &format!(
                "INSERT INTO {receipt_table} (namespace, batch_id, idempotency_key, receipt_json)
                 VALUES ($1, $2, $3, $4::text::jsonb)"
            ),
            &[
                &self.namespace,
                &draft.batch_id,
                &draft.idempotency_key,
                &receipt_json,
            ],
        )?;
        transaction.commit()?;
        Ok(ConditionalAppend {
            receipt,
            idempotent_replay: false,
        })
    }

    fn append_receipts(&self) -> Result<Vec<StoredAppendReceipt>, JournalError> {
        let receipt_table = self.table("append_receipts");
        let rows = self.client.borrow_mut().query(
            &format!(
                "SELECT receipt_json::text FROM {receipt_table} WHERE namespace = $1 ORDER BY seq ASC"
            ),
            &[&self.namespace],
        )?;
        rows.into_iter()
            .map(|row| {
                let encoded: String = row.get(0);
                serde_json::from_str(&encoded).map_err(JournalError::from)
            })
            .collect()
    }

    fn schema_revisions(&self) -> Result<Vec<StoredSchemaRevision>, JournalError> {
        let table = self.table("schema_revisions");
        let rows = self.client.borrow_mut().query(
            &format!(
                "SELECT revision_json::text FROM {table} WHERE namespace = $1 ORDER BY seq ASC"
            ),
            &[&self.namespace],
        )?;
        rows.into_iter()
            .map(|row| {
                let encoded: String = row.get(0);
                serde_json::from_str(&encoded).map_err(JournalError::from)
            })
            .collect()
    }

    fn active_schema_revision(&self) -> Result<Option<StoredSchemaRevision>, JournalError> {
        let revisions = self.table("schema_revisions");
        let state = self.table("schema_state");
        let row = self.client.borrow_mut().query_opt(
            &format!(
                "SELECT r.revision_json::text FROM {revisions} r
                 JOIN {state} s ON s.namespace = r.namespace AND s.active_digest = r.digest
                 WHERE s.namespace = $1"
            ),
            &[&self.namespace],
        )?;
        row.map(|row| {
            let encoded: String = row.get(0);
            serde_json::from_str(&encoded).map_err(JournalError::from)
        })
        .transpose()
    }

    fn history_certifications(&self) -> Result<Vec<StoredHistoryCertification>, JournalError> {
        let table = self.table("history_certifications");
        let rows = self.client.borrow_mut().query(
            &format!(
                "SELECT certification_json::text FROM {table}
                 WHERE namespace = $1 ORDER BY seq ASC"
            ),
            &[&self.namespace],
        )?;
        rows.into_iter()
            .map(|row| {
                let encoded: String = row.get(0);
                serde_json::from_str(&encoded).map_err(JournalError::from)
            })
            .collect()
    }

    fn seal_history_certification(
        &mut self,
        certification: &StoredHistoryCertification,
    ) -> Result<(), JournalError> {
        let table = self.table("history_certifications");
        let encoded = serde_json::to_string(certification)?;
        let client = self.client.get_mut();
        let inserted = client.execute(
            &format!(
                "INSERT INTO {table}
                 (namespace, schema_digest, prefix_digest, certification_json)
                 VALUES ($1, $2, $3, $4::text::jsonb) ON CONFLICT DO NOTHING"
            ),
            &[
                &self.namespace,
                &certification.schema_digest,
                &certification.cut.prefix_digest,
                &encoded,
            ],
        )?;
        if inserted == 0 {
            let row = client.query_one(
                &format!(
                    "SELECT certification_json::text FROM {table}
                     WHERE namespace = $1 AND schema_digest = $2 AND prefix_digest = $3"
                ),
                &[
                    &self.namespace,
                    &certification.schema_digest,
                    &certification.cut.prefix_digest,
                ],
            )?;
            let existing: String = row.get(0);
            if serde_json::from_str::<StoredHistoryCertification>(&existing)? != *certification {
                return Err(JournalError::HistoryCertificationCollision);
            }
        }
        Ok(())
    }

    fn register_schema_revision(
        &mut self,
        revision: &StoredSchemaRevision,
    ) -> Result<(), JournalError> {
        let table = self.table("schema_revisions");
        let encoded = serde_json::to_string(revision)?;
        let client = self.client.get_mut();
        let inserted = client.execute(
            &format!(
                "INSERT INTO {table} (namespace, digest, revision_json)
                 VALUES ($1, $2, $3::text::jsonb) ON CONFLICT DO NOTHING"
            ),
            &[&self.namespace, &revision.digest, &encoded],
        )?;
        if inserted == 0 {
            let row = client.query_one(
                &format!(
                    "SELECT revision_json::text FROM {table} WHERE namespace = $1 AND digest = $2"
                ),
                &[&self.namespace, &revision.digest],
            )?;
            let existing: String = row.get(0);
            if serde_json::from_str::<StoredSchemaRevision>(&existing)? != *revision {
                return Err(JournalError::SchemaDigestCollision(revision.digest.clone()));
            }
        }
        Ok(())
    }

    fn activate_schema_revision(
        &mut self,
        expected_active_digest: Option<&str>,
        schema_digest: &str,
        expected_cut: &JournalCutRef,
    ) -> Result<StoredSchemaRevision, JournalError> {
        let lock_table = self.table("namespace_locks");
        let revisions = self.table("schema_revisions");
        let state = self.table("schema_state");
        let journal_table = self.table("journal_entries");
        let client = self.client.get_mut();
        let mut transaction = client.transaction()?;
        transaction.execute(
            &format!("INSERT INTO {lock_table} (namespace) VALUES ($1) ON CONFLICT DO NOTHING"),
            &[&self.namespace],
        )?;
        transaction.query_one(
            &format!("SELECT namespace FROM {lock_table} WHERE namespace = $1 FOR UPDATE"),
            &[&self.namespace],
        )?;
        let active = transaction
            .query_opt(
                &format!("SELECT active_digest FROM {state} WHERE namespace = $1"),
                &[&self.namespace],
            )?
            .and_then(|row| row.get::<_, Option<String>>(0));
        if active.as_deref() != expected_active_digest {
            return Err(JournalError::StaleSchemaActivation);
        }
        let history = read_postgres_datoms(
            &mut transaction,
            &format!(
                "SELECT datom_json::text FROM {journal_table}
                 WHERE namespace = $1 ORDER BY seq ASC"
            ),
            &[&self.namespace],
        )?;
        let actual_cut = journal_cut(&history)?;
        if &actual_cut != expected_cut {
            return Err(JournalError::StaleCut {
                expected: expected_cut.clone(),
                actual: actual_cut,
            });
        }
        let row = transaction
            .query_opt(
                &format!(
                    "SELECT revision_json::text FROM {revisions} WHERE namespace = $1 AND digest = $2"
                ),
                &[&self.namespace, &schema_digest],
            )?
            .ok_or_else(|| JournalError::UnknownSchemaDigest(schema_digest.into()))?;
        let encoded: String = row.get(0);
        transaction.execute(
            &format!(
                "INSERT INTO {state} (namespace, active_digest) VALUES ($1, $2)
                 ON CONFLICT(namespace) DO UPDATE SET active_digest = EXCLUDED.active_digest"
            ),
            &[&self.namespace, &schema_digest],
        )?;
        transaction.commit()?;
        Ok(serde_json::from_str(&encoded)?)
    }
}

fn configure_connection(connection: &Connection) -> Result<(), JournalError> {
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    connection.pragma_update(None, "busy_timeout", 5_000)?;
    Ok(())
}

fn initialize_schema(connection: &Connection) -> Result<(), JournalError> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS journal_entries (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            element TEXT NOT NULL UNIQUE,
            datom_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS journal_entries_by_seq
            ON journal_entries(seq);
        CREATE TABLE IF NOT EXISTS append_receipts (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL UNIQUE,
            idempotency_key TEXT UNIQUE,
            receipt_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS schema_revisions (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            digest TEXT NOT NULL UNIQUE,
            revision_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS schema_state (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            active_digest TEXT,
            FOREIGN KEY(active_digest) REFERENCES schema_revisions(digest)
        );
        CREATE TABLE IF NOT EXISTS history_certifications (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            schema_digest TEXT NOT NULL,
            prefix_digest TEXT NOT NULL,
            certification_json TEXT NOT NULL,
            UNIQUE(schema_digest, prefix_digest)
        );
        ",
    )?;
    Ok(())
}

fn initialize_postgres_schema(client: &mut Client, schema: &str) -> Result<(), JournalError> {
    let schema = quote_pg_identifier(schema);
    let journal_table = format!("{schema}.{}", quote_pg_identifier("journal_entries"));
    let lock_table = format!("{schema}.{}", quote_pg_identifier("namespace_locks"));
    let receipt_table = format!("{schema}.{}", quote_pg_identifier("append_receipts"));
    let schema_revisions = format!("{schema}.{}", quote_pg_identifier("schema_revisions"));
    let schema_state = format!("{schema}.{}", quote_pg_identifier("schema_state"));
    let history_certifications =
        format!("{schema}.{}", quote_pg_identifier("history_certifications"));
    let mut transaction = client.transaction()?;
    transaction.execute(
        "SELECT pg_advisory_xact_lock($1)",
        &[&POSTGRES_SCHEMA_INIT_LOCK],
    )?;
    transaction.batch_execute(&format!(
        "
        CREATE SCHEMA IF NOT EXISTS {schema};
        CREATE TABLE IF NOT EXISTS {journal_table} (
            seq BIGSERIAL PRIMARY KEY,
            namespace TEXT NOT NULL,
            element TEXT NOT NULL,
            datom_json JSONB NOT NULL,
            UNIQUE(namespace, element)
        );
        CREATE INDEX IF NOT EXISTS journal_entries_namespace_seq
            ON {journal_table}(namespace, seq);
        CREATE TABLE IF NOT EXISTS {lock_table} (
            namespace TEXT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS {receipt_table} (
            seq BIGSERIAL PRIMARY KEY,
            namespace TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            idempotency_key TEXT,
            receipt_json JSONB NOT NULL,
            UNIQUE(namespace, batch_id),
            UNIQUE(namespace, idempotency_key)
        );
        CREATE TABLE IF NOT EXISTS {schema_revisions} (
            seq BIGSERIAL PRIMARY KEY,
            namespace TEXT NOT NULL,
            digest TEXT NOT NULL,
            revision_json JSONB NOT NULL,
            UNIQUE(namespace, digest)
        );
        CREATE TABLE IF NOT EXISTS {schema_state} (
            namespace TEXT PRIMARY KEY,
            active_digest TEXT
        );
        CREATE TABLE IF NOT EXISTS {history_certifications} (
            seq BIGSERIAL PRIMARY KEY,
            namespace TEXT NOT NULL,
            schema_digest TEXT NOT NULL,
            prefix_digest TEXT NOT NULL,
            certification_json JSONB NOT NULL,
            UNIQUE(namespace, schema_digest, prefix_digest)
        );
        "
    ))?;
    transaction.commit()?;
    Ok(())
}

fn read_datoms<P>(connection: &Connection, sql: &str, params: P) -> Result<Vec<Datom>, JournalError>
where
    P: rusqlite::Params,
{
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map(params, |row| row.get::<_, String>(0))?;

    let mut datoms = Vec::new();
    for row in rows {
        datoms.push(serde_json::from_str(&row?)?);
    }
    Ok(datoms)
}

fn read_postgres_datoms<C: postgres::GenericClient>(
    client: &mut C,
    sql: &str,
    params: &[&(dyn postgres::types::ToSql + Sync)],
) -> Result<Vec<Datom>, JournalError> {
    let rows = client.query(sql, params)?;
    rows.into_iter()
        .map(|row| {
            let json: String = row.get(0);
            serde_json::from_str(&json).map_err(JournalError::from)
        })
        .collect()
}

fn element_key(element: &ElementId) -> String {
    element.0.to_string()
}

fn map_insert_error(error: rusqlite::Error, element: ElementId) -> JournalError {
    match error {
        rusqlite::Error::SqliteFailure(details, _)
            if details.code == ErrorCode::ConstraintViolation =>
        {
            JournalError::DuplicateElementId(element)
        }
        other => JournalError::Sqlite(other),
    }
}

fn map_postgres_insert_error(error: postgres::Error, element: ElementId) -> JournalError {
    match error.code() {
        Some(&postgres::error::SqlState::UNIQUE_VIOLATION) => {
            JournalError::DuplicateElementId(element)
        }
        _ => JournalError::Postgres(error),
    }
}

fn validate_pg_identifier(value: &str) -> Result<String, JournalError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_digit())
    {
        return Err(JournalError::InvalidPostgresIdentifier(value.into()));
    }
    Ok(value.into())
}

fn quote_pg_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn journal_cut(entries: &[Datom]) -> Result<JournalCutRef, JournalError> {
    let encoded = serde_json::to_vec(entries)?;
    let mut hasher = Sha256::new();
    hasher.update(b"aether-journal-cut-v1");
    hasher.update((encoded.len() as u64).to_be_bytes());
    hasher.update(encoded);
    Ok(JournalCutRef {
        last_element: entries.last().map(|datom| datom.element),
        entry_count: entries.len() as u64,
        prefix_digest: hex_encode(&hasher.finalize()),
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn validate_unique_elements(history: &[Datom], datoms: &[Datom]) -> Result<(), JournalError> {
    let mut known = history
        .iter()
        .map(|datom| datom.element)
        .collect::<BTreeSet<_>>();
    for datom in datoms {
        if !known.insert(datom.element) {
            return Err(JournalError::DuplicateElementId(datom.element));
        }
    }
    Ok(())
}

fn find_idempotent_receipt(
    receipts: &[StoredAppendReceipt],
    draft: &AppendReceiptDraft,
) -> Result<Option<StoredAppendReceipt>, JournalError> {
    let Some(key) = &draft.idempotency_key else {
        return Ok(None);
    };
    let existing = receipts
        .iter()
        .find(|receipt| receipt.draft.idempotency_key.as_ref() == Some(key))
        .cloned();
    if let Some(existing) = &existing {
        ensure_idempotent_digest(existing, draft)?;
    }
    Ok(existing)
}

fn ensure_idempotent_digest(
    existing: &StoredAppendReceipt,
    draft: &AppendReceiptDraft,
) -> Result<(), JournalError> {
    if existing.draft.batch_digest == draft.batch_digest
        && existing.draft.schema_digest == draft.schema_digest
    {
        Ok(())
    } else {
        Err(JournalError::IdempotencyConflict(
            draft.idempotency_key.clone().unwrap_or_default(),
        ))
    }
}

fn register_schema_record(
    records: &mut Vec<StoredSchemaRevision>,
    revision: &StoredSchemaRevision,
) -> Result<(), JournalError> {
    if let Some(existing) = records
        .iter()
        .find(|existing| existing.digest == revision.digest)
    {
        if existing == revision {
            return Ok(());
        }
        return Err(JournalError::SchemaDigestCollision(revision.digest.clone()));
    }
    records.push(revision.clone());
    Ok(())
}

fn seal_history_certification_record(
    records: &mut Vec<StoredHistoryCertification>,
    certification: &StoredHistoryCertification,
) -> Result<(), JournalError> {
    if let Some(existing) = records.iter().find(|existing| {
        existing.schema_digest == certification.schema_digest
            && existing.cut.prefix_digest == certification.cut.prefix_digest
    }) {
        if existing == certification {
            return Ok(());
        }
        return Err(JournalError::HistoryCertificationCollision);
    }
    records.push(certification.clone());
    Ok(())
}

#[derive(Debug, Error)]
pub enum JournalError {
    #[error("duplicate element id {0}")]
    DuplicateElementId(ElementId),
    #[error("unknown element id {0}")]
    UnknownElementId(ElementId),
    #[error("stale journal cut: expected {expected:?}, actual {actual:?}")]
    StaleCut {
        expected: JournalCutRef,
        actual: JournalCutRef,
    },
    #[error("idempotency key {0} was reused for a different batch")]
    IdempotencyConflict(String),
    #[error("schema activation precondition is stale")]
    StaleSchemaActivation,
    #[error("unknown schema digest {0}")]
    UnknownSchemaDigest(String),
    #[error("schema digest collision for {0}")]
    SchemaDigestCollision(String),
    #[error("active schema changed: expected {expected}, actual {actual:?}")]
    ActiveSchemaChanged {
        expected: String,
        actual: Option<String>,
    },
    #[error("history certification identity collision")]
    HistoryCertificationCollision,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Postgres(#[from] postgres::Error),
    #[error("invalid PostgreSQL TLS configuration: {0}")]
    InvalidTlsConfiguration(String),
    #[error(
        "plaintext PostgreSQL is development-only and restricted to literal loopback endpoints"
    )]
    PlaintextPostgresForbidden,
    #[error("failed to read {kind} at {path}: {source}")]
    TlsFile {
        kind: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid {kind}: {source}")]
    InvalidTlsMaterial {
        kind: &'static str,
        #[source]
        source: native_tls::Error,
    },
    #[error("invalid postgres identifier {0}")]
    InvalidPostgresIdentifier(String),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::{
        AppendReceiptDraft, InMemoryJournal, Journal, JournalError, PostgresJournal,
        PostgresTlsConfig, PostgresTlsMode, SqliteJournal, StoredSchemaRevision,
    };

    #[test]
    fn postgres_tls_defaults_to_verify_full() {
        let tls = PostgresTlsConfig::default();
        assert_eq!(tls.mode, PostgresTlsMode::VerifyFull);
        tls.validate("postgres://aether@example.invalid/aether")
            .expect("production TLS configuration");
    }

    #[test]
    fn development_plaintext_is_restricted_to_literal_loopback() {
        let tls = PostgresTlsConfig::development_plaintext();
        tls.validate("postgres://aether@127.0.0.1/aether?sslmode=require")
            .expect("explicit loopback development mode");
        tls.validate("postgres://aether@localhost/aether")
            .expect("localhost development mode");
        assert!(matches!(
            tls.validate("postgres://aether@db.internal/aether?sslmode=disable"),
            Err(JournalError::PlaintextPostgresForbidden)
        ));
    }

    #[test]
    fn postgres_tls_rejects_partial_client_identity_and_empty_private_roots() {
        let partial_identity = PostgresTlsConfig {
            client_certificate_path: Some(PathBuf::from("client.pem")),
            ..PostgresTlsConfig::default()
        };
        assert!(matches!(
            partial_identity.validate("postgres://aether@example.invalid/aether"),
            Err(JournalError::InvalidTlsConfiguration(message))
                if message.contains("configured together")
        ));

        let no_roots = PostgresTlsConfig {
            disable_system_roots: true,
            ..PostgresTlsConfig::default()
        };
        assert!(matches!(
            no_roots.validate("postgres://aether@example.invalid/aether"),
            Err(JournalError::InvalidTlsConfiguration(message))
                if message.contains("at least one CA")
        ));
    }

    #[test]
    fn verify_ca_and_two_ca_rotation_are_explicit_valid_modes() {
        let tls = PostgresTlsConfig {
            mode: PostgresTlsMode::VerifyCa,
            ca_certificate_paths: vec![PathBuf::from("old-ca.pem"), PathBuf::from("new-ca.pem")],
            disable_system_roots: true,
            ..PostgresTlsConfig::default()
        };
        tls.validate("postgres://aether@example.invalid/aether")
            .expect("two-CA rotation configuration");
        assert_eq!(tls.ca_certificate_paths.len(), 2);
    }

    #[test]
    fn private_key_read_errors_are_redacted() {
        let error = JournalError::TlsFile {
            kind: "client private key",
            path: PathBuf::from("<redacted>"),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "missing"),
        };
        let rendered = error.to_string();
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("secret-client-key.pem"));
    }
    use aether_ast::{
        AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, ReplicaId, Value,
    };
    use std::{
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Barrier,
        },
        thread,
        time::{SystemTime, UNIX_EPOCH},
    };

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

    fn sample_datom(element: u64, value: &str) -> Datom {
        Datom {
            entity: EntityId::new(1),
            attribute: AttributeId::new(2),
            value: Value::String(value.into()),
            op: OperationKind::Assert,
            element: ElementId::new(element),
            replica: ReplicaId::new(1),
            causal_context: Default::default(),
            provenance: DatomProvenance::default(),
            policy: None,
        }
    }

    fn draft(batch: &str, digest: &str, idempotency_key: Option<&str>) -> AppendReceiptDraft {
        AppendReceiptDraft {
            batch_id: batch.into(),
            schema_version: "v1".into(),
            schema_digest: "schema-digest".into(),
            batch_digest: digest.into(),
            principal: "test".into(),
            admission_engine_version: "admission-v1".into(),
            idempotency_key: idempotency_key.map(str::to_owned),
            schema_ref_was_implicit: false,
        }
    }

    fn stored_schema(version: &str, digest: &str) -> StoredSchemaRevision {
        StoredSchemaRevision {
            version: version.into(),
            digest: digest.into(),
            schema_json: "{}".into(),
            predecessor_digest: None,
            predecessor_version: None,
            compatibility: "exact".into(),
            status: "registered".into(),
        }
    }

    fn conditional_append_contract(journal: &mut impl Journal) {
        let schema = StoredSchemaRevision {
            version: "v1".into(),
            digest: "schema-digest".into(),
            schema_json: "{}".into(),
            predecessor_digest: None,
            predecessor_version: None,
            compatibility: "exact".into(),
            status: "registered".into(),
        };
        journal
            .register_schema_revision(&schema)
            .expect("register schema");
        let initial = journal.cut().expect("initial cut");
        journal
            .activate_schema_revision(None, "schema-digest", &initial)
            .expect("activate schema");
        let committed = journal
            .append_if_cut(
                &initial,
                &[sample_datom(1, "first")],
                &draft("batch-1", "digest-1", Some("retry-1")),
            )
            .expect("conditional append");
        assert!(!committed.idempotent_replay);
        assert_eq!(committed.receipt.appended, 1);
        assert_eq!(journal.history().expect("history").len(), 1);
        assert_eq!(journal.append_receipts().expect("receipts").len(), 1);

        let replay = journal
            .append_if_cut(
                &initial,
                &[sample_datom(1, "first")],
                &draft("batch-retry", "digest-1", Some("retry-1")),
            )
            .expect("idempotent replay");
        assert!(replay.idempotent_replay);
        assert_eq!(journal.history().expect("history").len(), 1);
        assert_eq!(journal.append_receipts().expect("receipts").len(), 1);

        assert!(matches!(
            journal.append_if_cut(
                &initial,
                &[sample_datom(2, "different")],
                &draft("batch-conflict", "digest-2", Some("retry-1")),
            ),
            Err(JournalError::IdempotencyConflict(key)) if key == "retry-1"
        ));
        assert!(matches!(
            journal.append_if_cut(
                &initial,
                &[sample_datom(2, "stale")],
                &draft("batch-stale", "digest-stale", None),
            ),
            Err(JournalError::StaleCut { .. })
        ));
        assert_eq!(journal.history().expect("history").len(), 1);
        assert_eq!(journal.append_receipts().expect("receipts").len(), 1);
    }

    #[test]
    fn in_memory_conditional_append_is_atomic_and_idempotent() {
        conditional_append_contract(&mut InMemoryJournal::new());
    }

    #[test]
    fn sqlite_conditional_append_is_atomic_durable_and_idempotent() {
        let temp = TestDbPath::new("conditional");
        {
            let mut journal = SqliteJournal::open(temp.path()).expect("open sqlite journal");
            conditional_append_contract(&mut journal);
        }
        let journal = SqliteJournal::open(temp.path()).expect("reopen sqlite journal");
        assert_eq!(journal.history().expect("history").len(), 1);
        assert_eq!(journal.append_receipts().expect("receipts").len(), 1);
    }

    #[test]
    fn sqlite_append_and_schema_activation_are_linearizable() {
        let temp = TestDbPath::new("append-activation-race");
        let initial = {
            let mut journal = SqliteJournal::open(temp.path()).expect("open sqlite journal");
            journal
                .register_schema_revision(&stored_schema("v1", "schema-digest"))
                .expect("register v1");
            journal
                .register_schema_revision(&stored_schema("v2", "schema-digest-v2"))
                .expect("register v2");
            let initial = journal.cut().expect("initial cut");
            journal
                .activate_schema_revision(None, "schema-digest", &initial)
                .expect("activate v1");
            initial
        };
        let barrier = Arc::new(Barrier::new(3));
        let append_path = temp.path().to_path_buf();
        let append_cut = initial.clone();
        let append_barrier = barrier.clone();
        let append = thread::spawn(move || {
            let mut journal = SqliteJournal::open(append_path).expect("open append connection");
            append_barrier.wait();
            journal.append_if_cut(
                &append_cut,
                &[sample_datom(1, "winner")],
                &draft("race-batch", "race-digest", None),
            )
        });
        let activation_path = temp.path().to_path_buf();
        let activation_cut = initial;
        let activation_barrier = barrier.clone();
        let activation = thread::spawn(move || {
            let mut journal =
                SqliteJournal::open(activation_path).expect("open activation connection");
            activation_barrier.wait();
            journal.activate_schema_revision(
                Some("schema-digest"),
                "schema-digest-v2",
                &activation_cut,
            )
        });
        barrier.wait();
        let append = append.join().expect("append thread");
        let activation = activation.join().expect("activation thread");
        assert_ne!(append.is_ok(), activation.is_ok());
        assert!(matches!(
            (&append, &activation),
            (Ok(_), Err(JournalError::StaleCut { .. }))
                | (Err(JournalError::ActiveSchemaChanged { .. }), Ok(_))
        ));
        let journal = SqliteJournal::open(temp.path()).expect("reopen race journal");
        assert_eq!(
            journal.history().expect("history").len(),
            usize::from(append.is_ok())
        );
        assert_eq!(
            journal.append_receipts().expect("receipts").len(),
            usize::from(append.is_ok())
        );
    }

    #[test]
    fn append_preserves_order_and_history() {
        let mut journal = InMemoryJournal::new();
        journal
            .append(&[sample_datom(1, "a"), sample_datom(2, "b")])
            .expect("append entries");

        let history = journal.history().expect("history");
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].element, ElementId::new(1));
        assert_eq!(history[1].element, ElementId::new(2));
    }

    #[test]
    fn append_rejects_duplicates_without_partial_writes() {
        let mut journal = InMemoryJournal::new();
        journal
            .append(&[sample_datom(1, "seed")])
            .expect("append seed");

        let duplicate = journal.append(&[sample_datom(2, "next"), sample_datom(2, "dupe")]);
        assert!(matches!(
            duplicate,
            Err(JournalError::DuplicateElementId(id)) if id == ElementId::new(2)
        ));

        let history = journal.history().expect("history");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].element, ElementId::new(1));
    }

    #[test]
    fn prefix_returns_inclusive_journal_prefix() {
        let mut journal = InMemoryJournal::new();
        journal
            .append(&[
                sample_datom(1, "a"),
                sample_datom(2, "b"),
                sample_datom(3, "c"),
            ])
            .expect("append entries");

        let prefix = journal.prefix(&ElementId::new(2)).expect("prefix");
        assert_eq!(prefix.len(), 2);
        assert_eq!(prefix[0].element, ElementId::new(1));
        assert_eq!(prefix[1].element, ElementId::new(2));
    }

    #[test]
    fn prefix_reports_unknown_elements() {
        let journal = InMemoryJournal::new();
        assert!(matches!(
            journal.prefix(&ElementId::new(9)),
            Err(JournalError::UnknownElementId(id)) if id == ElementId::new(9)
        ));
    }

    #[test]
    fn sqlite_journal_replays_history_after_restart() {
        let temp = TestDbPath::new("history");
        {
            let mut journal = SqliteJournal::open(temp.path()).expect("open sqlite journal");
            journal
                .append(&[
                    sample_datom(1, "alpha"),
                    sample_datom(3, "beta"),
                    sample_datom(9, "gamma"),
                ])
                .expect("append sqlite entries");
        }

        let journal = SqliteJournal::open(temp.path()).expect("reopen sqlite journal");
        let history = journal.history().expect("history");
        assert_eq!(
            history
                .iter()
                .map(|datom| datom.element.0)
                .collect::<Vec<_>>(),
            vec![1, 3, 9]
        );
    }

    #[test]
    fn sqlite_journal_prefix_is_inclusive_by_append_order() {
        let temp = TestDbPath::new("prefix");
        let mut journal = SqliteJournal::open(temp.path()).expect("open sqlite journal");
        journal
            .append(&[
                sample_datom(10, "first"),
                sample_datom(3, "second"),
                sample_datom(7, "third"),
            ])
            .expect("append sqlite entries");

        let prefix = journal.prefix(&ElementId::new(3)).expect("prefix");
        assert_eq!(
            prefix
                .iter()
                .map(|datom| datom.element.0)
                .collect::<Vec<_>>(),
            vec![10, 3]
        );
    }

    #[test]
    fn sqlite_journal_rejects_duplicates_without_partial_writes() {
        let temp = TestDbPath::new("duplicates");
        let mut journal = SqliteJournal::open(temp.path()).expect("open sqlite journal");
        journal
            .append(&[sample_datom(1, "seed")])
            .expect("append seed");

        let duplicate = journal.append(&[sample_datom(2, "next"), sample_datom(2, "dupe")]);
        assert!(matches!(
            duplicate,
            Err(JournalError::DuplicateElementId(id)) if id == ElementId::new(2)
        ));

        let history = journal.history().expect("history");
        assert_eq!(
            history
                .iter()
                .map(|datom| datom.element.0)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn sqlite_journal_detects_existing_duplicate_elements() {
        let temp = TestDbPath::new("existing-duplicate");
        let mut journal = SqliteJournal::open(temp.path()).expect("open sqlite journal");
        journal
            .append(&[sample_datom(1, "seed")])
            .expect("append seed");

        let duplicate = journal.append(&[sample_datom(2, "next"), sample_datom(1, "dupe")]);
        assert!(matches!(
            duplicate,
            Err(JournalError::DuplicateElementId(id)) if id == ElementId::new(1)
        ));

        let history = journal.history().expect("history");
        assert_eq!(
            history
                .iter()
                .map(|datom| datom.element.0)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn postgres_journal_replays_history_after_restart_when_configured() {
        let Some(database_url) = postgres_test_url() else {
            return;
        };
        let namespace = unique_postgres_namespace("restart");
        {
            let mut journal = open_test_postgres(&database_url, "aether_test", &namespace)
                .expect("open postgres journal");
            journal
                .append(&[
                    sample_datom(1, "alpha"),
                    sample_datom(3, "beta"),
                    sample_datom(9, "gamma"),
                ])
                .expect("append postgres entries");
        }

        let journal = open_test_postgres(&database_url, "aether_test", &namespace)
            .expect("reopen postgres journal");
        let history = journal.history().expect("history");
        assert_eq!(
            history
                .iter()
                .map(|datom| datom.element.0)
                .collect::<Vec<_>>(),
            vec![1, 3, 9]
        );
    }

    #[test]
    fn postgres_journal_scopes_duplicates_by_namespace_when_configured() {
        let Some(database_url) = postgres_test_url() else {
            return;
        };
        let left_namespace = unique_postgres_namespace("left");
        let right_namespace = unique_postgres_namespace("right");
        let mut left = open_test_postgres(&database_url, "aether_test", left_namespace)
            .expect("open left postgres journal");
        let mut right = open_test_postgres(&database_url, "aether_test", right_namespace)
            .expect("open right postgres journal");

        left.append(&[sample_datom(1, "left")])
            .expect("append left");
        right
            .append(&[sample_datom(1, "right")])
            .expect("same element can exist in another namespace");
        let duplicate = left.append(&[sample_datom(1, "left-again")]);
        assert!(matches!(
            duplicate,
            Err(JournalError::DuplicateElementId(id)) if id == ElementId::new(1)
        ));
    }

    #[test]
    fn postgres_journal_serializes_concurrent_namespace_appends_when_configured() {
        let Some(database_url) = postgres_test_url() else {
            return;
        };
        let namespace = unique_postgres_namespace("concurrent");
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();
        for offset in 0..4 {
            let mut journal = open_test_postgres(&database_url, "aether_test", namespace.clone())
                .expect("open postgres journal");
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                journal
                    .append(&[sample_datom(100 + offset, &format!("value-{offset}"))])
                    .expect("append concurrent datom");
            }));
        }
        for handle in handles {
            handle.join().expect("join concurrent append");
        }

        let journal = open_test_postgres(&database_url, "aether_test", namespace)
            .expect("reopen postgres journal");
        let history = journal.history().expect("history");
        assert_eq!(history.len(), 4);
        let mut elements = history
            .iter()
            .map(|datom| datom.element.0)
            .collect::<Vec<_>>();
        elements.sort_unstable();
        assert_eq!(elements, vec![100, 101, 102, 103]);
        let prefix = journal
            .prefix(&history.last().expect("last datom").element)
            .expect("prefix at committed tail");
        assert_eq!(prefix.len(), 4);
    }

    #[test]
    fn postgres_append_and_schema_activation_are_linearizable_when_configured() {
        let Some(database_url) = postgres_test_url() else {
            return;
        };
        let namespace = unique_postgres_namespace("append_activation_race");
        let initial = {
            let mut journal = open_test_postgres(&database_url, "aether_test", namespace.clone())
                .expect("open postgres journal");
            journal
                .register_schema_revision(&stored_schema("v1", "schema-digest"))
                .expect("register v1");
            journal
                .register_schema_revision(&stored_schema("v2", "schema-digest-v2"))
                .expect("register v2");
            let initial = journal.cut().expect("initial cut");
            journal
                .activate_schema_revision(None, "schema-digest", &initial)
                .expect("activate v1");
            initial
        };
        let barrier = Arc::new(Barrier::new(3));
        let append_url = database_url.clone();
        let append_namespace = namespace.clone();
        let append_cut = initial.clone();
        let append_barrier = barrier.clone();
        let append = thread::spawn(move || {
            let mut journal = open_test_postgres(&append_url, "aether_test", append_namespace)
                .expect("open append connection");
            append_barrier.wait();
            journal.append_if_cut(
                &append_cut,
                &[sample_datom(1, "winner")],
                &draft("race-batch", "race-digest", None),
            )
        });
        let activation_cut = initial;
        let activation_barrier = barrier.clone();
        let activation = thread::spawn(move || {
            let mut journal = open_test_postgres(&database_url, "aether_test", namespace)
                .expect("open activation connection");
            activation_barrier.wait();
            journal.activate_schema_revision(
                Some("schema-digest"),
                "schema-digest-v2",
                &activation_cut,
            )
        });
        barrier.wait();
        let append = append.join().expect("append thread");
        let activation = activation.join().expect("activation thread");
        assert_ne!(append.is_ok(), activation.is_ok());
        assert!(matches!(
            (&append, &activation),
            (Ok(_), Err(JournalError::StaleCut { .. }))
                | (Err(JournalError::ActiveSchemaChanged { .. }), Ok(_))
        ));
    }

    fn postgres_test_url() -> Option<String> {
        std::env::var("AETHER_TEST_POSTGRES_URL")
            .or_else(|_| std::env::var("AETHER_POSTGRES_TEST_URL"))
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    }

    fn open_test_postgres(
        database_url: &str,
        schema: &str,
        namespace: impl Into<String>,
    ) -> Result<PostgresJournal, JournalError> {
        if let Ok(ca) = std::env::var("AETHER_POSTGRES_TLS_CA") {
            let tls = PostgresTlsConfig {
                ca_certificate_paths: vec![PathBuf::from(ca)],
                disable_system_roots: true,
                ..PostgresTlsConfig::default()
            };
            PostgresJournal::open_with_tls(database_url, schema, namespace, &tls)
        } else {
            PostgresJournal::open(database_url, schema, namespace)
        }
    }

    fn unique_postgres_namespace(name: &str) -> String {
        let unique = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        format!("test_{name}_{nanos}_{unique}")
    }

    struct TestDbPath {
        path: PathBuf,
    }

    impl TestDbPath {
        fn new(name: &str) -> Self {
            let unique = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos();
            let mut path = std::env::temp_dir();
            path.push(format!("aether-storage-{name}-{nanos}-{unique}.sqlite"));
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDbPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);

            let wal = PathBuf::from(format!("{}-wal", self.path.display()));
            let shm = PathBuf::from(format!("{}-shm", self.path.display()));
            let _ = std::fs::remove_file(wal);
            let _ = std::fs::remove_file(shm);
        }
    }
}
