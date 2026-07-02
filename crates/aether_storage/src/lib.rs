use aether_ast::{Datom, ElementId};
use postgres::{Client, NoTls};
use rusqlite::{params, Connection, ErrorCode, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};
use thiserror::Error;

pub trait Journal {
    fn append(&mut self, datoms: &[Datom]) -> Result<(), JournalError>;
    fn history(&self) -> Result<Vec<Datom>, JournalError>;
    fn prefix(&self, at: &ElementId) -> Result<Vec<Datom>, JournalError>;
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct JournalSnapshot {
    pub entries: Vec<Datom>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct InMemoryJournal {
    entries: Vec<Datom>,
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

        let transaction = self.connection.transaction()?;
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
}

impl PostgresJournal {
    pub fn open(
        database_url: &str,
        schema: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self, JournalError> {
        let schema = validate_pg_identifier(&schema.into())?;
        let namespace = namespace.into();
        let mut client = Client::connect(database_url, NoTls)?;
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
                    "INSERT INTO {journal_table} (namespace, element, datom_json) VALUES ($1, $2, $3::jsonb)"
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
        read_postgres_datoms(
            &mut self.client.borrow_mut(),
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
            &mut client,
            &format!(
                "SELECT datom_json::text FROM {journal_table} WHERE namespace = $1 AND seq <= $2 ORDER BY seq ASC"
            ),
            &[&self.namespace, &seq],
        )
    }
}

fn configure_connection(connection: &Connection) -> Result<(), JournalError> {
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
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
        ",
    )?;
    Ok(())
}

fn initialize_postgres_schema(client: &mut Client, schema: &str) -> Result<(), JournalError> {
    let schema = quote_pg_identifier(schema);
    let journal_table = format!("{schema}.{}", quote_pg_identifier("journal_entries"));
    let lock_table = format!("{schema}.{}", quote_pg_identifier("namespace_locks"));
    client.batch_execute(&format!(
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
        "
    ))?;
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

fn read_postgres_datoms(
    client: &mut Client,
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

#[derive(Debug, Error)]
pub enum JournalError {
    #[error("duplicate element id {0}")]
    DuplicateElementId(ElementId),
    #[error("unknown element id {0}")]
    UnknownElementId(ElementId),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Postgres(#[from] postgres::Error),
    #[error("invalid postgres identifier {0}")]
    InvalidPostgresIdentifier(String),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::{InMemoryJournal, Journal, JournalError, PostgresJournal, SqliteJournal};
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
            let mut journal = PostgresJournal::open(&database_url, "aether_test", &namespace)
                .expect("open postgres journal");
            journal
                .append(&[
                    sample_datom(1, "alpha"),
                    sample_datom(3, "beta"),
                    sample_datom(9, "gamma"),
                ])
                .expect("append postgres entries");
        }

        let journal = PostgresJournal::open(&database_url, "aether_test", &namespace)
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
        let mut left = PostgresJournal::open(&database_url, "aether_test", left_namespace)
            .expect("open left postgres journal");
        let mut right = PostgresJournal::open(&database_url, "aether_test", right_namespace)
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
            let database_url = database_url.clone();
            let namespace = namespace.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let mut journal = PostgresJournal::open(&database_url, "aether_test", namespace)
                    .expect("open postgres journal");
                barrier.wait();
                journal
                    .append(&[sample_datom(100 + offset, &format!("value-{offset}"))])
                    .expect("append concurrent datom");
            }));
        }
        for handle in handles {
            handle.join().expect("join concurrent append");
        }

        let journal = PostgresJournal::open(&database_url, "aether_test", namespace)
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

    fn postgres_test_url() -> Option<String> {
        std::env::var("AETHER_POSTGRES_TEST_URL")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
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
