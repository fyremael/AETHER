use aether_ast::{Datom, ElementId};
use rusqlite::{params, Connection, ErrorCode, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{
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
    Serde(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::{InMemoryJournal, Journal, JournalError, SqliteJournal};
    use aether_ast::{
        AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, ReplicaId, Value,
    };
    use std::{
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
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
