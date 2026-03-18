use aether_ast::{Datom, ElementId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
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

#[derive(Debug, Error)]
pub enum JournalError {
    #[error("duplicate element id {0}")]
    DuplicateElementId(ElementId),
    #[error("unknown element id {0}")]
    UnknownElementId(ElementId),
}

#[cfg(test)]
mod tests {
    use super::{InMemoryJournal, Journal, JournalError};
    use aether_ast::{
        AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, ReplicaId, Value,
    };

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
}
