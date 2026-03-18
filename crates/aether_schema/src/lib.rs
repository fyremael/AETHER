use aether_ast::{AttributeId, PredicateId};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ValueType {
    Bool,
    I64,
    U64,
    F64,
    String,
    Bytes,
    Entity,
    List(Box<ValueType>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AttributeClass {
    ScalarLww,
    SetAddWins,
    SequenceRga,
    RefScalar,
    RefSet,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AttributeSchema {
    pub id: AttributeId,
    pub name: String,
    pub class: AttributeClass,
    pub value_type: ValueType,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PredicateSignature {
    pub id: PredicateId,
    pub name: String,
    pub fields: Vec<ValueType>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    pub version: String,
    pub attributes: IndexMap<AttributeId, AttributeSchema>,
    pub predicates: IndexMap<PredicateId, PredicateSignature>,
}

impl Schema {
    pub fn new(version: impl Into<String>) -> Self {
        Self {
            version: version.into(),
            attributes: IndexMap::new(),
            predicates: IndexMap::new(),
        }
    }

    pub fn register_attribute(&mut self, attribute: AttributeSchema) -> Result<(), SchemaError> {
        if self.attributes.contains_key(&attribute.id) {
            return Err(SchemaError::DuplicateAttributeId(attribute.id));
        }
        if self
            .attributes
            .values()
            .any(|existing| existing.name == attribute.name)
        {
            return Err(SchemaError::DuplicateAttributeName(attribute.name));
        }
        self.attributes.insert(attribute.id, attribute);
        Ok(())
    }

    pub fn register_predicate(&mut self, predicate: PredicateSignature) -> Result<(), SchemaError> {
        if self.predicates.contains_key(&predicate.id) {
            return Err(SchemaError::DuplicatePredicateId(predicate.id));
        }
        if self
            .predicates
            .values()
            .any(|existing| existing.name == predicate.name)
        {
            return Err(SchemaError::DuplicatePredicateName(predicate.name));
        }
        self.predicates.insert(predicate.id, predicate);
        Ok(())
    }

    pub fn attribute(&self, id: &AttributeId) -> Option<&AttributeSchema> {
        self.attributes.get(id)
    }

    pub fn predicate(&self, id: &PredicateId) -> Option<&PredicateSignature> {
        self.predicates.get(id)
    }

    pub fn validate_predicate_arity(
        &self,
        id: &PredicateId,
        arity: usize,
    ) -> Result<(), SchemaError> {
        let signature = self
            .predicate(id)
            .ok_or(SchemaError::UnknownPredicate(*id))?;
        if signature.fields.len() != arity {
            return Err(SchemaError::PredicateArityMismatch {
                predicate: *id,
                expected: signature.fields.len(),
                actual: arity,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("duplicate attribute id {0}")]
    DuplicateAttributeId(AttributeId),
    #[error("duplicate attribute name {0}")]
    DuplicateAttributeName(String),
    #[error("duplicate predicate id {0}")]
    DuplicatePredicateId(PredicateId),
    #[error("duplicate predicate name {0}")]
    DuplicatePredicateName(String),
    #[error("unknown attribute {0}")]
    UnknownAttribute(AttributeId),
    #[error("unknown predicate {0}")]
    UnknownPredicate(PredicateId),
    #[error("predicate {predicate} has arity {actual}, expected {expected}")]
    PredicateArityMismatch {
        predicate: PredicateId,
        expected: usize,
        actual: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::{
        AttributeClass, AttributeSchema, PredicateSignature, Schema, SchemaError, ValueType,
    };
    use aether_ast::{AttributeId, PredicateId};

    fn sample_attribute(id: u64, name: &str) -> AttributeSchema {
        AttributeSchema {
            id: AttributeId::new(id),
            name: name.into(),
            class: AttributeClass::ScalarLww,
            value_type: ValueType::String,
        }
    }

    fn sample_predicate(id: u64, name: &str, arity: usize) -> PredicateSignature {
        PredicateSignature {
            id: PredicateId::new(id),
            name: name.into(),
            fields: vec![ValueType::Entity; arity],
        }
    }

    #[test]
    fn duplicate_attribute_ids_and_names_are_rejected() {
        let mut schema = Schema::new("v1");
        schema
            .register_attribute(sample_attribute(1, "task.status"))
            .expect("register first attribute");

        let duplicate_id = schema.register_attribute(sample_attribute(1, "task.owner"));
        assert!(matches!(
            duplicate_id,
            Err(SchemaError::DuplicateAttributeId(id)) if id == AttributeId::new(1)
        ));

        let duplicate_name = schema.register_attribute(sample_attribute(2, "task.status"));
        assert!(matches!(
            duplicate_name,
            Err(SchemaError::DuplicateAttributeName(name)) if name == "task.status"
        ));
    }

    #[test]
    fn duplicate_predicate_ids_and_names_are_rejected() {
        let mut schema = Schema::new("v1");
        schema
            .register_predicate(sample_predicate(1, "ready", 1))
            .expect("register first predicate");

        let duplicate_id = schema.register_predicate(sample_predicate(1, "blocked", 1));
        assert!(matches!(
            duplicate_id,
            Err(SchemaError::DuplicatePredicateId(id)) if id == PredicateId::new(1)
        ));

        let duplicate_name = schema.register_predicate(sample_predicate(2, "ready", 1));
        assert!(matches!(
            duplicate_name,
            Err(SchemaError::DuplicatePredicateName(name)) if name == "ready"
        ));
    }

    #[test]
    fn predicate_lookup_and_arity_validation_report_errors() {
        let mut schema = Schema::new("v1");
        schema
            .register_predicate(sample_predicate(5, "depends_on", 2))
            .expect("register predicate");

        assert_eq!(
            schema
                .predicate(&PredicateId::new(5))
                .map(|predicate| predicate.name.as_str()),
            Some("depends_on")
        );
        assert!(matches!(
            schema.validate_predicate_arity(&PredicateId::new(99), 1),
            Err(SchemaError::UnknownPredicate(id)) if id == PredicateId::new(99)
        ));
        assert!(matches!(
            schema.validate_predicate_arity(&PredicateId::new(5), 1),
            Err(SchemaError::PredicateArityMismatch {
                predicate,
                expected: 2,
                actual: 1,
            }) if predicate == PredicateId::new(5)
        ));
        assert!(schema
            .validate_predicate_arity(&PredicateId::new(5), 2)
            .is_ok());
    }
}
