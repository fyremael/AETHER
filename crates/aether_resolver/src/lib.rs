use aether_ast::{AttributeId, Datom, ElementId, EntityId, OperationKind, Value};
use aether_schema::{AttributeClass, AttributeSchema, Schema};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub trait Resolver {
    fn current(&self, schema: &Schema, datoms: &[Datom]) -> Result<ResolvedState, ResolveError>;
    fn as_of(
        &self,
        schema: &Schema,
        datoms: &[Datom],
        at: &ElementId,
    ) -> Result<ResolvedState, ResolveError>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ResolvedValue {
    Scalar(Option<Value>),
    Set(Vec<Value>),
    Sequence(Vec<Value>),
}

impl Default for ResolvedValue {
    fn default() -> Self {
        Self::Scalar(None)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EntityState {
    pub attributes: IndexMap<AttributeId, ResolvedValue>,
}

impl EntityState {
    pub fn attribute(&self, id: &AttributeId) -> Option<&ResolvedValue> {
        self.attributes.get(id)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ResolvedState {
    pub entities: IndexMap<EntityId, EntityState>,
    pub as_of: Option<ElementId>,
}

impl ResolvedState {
    pub fn entity(&self, id: &EntityId) -> Option<&EntityState> {
        self.entities.get(id)
    }
}

#[derive(Default)]
pub struct MaterializedResolver;

impl Resolver for MaterializedResolver {
    fn current(&self, schema: &Schema, datoms: &[Datom]) -> Result<ResolvedState, ResolveError> {
        let as_of = datoms.last().map(|datom| datom.element);
        resolve_datoms(schema, datoms, as_of)
    }

    fn as_of(
        &self,
        schema: &Schema,
        datoms: &[Datom],
        at: &ElementId,
    ) -> Result<ResolvedState, ResolveError> {
        let end = datoms
            .iter()
            .position(|datom| datom.element == *at)
            .ok_or(ResolveError::UnknownElementId(*at))?;
        resolve_datoms(schema, &datoms[..=end], Some(*at))
    }
}

fn resolve_datoms(
    schema: &Schema,
    datoms: &[Datom],
    as_of: Option<ElementId>,
) -> Result<ResolvedState, ResolveError> {
    let mut state = ResolvedState {
        entities: IndexMap::new(),
        as_of,
    };

    for datom in datoms {
        let attribute = schema
            .attribute(&datom.attribute)
            .ok_or(ResolveError::UnknownAttribute(datom.attribute))?;
        let entity_state = state.entities.entry(datom.entity).or_default();
        let slot = entity_state
            .attributes
            .entry(datom.attribute)
            .or_insert_with(|| default_value_for(attribute));

        match (attribute.class, slot) {
            (
                AttributeClass::ScalarLww | AttributeClass::RefScalar,
                ResolvedValue::Scalar(value),
            ) => match datom.op {
                OperationKind::Retract | OperationKind::Remove | OperationKind::Release => {
                    *value = None;
                }
                _ => {
                    *value = Some(datom.value.clone());
                }
            },
            (AttributeClass::SetAddWins | AttributeClass::RefSet, ResolvedValue::Set(values)) => {
                match datom.op {
                    OperationKind::Retract | OperationKind::Remove | OperationKind::Release => {
                        values.retain(|value| value != &datom.value);
                    }
                    _ => {
                        if !values.iter().any(|value| value == &datom.value) {
                            values.push(datom.value.clone());
                        }
                    }
                }
            }
            (AttributeClass::SequenceRga, ResolvedValue::Sequence(values)) => match datom.op {
                OperationKind::Retract | OperationKind::Remove => {
                    values.retain(|value| value != &datom.value);
                }
                _ => values.push(datom.value.clone()),
            },
            _ => return Err(ResolveError::AttributeClassMismatch(datom.attribute)),
        }
    }

    Ok(state)
}

fn default_value_for(attribute: &AttributeSchema) -> ResolvedValue {
    match attribute.class {
        AttributeClass::ScalarLww | AttributeClass::RefScalar => ResolvedValue::Scalar(None),
        AttributeClass::SetAddWins | AttributeClass::RefSet => ResolvedValue::Set(Vec::new()),
        AttributeClass::SequenceRga => ResolvedValue::Sequence(Vec::new()),
    }
}

#[derive(Debug, Error)]
pub enum ResolveError {
    #[error("unknown attribute {0}")]
    UnknownAttribute(AttributeId),
    #[error("unknown element id {0}")]
    UnknownElementId(ElementId),
    #[error("attribute class mismatch for attribute {0}")]
    AttributeClassMismatch(AttributeId),
}

#[cfg(test)]
mod tests {
    use super::{MaterializedResolver, ResolvedValue, Resolver};
    use aether_ast::{
        AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, ReplicaId, Value,
    };
    use aether_schema::{AttributeClass, AttributeSchema, Schema, ValueType};

    const SCALAR_ATTR: AttributeId = AttributeId(1);
    const SET_ATTR: AttributeId = AttributeId(2);
    const SEQUENCE_ATTR: AttributeId = AttributeId(3);

    fn schema() -> Schema {
        let mut schema = Schema::new("v1");
        for (id, name, class) in [
            (SCALAR_ATTR, "task.status", AttributeClass::ScalarLww),
            (SET_ATTR, "task.tags", AttributeClass::SetAddWins),
            (SEQUENCE_ATTR, "task.steps", AttributeClass::SequenceRga),
        ] {
            schema
                .register_attribute(AttributeSchema {
                    id,
                    name: name.into(),
                    class,
                    value_type: ValueType::String,
                })
                .expect("register attribute");
        }
        schema
    }

    fn datom(attribute: AttributeId, element: u64, op: OperationKind, value: &str) -> Datom {
        Datom {
            entity: EntityId::new(1),
            attribute,
            value: Value::String(value.into()),
            op,
            element: ElementId::new(element),
            replica: ReplicaId::new(1),
            causal_context: Default::default(),
            provenance: DatomProvenance::default(),
            policy: None,
        }
    }

    #[test]
    fn scalar_lww_and_retract_behavior_are_deterministic() {
        let schema = schema();
        let datoms = vec![
            datom(SCALAR_ATTR, 1, OperationKind::Assert, "open"),
            datom(SCALAR_ATTR, 2, OperationKind::Assert, "closed"),
            datom(SCALAR_ATTR, 3, OperationKind::Retract, "closed"),
        ];
        let resolver = MaterializedResolver;

        let as_of = resolver
            .as_of(&schema, &datoms, &ElementId::new(2))
            .expect("resolve as_of");
        let current = resolver.current(&schema, &datoms).expect("resolve current");

        assert_eq!(
            as_of
                .entity(&EntityId::new(1))
                .and_then(|entity| entity.attribute(&SCALAR_ATTR)),
            Some(&ResolvedValue::Scalar(Some(Value::String("closed".into()))))
        );
        assert_eq!(
            current
                .entity(&EntityId::new(1))
                .and_then(|entity| entity.attribute(&SCALAR_ATTR)),
            Some(&ResolvedValue::Scalar(None))
        );
    }

    #[test]
    fn set_add_and_remove_behavior_is_preserved() {
        let schema = schema();
        let datoms = vec![
            datom(SET_ATTR, 1, OperationKind::Add, "alpha"),
            datom(SET_ATTR, 2, OperationKind::Add, "beta"),
            datom(SET_ATTR, 3, OperationKind::Remove, "alpha"),
        ];
        let resolver = MaterializedResolver;

        let current = resolver.current(&schema, &datoms).expect("resolve current");
        assert_eq!(
            current
                .entity(&EntityId::new(1))
                .and_then(|entity| entity.attribute(&SET_ATTR)),
            Some(&ResolvedValue::Set(vec![Value::String("beta".into())]))
        );
    }

    #[test]
    fn sequence_replay_is_stable() {
        let schema = schema();
        let datoms = vec![
            datom(SEQUENCE_ATTR, 1, OperationKind::InsertAfter, "a"),
            datom(SEQUENCE_ATTR, 2, OperationKind::InsertAfter, "b"),
            datom(SEQUENCE_ATTR, 3, OperationKind::Remove, "a"),
            datom(SEQUENCE_ATTR, 4, OperationKind::InsertAfter, "c"),
        ];
        let resolver = MaterializedResolver;

        let current = resolver.current(&schema, &datoms).expect("resolve current");
        assert_eq!(
            current
                .entity(&EntityId::new(1))
                .and_then(|entity| entity.attribute(&SEQUENCE_ATTR)),
            Some(&ResolvedValue::Sequence(vec![
                Value::String("b".into()),
                Value::String("c".into()),
            ]))
        );
    }

    #[test]
    fn current_equals_as_of_last_element() {
        let schema = schema();
        let datoms = vec![
            datom(SCALAR_ATTR, 1, OperationKind::Assert, "queued"),
            datom(SCALAR_ATTR, 2, OperationKind::Assert, "running"),
        ];
        let resolver = MaterializedResolver;

        let current = resolver.current(&schema, &datoms).expect("resolve current");
        let as_of = resolver
            .as_of(&schema, &datoms, &ElementId::new(2))
            .expect("resolve as_of");

        assert_eq!(current, as_of);
    }
}
