use aether_ast::{AttributeId, Datom, ElementId, EntityId, OperationKind, PolicyEnvelope, Value};
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
pub struct ResolvedFact {
    pub value: Value,
    pub source_datom_ids: Vec<ElementId>,
    pub policy: Option<PolicyEnvelope>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EntityState {
    pub attributes: IndexMap<AttributeId, ResolvedValue>,
    pub facts: IndexMap<AttributeId, Vec<ResolvedFact>>,
}

impl EntityState {
    pub fn attribute(&self, id: &AttributeId) -> Option<&ResolvedValue> {
        self.attributes.get(id)
    }

    pub fn facts(&self, id: &AttributeId) -> &[ResolvedFact] {
        self.facts.get(id).map(Vec::as_slice).unwrap_or(&[])
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

#[derive(Clone, Debug)]
struct SequenceEntry {
    element: ElementId,
    anchor: Option<ElementId>,
    fact: ResolvedFact,
    removed: bool,
}

#[derive(Clone, Debug, Default)]
struct ResolverWorkingState {
    resolved: ResolvedState,
    sequences: IndexMap<(EntityId, AttributeId), Vec<SequenceEntry>>,
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
    let mut state = ResolverWorkingState {
        resolved: ResolvedState {
            entities: IndexMap::new(),
            as_of,
        },
        sequences: IndexMap::new(),
    };

    for datom in datoms {
        let attribute = schema
            .attribute(&datom.attribute)
            .ok_or(ResolveError::UnknownAttribute(datom.attribute))?;
        validate_operation(attribute, datom)?;
        apply_datom(&mut state, attribute, datom)?;
    }

    Ok(state.resolved)
}

fn default_value_for(attribute: &AttributeSchema) -> ResolvedValue {
    match attribute.class {
        AttributeClass::ScalarLww | AttributeClass::RefScalar => ResolvedValue::Scalar(None),
        AttributeClass::SetAddWins | AttributeClass::RefSet => ResolvedValue::Set(Vec::new()),
        AttributeClass::SequenceRga => ResolvedValue::Sequence(Vec::new()),
    }
}

fn resolved_fact(datom: &Datom) -> ResolvedFact {
    ResolvedFact {
        value: datom.value.clone(),
        source_datom_ids: vec![datom.element],
        policy: datom.policy.clone().map(PolicyEnvelope::normalized),
    }
}

fn validate_operation(attribute: &AttributeSchema, datom: &Datom) -> Result<(), ResolveError> {
    let valid = match attribute.class {
        AttributeClass::ScalarLww | AttributeClass::RefScalar => matches!(
            datom.op,
            OperationKind::Assert
                | OperationKind::Claim
                | OperationKind::LeaseOpen
                | OperationKind::LeaseRenew
                | OperationKind::Annotate
                | OperationKind::Retract
                | OperationKind::Release
                | OperationKind::LeaseExpire
        ),
        AttributeClass::SetAddWins | AttributeClass::RefSet => matches!(
            datom.op,
            OperationKind::Add
                | OperationKind::Claim
                | OperationKind::Annotate
                | OperationKind::Remove
                | OperationKind::Release
                | OperationKind::LeaseExpire
                | OperationKind::Retract
        ),
        AttributeClass::SequenceRga => matches!(
            datom.op,
            OperationKind::InsertAfter | OperationKind::Remove | OperationKind::Retract
        ),
    };

    if valid {
        Ok(())
    } else {
        Err(ResolveError::InvalidOperationForAttribute {
            attribute: datom.attribute,
            class: attribute.class,
            op: datom.op,
        })
    }
}

fn apply_datom(
    state: &mut ResolverWorkingState,
    attribute: &AttributeSchema,
    datom: &Datom,
) -> Result<(), ResolveError> {
    match attribute.class {
        AttributeClass::ScalarLww | AttributeClass::RefScalar => {
            let entity_state = state.resolved.entities.entry(datom.entity).or_default();
            let slot = entity_state
                .attributes
                .entry(datom.attribute)
                .or_insert_with(|| default_value_for(attribute));
            let ResolvedValue::Scalar(value) = slot else {
                return Err(ResolveError::AttributeClassMismatch(datom.attribute));
            };

            match datom.op {
                OperationKind::Retract | OperationKind::Release | OperationKind::LeaseExpire => {
                    *value = None;
                    entity_state
                        .facts
                        .entry(datom.attribute)
                        .or_default()
                        .clear();
                }
                OperationKind::Assert
                | OperationKind::Claim
                | OperationKind::LeaseOpen
                | OperationKind::LeaseRenew
                | OperationKind::Annotate => {
                    *value = Some(datom.value.clone());
                    let facts = entity_state.facts.entry(datom.attribute).or_default();
                    facts.clear();
                    facts.push(resolved_fact(datom));
                }
                _ => unreachable!("operation validity is checked before applying datoms"),
            }
        }
        AttributeClass::SetAddWins | AttributeClass::RefSet => {
            let entity_state = state.resolved.entities.entry(datom.entity).or_default();
            let slot = entity_state
                .attributes
                .entry(datom.attribute)
                .or_insert_with(|| default_value_for(attribute));
            let ResolvedValue::Set(values) = slot else {
                return Err(ResolveError::AttributeClassMismatch(datom.attribute));
            };
            let facts = entity_state.facts.entry(datom.attribute).or_default();

            match datom.op {
                OperationKind::Remove
                | OperationKind::Release
                | OperationKind::LeaseExpire
                | OperationKind::Retract => {
                    values.retain(|value| value != &datom.value);
                    facts.retain(|fact| fact.value != datom.value);
                }
                OperationKind::Add | OperationKind::Claim | OperationKind::Annotate => {
                    if !values.iter().any(|value| value == &datom.value) {
                        values.push(datom.value.clone());
                    }
                    if !facts.iter().any(|fact| fact.value == datom.value) {
                        facts.push(resolved_fact(datom));
                    }
                }
                _ => unreachable!("operation validity is checked before applying datoms"),
            }
        }
        AttributeClass::SequenceRga => apply_sequence_datom(state, attribute, datom)?,
    }

    Ok(())
}

fn apply_sequence_datom(
    state: &mut ResolverWorkingState,
    attribute: &AttributeSchema,
    datom: &Datom,
) -> Result<(), ResolveError> {
    let sequence = state
        .sequences
        .entry((datom.entity, datom.attribute))
        .or_default();

    match datom.op {
        OperationKind::InsertAfter => {
            let anchor = match datom.provenance.parent_datom_ids.as_slice() {
                [] if sequence.is_empty() => None,
                [] => {
                    return Err(ResolveError::MissingSequenceAnchor {
                        attribute: datom.attribute,
                        element: datom.element,
                    });
                }
                [anchor] => Some(*anchor),
                parents => {
                    return Err(ResolveError::MalformedSequenceAnchor {
                        attribute: datom.attribute,
                        element: datom.element,
                        parent_count: parents.len(),
                    });
                }
            };

            if let Some(anchor) = anchor {
                if !sequence.iter().any(|entry| entry.element == anchor) {
                    return Err(ResolveError::UnknownSequenceAnchor {
                        attribute: datom.attribute,
                        element: datom.element,
                        anchor,
                    });
                }
            }

            sequence.push(SequenceEntry {
                element: datom.element,
                anchor,
                fact: resolved_fact(datom),
                removed: false,
            });
        }
        OperationKind::Remove | OperationKind::Retract => {
            for entry in sequence
                .iter_mut()
                .filter(|entry| !entry.removed && entry.fact.value == datom.value)
            {
                entry.removed = true;
            }
        }
        _ => unreachable!("operation validity is checked before applying datoms"),
    }

    rebuild_sequence_attribute(state, attribute, datom.entity);
    Ok(())
}

fn rebuild_sequence_attribute(
    state: &mut ResolverWorkingState,
    attribute: &AttributeSchema,
    entity: EntityId,
) {
    let sequence = state
        .sequences
        .get(&(entity, attribute.id))
        .cloned()
        .unwrap_or_default();
    let mut children: IndexMap<Option<ElementId>, Vec<SequenceEntry>> = IndexMap::new();
    for entry in sequence {
        children.entry(entry.anchor).or_default().push(entry);
    }
    for values in children.values_mut() {
        values.sort_by_key(|entry| entry.element);
    }

    let mut ordered_facts = Vec::new();
    collect_sequence_facts(None, &children, &mut ordered_facts);
    let ordered_values = ordered_facts
        .iter()
        .map(|fact| fact.value.clone())
        .collect::<Vec<_>>();

    let entity_state = state.resolved.entities.entry(entity).or_default();
    entity_state
        .attributes
        .insert(attribute.id, ResolvedValue::Sequence(ordered_values));
    entity_state.facts.insert(attribute.id, ordered_facts);
}

fn collect_sequence_facts(
    anchor: Option<ElementId>,
    children: &IndexMap<Option<ElementId>, Vec<SequenceEntry>>,
    ordered_facts: &mut Vec<ResolvedFact>,
) {
    let Some(entries) = children.get(&anchor) else {
        return;
    };

    for entry in entries {
        if !entry.removed {
            ordered_facts.push(entry.fact.clone());
        }
        collect_sequence_facts(Some(entry.element), children, ordered_facts);
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
    #[error("operation {op:?} is invalid for attribute {attribute} with class {class:?}")]
    InvalidOperationForAttribute {
        attribute: AttributeId,
        class: AttributeClass,
        op: OperationKind,
    },
    #[error("sequence insert {element} on attribute {attribute} requires exactly one anchor")]
    MalformedSequenceAnchor {
        attribute: AttributeId,
        element: ElementId,
        parent_count: usize,
    },
    #[error("sequence insert {element} on attribute {attribute} requires an anchor")]
    MissingSequenceAnchor {
        attribute: AttributeId,
        element: ElementId,
    },
    #[error(
        "sequence insert {element} on attribute {attribute} references unknown anchor {anchor}"
    )]
    UnknownSequenceAnchor {
        attribute: AttributeId,
        element: ElementId,
        anchor: ElementId,
    },
}

#[cfg(test)]
mod tests {
    use super::{MaterializedResolver, ResolveError, ResolvedFact, ResolvedValue, Resolver};
    use aether_ast::{
        AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, PolicyEnvelope,
        ReplicaId, Value,
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

    fn datom_with_policy(
        attribute: AttributeId,
        element: u64,
        op: OperationKind,
        value: &str,
        policy: PolicyEnvelope,
    ) -> Datom {
        let mut datom = datom(attribute, element, op, value);
        datom.policy = Some(policy);
        datom
    }

    fn sequence_insert_after(element: u64, value: &str, anchors: &[u64]) -> Datom {
        let mut datom = datom(SEQUENCE_ATTR, element, OperationKind::InsertAfter, value);
        datom.provenance.parent_datom_ids = anchors.iter().copied().map(ElementId::new).collect();
        datom
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
        assert_eq!(
            as_of
                .entity(&EntityId::new(1))
                .map(|entity| entity.facts(&SCALAR_ATTR)),
            Some(
                [ResolvedFact {
                    value: Value::String("closed".into()),
                    source_datom_ids: vec![ElementId::new(2)],
                    policy: None,
                }]
                .as_slice()
            )
        );
        assert!(current
            .entity(&EntityId::new(1))
            .is_some_and(|entity| entity.facts(&SCALAR_ATTR).is_empty()));
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
        assert_eq!(
            current
                .entity(&EntityId::new(1))
                .map(|entity| entity.facts(&SET_ATTR)),
            Some(
                [ResolvedFact {
                    value: Value::String("beta".into()),
                    source_datom_ids: vec![ElementId::new(2)],
                    policy: None,
                }]
                .as_slice()
            )
        );
    }

    #[test]
    fn sequence_replay_is_stable() {
        let schema = schema();
        let datoms = vec![
            sequence_insert_after(1, "a", &[]),
            sequence_insert_after(2, "b", &[1]),
            datom(SEQUENCE_ATTR, 3, OperationKind::Remove, "a"),
            sequence_insert_after(4, "c", &[1]),
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
        assert_eq!(
            current
                .entity(&EntityId::new(1))
                .map(|entity| entity.facts(&SEQUENCE_ATTR)),
            Some(
                [
                    ResolvedFact {
                        value: Value::String("b".into()),
                        source_datom_ids: vec![ElementId::new(2)],
                        policy: None,
                    },
                    ResolvedFact {
                        value: Value::String("c".into()),
                        source_datom_ids: vec![ElementId::new(4)],
                        policy: None,
                    },
                ]
                .as_slice()
            )
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

    #[test]
    fn scalar_resolved_fact_preserves_policy() {
        let schema = schema();
        let datoms = vec![datom_with_policy(
            SCALAR_ATTR,
            1,
            OperationKind::Assert,
            "ready",
            PolicyEnvelope {
                capabilities: vec!["executor".into()],
                visibilities: vec!["ops".into()],
            },
        )];

        let current = MaterializedResolver
            .current(&schema, &datoms)
            .expect("resolve current with policy");
        let facts = current
            .entity(&EntityId::new(1))
            .expect("entity state")
            .facts(&SCALAR_ATTR);

        assert_eq!(facts.len(), 1);
        assert_eq!(
            facts[0].policy,
            Some(PolicyEnvelope {
                capabilities: vec!["executor".into()],
                visibilities: vec!["ops".into()],
            })
        );
    }

    #[test]
    fn invalid_operation_for_attribute_is_rejected() {
        let schema = schema();
        let error = MaterializedResolver
            .current(
                &schema,
                &[datom(SCALAR_ATTR, 1, OperationKind::Add, "invalid")],
            )
            .expect_err("invalid scalar add should fail");

        assert!(matches!(
            error,
            ResolveError::InvalidOperationForAttribute {
                attribute,
                class: AttributeClass::ScalarLww,
                op: OperationKind::Add,
            } if attribute == SCALAR_ATTR
        ));
    }

    #[test]
    fn every_v1_operation_has_a_valid_home_class() {
        let schema = schema();

        let passing_cases = [
            datom(SCALAR_ATTR, 1, OperationKind::Assert, "scalar-assert"),
            datom(SCALAR_ATTR, 2, OperationKind::Retract, "scalar-assert"),
            datom(SET_ATTR, 3, OperationKind::Add, "set-add"),
            datom(SET_ATTR, 4, OperationKind::Remove, "set-add"),
            datom(SCALAR_ATTR, 5, OperationKind::Claim, "worker-a"),
            datom(SCALAR_ATTR, 6, OperationKind::Release, "worker-a"),
            datom(SCALAR_ATTR, 7, OperationKind::LeaseOpen, "active"),
            datom(SCALAR_ATTR, 8, OperationKind::LeaseRenew, "active"),
            datom(SCALAR_ATTR, 9, OperationKind::LeaseExpire, "active"),
            datom(SCALAR_ATTR, 10, OperationKind::Annotate, "annotated"),
        ];

        for datom in passing_cases {
            MaterializedResolver
                .current(&schema, &[datom])
                .expect("documented valid operation should resolve");
        }

        MaterializedResolver
            .current(
                &schema,
                &[sequence_insert_after(11, "sequence-bootstrap", &[])],
            )
            .expect("insert-after bootstrap should resolve");
    }

    #[test]
    fn documented_invalid_operation_matrix_examples_are_rejected() {
        let schema = schema();

        let invalid_cases = [
            (SCALAR_ATTR, OperationKind::Add, "scalar-invalid-add"),
            (SCALAR_ATTR, OperationKind::Remove, "scalar-invalid-remove"),
            (
                SCALAR_ATTR,
                OperationKind::InsertAfter,
                "scalar-invalid-insert",
            ),
            (SET_ATTR, OperationKind::Assert, "set-invalid-assert"),
            (SET_ATTR, OperationKind::LeaseOpen, "set-invalid-lease-open"),
            (
                SET_ATTR,
                OperationKind::LeaseRenew,
                "set-invalid-lease-renew",
            ),
            (SET_ATTR, OperationKind::InsertAfter, "set-invalid-insert"),
            (
                SEQUENCE_ATTR,
                OperationKind::Assert,
                "sequence-invalid-assert",
            ),
            (SEQUENCE_ATTR, OperationKind::Add, "sequence-invalid-add"),
            (
                SEQUENCE_ATTR,
                OperationKind::Claim,
                "sequence-invalid-claim",
            ),
            (
                SEQUENCE_ATTR,
                OperationKind::LeaseOpen,
                "sequence-invalid-lease-open",
            ),
            (
                SEQUENCE_ATTR,
                OperationKind::LeaseRenew,
                "sequence-invalid-lease-renew",
            ),
            (
                SEQUENCE_ATTR,
                OperationKind::LeaseExpire,
                "sequence-invalid-lease-expire",
            ),
            (
                SEQUENCE_ATTR,
                OperationKind::Annotate,
                "sequence-invalid-annotate",
            ),
            (
                SEQUENCE_ATTR,
                OperationKind::Release,
                "sequence-invalid-release",
            ),
        ];

        for (attribute, op, value) in invalid_cases {
            let error = MaterializedResolver
                .current(&schema, &[datom(attribute, 1, op, value)])
                .expect_err("documented invalid operation should fail");
            assert!(matches!(
                error,
                ResolveError::InvalidOperationForAttribute { attribute: actual, op: actual_op, .. }
                    if actual == attribute && actual_op == op
            ));
        }
    }

    #[test]
    fn claim_release_and_lease_cycles_replay_deterministically() {
        let schema = schema();
        let datoms = vec![
            datom(SCALAR_ATTR, 1, OperationKind::Claim, "worker-a"),
            datom(SCALAR_ATTR, 2, OperationKind::Release, "worker-a"),
            datom(SCALAR_ATTR, 3, OperationKind::LeaseOpen, "active"),
            datom(SCALAR_ATTR, 4, OperationKind::LeaseRenew, "active"),
            datom(SCALAR_ATTR, 5, OperationKind::LeaseExpire, "active"),
        ];

        let as_of_claim = MaterializedResolver
            .as_of(&schema, &datoms, &ElementId::new(1))
            .expect("resolve claim cut");
        let as_of_lease = MaterializedResolver
            .as_of(&schema, &datoms, &ElementId::new(4))
            .expect("resolve lease-renew cut");
        let current = MaterializedResolver
            .current(&schema, &datoms)
            .expect("resolve current");

        assert_eq!(
            as_of_claim
                .entity(&EntityId::new(1))
                .and_then(|entity| entity.attribute(&SCALAR_ATTR)),
            Some(&ResolvedValue::Scalar(Some(Value::String(
                "worker-a".into()
            ))))
        );
        assert_eq!(
            as_of_lease
                .entity(&EntityId::new(1))
                .and_then(|entity| entity.attribute(&SCALAR_ATTR)),
            Some(&ResolvedValue::Scalar(Some(Value::String("active".into()))))
        );
        assert_eq!(
            current
                .entity(&EntityId::new(1))
                .and_then(|entity| entity.attribute(&SCALAR_ATTR)),
            Some(&ResolvedValue::Scalar(None))
        );
    }

    #[test]
    fn sequence_insert_after_orders_children_by_anchor_then_element() {
        let schema = schema();
        let datoms = vec![
            sequence_insert_after(1, "a", &[]),
            sequence_insert_after(2, "c", &[1]),
            sequence_insert_after(3, "b", &[1]),
        ];

        let current = MaterializedResolver
            .current(&schema, &datoms)
            .expect("resolve anchored sequence");
        assert_eq!(
            current
                .entity(&EntityId::new(1))
                .and_then(|entity| entity.attribute(&SEQUENCE_ATTR)),
            Some(&ResolvedValue::Sequence(vec![
                Value::String("a".into()),
                Value::String("c".into()),
                Value::String("b".into()),
            ]))
        );
    }

    #[test]
    fn non_bootstrap_sequence_insert_requires_exactly_one_known_anchor() {
        let schema = schema();

        let missing_anchor = MaterializedResolver
            .current(
                &schema,
                &[
                    sequence_insert_after(1, "a", &[]),
                    sequence_insert_after(2, "b", &[]),
                ],
            )
            .expect_err("non-bootstrap insert without anchor should fail");
        assert!(matches!(
            missing_anchor,
            ResolveError::MissingSequenceAnchor {
                attribute,
                element,
            } if attribute == SEQUENCE_ATTR && element == ElementId::new(2)
        ));

        let malformed = MaterializedResolver
            .current(
                &schema,
                &[
                    sequence_insert_after(1, "a", &[]),
                    sequence_insert_after(2, "b", &[1, 9]),
                ],
            )
            .expect_err("multi-anchor insert should fail");
        assert!(matches!(
            malformed,
            ResolveError::MalformedSequenceAnchor {
                attribute,
                element,
                parent_count,
            } if attribute == SEQUENCE_ATTR
                && element == ElementId::new(2)
                && parent_count == 2
        ));

        let unknown = MaterializedResolver
            .current(
                &schema,
                &[
                    sequence_insert_after(1, "a", &[]),
                    sequence_insert_after(2, "b", &[9]),
                ],
            )
            .expect_err("unknown anchor should fail");
        assert!(matches!(
            unknown,
            ResolveError::UnknownSequenceAnchor {
                attribute,
                element,
                anchor,
            } if attribute == SEQUENCE_ATTR
                && element == ElementId::new(2)
                && anchor == ElementId::new(9)
        ));
    }
}
