use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(
            Clone,
            Copy,
            Debug,
            Default,
            Eq,
            Hash,
            Ord,
            PartialEq,
            PartialOrd,
            Serialize,
            Deserialize,
        )]
        pub struct $name(pub u64);

        impl $name {
            pub const fn new(value: u64) -> Self {
                Self(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

id_type!(EntityId);
id_type!(AttributeId);
id_type!(ElementId);
id_type!(PredicateId);
id_type!(RuleId);
id_type!(TupleId);
id_type!(ReplicaId);

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Entity(EntityId),
    List(Vec<Value>),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperationKind {
    #[default]
    Assert,
    Retract,
    Add,
    Remove,
    InsertAfter,
    LeaseOpen,
    LeaseRenew,
    LeaseExpire,
    Claim,
    Release,
    Annotate,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CausalContext {
    pub frontier: Vec<ElementId>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceRef {
    pub uri: String,
    pub digest: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DatomProvenance {
    pub author_principal: String,
    pub agent_id: String,
    pub tool_id: String,
    pub session_id: String,
    pub source_ref: SourceRef,
    pub parent_datom_ids: Vec<ElementId>,
    pub confidence: f32,
    pub trust_domain: String,
    pub schema_version: String,
}

impl Default for DatomProvenance {
    fn default() -> Self {
        Self {
            author_principal: String::new(),
            agent_id: String::new(),
            tool_id: String::new(),
            session_id: String::new(),
            source_ref: SourceRef::default(),
            parent_datom_ids: Vec::new(),
            confidence: 1.0,
            trust_domain: String::new(),
            schema_version: String::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyEnvelope {
    pub capability: Option<String>,
    pub visibility: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Datom {
    pub entity: EntityId,
    pub attribute: AttributeId,
    pub value: Value,
    pub op: OperationKind,
    pub element: ElementId,
    pub replica: ReplicaId,
    pub causal_context: CausalContext,
    pub provenance: DatomProvenance,
    pub policy: Option<PolicyEnvelope>,
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Variable(pub String);

impl Variable {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PredicateRef {
    pub id: PredicateId,
    pub name: String,
    pub arity: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Term {
    Variable(Variable),
    Value(Value),
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Atom {
    pub predicate: PredicateRef,
    pub terms: Vec<Term>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Positive(Atom),
    Negative(Atom),
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RuleAst {
    pub id: RuleId,
    pub head: Atom,
    pub body: Vec<Literal>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct QueryAst {
    pub goals: Vec<Atom>,
    pub keep: Vec<Variable>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RuleProgram {
    pub predicates: Vec<PredicateRef>,
    pub rules: Vec<RuleAst>,
    pub materialized: Vec<PredicateId>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Tuple {
    pub id: TupleId,
    pub predicate: PredicateId,
    pub values: Vec<Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DerivedTupleMetadata {
    pub rule_id: RuleId,
    pub predicate_id: PredicateId,
    pub stratum: usize,
    pub scc_id: usize,
    pub iteration: usize,
    pub parent_tuple_ids: Vec<TupleId>,
    pub source_datom_ids: Vec<ElementId>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DerivedTuple {
    pub tuple: Tuple,
    pub metadata: DerivedTupleMetadata,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DerivationTrace {
    pub root: TupleId,
    pub tuples: Vec<DerivedTuple>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PhaseSignature {
    pub available: Vec<String>,
    pub provides: Vec<String>,
    pub keep: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PhaseNode {
    pub id: String,
    pub signature: PhaseSignature,
    pub recursive_scc: Option<usize>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PhaseEdge {
    pub from: String,
    pub to: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PhaseGraph {
    pub nodes: Vec<PhaseNode>,
    pub edges: Vec<PhaseEdge>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PlanExplanation {
    pub summary: String,
    pub phase_graph: PhaseGraph,
}

#[cfg(test)]
mod tests {
    use super::{
        AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, ReplicaId,
        SourceRef, Value,
    };

    #[test]
    fn ids_are_deterministic_and_displayable() {
        let entity = EntityId::new(7);
        let attribute = AttributeId::new(11);
        let element = ElementId::new(19);

        assert_eq!(entity, EntityId::new(7));
        assert_eq!(attribute.to_string(), "11");
        assert_eq!(element.to_string(), "19");
    }

    #[test]
    fn value_default_is_null_and_round_trips_through_serde() {
        assert_eq!(Value::default(), Value::Null);

        let value = Value::List(vec![
            Value::String("task-1".into()),
            Value::Bool(true),
            Value::Entity(EntityId::new(42)),
        ]);

        let json = serde_json::to_string(&value).expect("serialize value");
        let decoded: Value = serde_json::from_str(&json).expect("deserialize value");

        assert_eq!(decoded, value);
    }

    #[test]
    fn provenance_defaults_and_datom_round_trip_preserve_fields() {
        let provenance = DatomProvenance {
            author_principal: "jamie".into(),
            agent_id: "codex".into(),
            tool_id: "shell".into(),
            session_id: "session-1".into(),
            source_ref: SourceRef {
                uri: "file:///fixture".into(),
                digest: Some("sha256:abc".into()),
            },
            parent_datom_ids: vec![ElementId::new(2), ElementId::new(3)],
            confidence: 0.75,
            trust_domain: "dev".into(),
            schema_version: "v1".into(),
        };
        let datom = Datom {
            entity: EntityId::new(1),
            attribute: AttributeId::new(2),
            value: Value::String("ready".into()),
            op: OperationKind::Assert,
            element: ElementId::new(4),
            replica: ReplicaId::new(5),
            causal_context: Default::default(),
            provenance: provenance.clone(),
            policy: None,
        };

        assert_eq!(DatomProvenance::default().confidence, 1.0);

        let json = serde_json::to_string(&datom).expect("serialize datom");
        let decoded: Datom = serde_json::from_str(&json).expect("deserialize datom");

        assert_eq!(decoded.provenance, provenance);
        assert_eq!(decoded, datom);
    }
}
