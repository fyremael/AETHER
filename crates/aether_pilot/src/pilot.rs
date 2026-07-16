use aether_ast::{
    AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, ReplicaId, Value,
};

pub const COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT: u64 = 5;
pub const COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT: u64 = 9;

pub fn coordination_pilot_dsl(view: &str, query_body: &str) -> String {
    format!(
        r#"
schema v1 {{
  attr task.depends_on: RefSet<Entity>
  attr task.status: ScalarLWW<String>
  attr task.claimed_by: ScalarLWW<String>
  attr task.lease_epoch: ScalarLWW<U64>
  attr task.lease_state: ScalarLWW<String>
  attr heartbeat.task: RefScalar<Entity>
  attr heartbeat.worker: ScalarLWW<String>
  attr heartbeat.epoch: ScalarLWW<U64>
  attr heartbeat.at: ScalarLWW<U64>
  attr outcome.task: RefScalar<Entity>
  attr outcome.worker: ScalarLWW<String>
  attr outcome.epoch: ScalarLWW<U64>
  attr outcome.status: ScalarLWW<String>
  attr outcome.detail: ScalarLWW<String>
}}

predicates {{
  task(Entity)
  worker(String)
  worker_capability(String, String)
  task_depends_on(Entity, Entity)
  task_status(Entity, String)
  task_claimed_by(Entity, String)
  task_lease_epoch(Entity, U64)
  task_lease_state(Entity, String)
  heartbeat_task(Entity, Entity)
  heartbeat_worker(Entity, String)
  heartbeat_epoch(Entity, U64)
  heartbeat_at(Entity, U64)
  outcome_task(Entity, Entity)
  outcome_worker(Entity, String)
  outcome_epoch(Entity, U64)
  outcome_status(Entity, String)
  outcome_detail(Entity, String)
  task_complete(Entity)
  dependency_blocked(Entity)
  lease_active(Entity, String, U64)
  lease_heartbeat(Entity, String, U64, U64)
  live_authority(Entity, String, U64, U64)
  active_claim(Entity)
  task_ready(Entity)
  worker_can_claim(Entity, String)
  execution_authorized(Entity, String, U64)
  execution_outcome_recorded(Entity, String, U64, String, String)
  execution_outcome_accepted(Entity, String, U64, String, String)
  execution_outcome_rejected_stale(Entity, String, U64, String, String)
}}

facts {{
  task(entity(1))
  task(entity(2))
  task(entity(3))
  worker("worker-a")
  worker("worker-b")
  worker_capability("worker-a", "executor")
  worker_capability("worker-b", "executor")
}}

rules {{
  task_complete(t) <- task_status(t, "done")
  dependency_blocked(t) <- task_depends_on(t, dep), not task_complete(dep)
  lease_active(t, w, epoch) <- task_claimed_by(t, w), task_lease_epoch(t, epoch), task_lease_state(t, "active")
  lease_heartbeat(t, w, epoch, beat) <- heartbeat_task(h, t), heartbeat_worker(h, w), heartbeat_epoch(h, epoch), heartbeat_at(h, beat)
  live_authority(t, w, epoch, beat) <- lease_active(t, w, epoch), lease_heartbeat(t, w, epoch, beat)
  active_claim(t) <- live_authority(t, w, epoch, beat)
  task_ready(t) <- task(t), not task_complete(t), not dependency_blocked(t), not active_claim(t)
  worker_can_claim(t, w) <- task_ready(t), worker(w), worker_capability(w, "executor")
  execution_authorized(t, w, epoch) <- live_authority(t, w, epoch, beat)
  execution_outcome_recorded(t, w, epoch, status, detail) <- outcome_task(o, t), outcome_worker(o, w), outcome_epoch(o, epoch), outcome_status(o, status), outcome_detail(o, detail)
  execution_outcome_accepted(t, w, epoch, status, detail) <- execution_outcome_recorded(t, w, epoch, status, detail), execution_authorized(t, w, epoch)
  execution_outcome_rejected_stale(t, w, epoch, status, detail) <- execution_outcome_recorded(t, w, epoch, status, detail), not execution_authorized(t, w, epoch)
  task_complete(t) <- execution_outcome_accepted(t, w, epoch, "completed", detail)
}}

materialize {{
  lease_heartbeat
  live_authority
  task_ready
  worker_can_claim
  execution_authorized
  execution_outcome_recorded
  execution_outcome_accepted
  execution_outcome_rejected_stale
}}

query {{
  {view}
  {query_body}
}}
"#
    )
}

pub fn coordination_pilot_seed_history() -> Vec<Datom> {
    vec![
        dependency_datom(1, 2, 1),
        datom(
            EntityId::new(2),
            AttributeId::new(2),
            Value::String("done".into()),
            OperationKind::Assert,
            2,
        ),
        datom(
            EntityId::new(1),
            AttributeId::new(3),
            Value::String("worker-a".into()),
            OperationKind::Claim,
            3,
        ),
        datom(
            EntityId::new(1),
            AttributeId::new(4),
            Value::U64(1),
            OperationKind::LeaseOpen,
            4,
        ),
        datom(
            EntityId::new(1),
            AttributeId::new(5),
            Value::String("active".into()),
            OperationKind::LeaseOpen,
            5,
        ),
        heartbeat_entity_datom(1001, 6, 1, 6),
        heartbeat_string_datom(1001, 7, "worker-a", 7),
        heartbeat_datum_u64(1001, 8, 1, 8),
        heartbeat_datum_u64(1001, 9, 100, 9),
        datom(
            EntityId::new(1),
            AttributeId::new(3),
            Value::String("worker-b".into()),
            OperationKind::Claim,
            10,
        ),
        datom(
            EntityId::new(1),
            AttributeId::new(4),
            Value::U64(2),
            OperationKind::LeaseRenew,
            11,
        ),
        heartbeat_entity_datom(1002, 6, 1, 12),
        heartbeat_string_datom(1002, 7, "worker-b", 13),
        heartbeat_datum_u64(1002, 8, 2, 14),
        heartbeat_datum_u64(1002, 9, 200, 15),
        outcome_entity_datom(2001, 10, 1, 16),
        outcome_string_datom(2001, 11, "worker-a", 17),
        outcome_datum_u64(2001, 12, 1, 18),
        outcome_string_datom(2001, 13, "completed", 19),
        outcome_string_datom(2001, 14, "stale-worker-a", 20),
        outcome_entity_datom(2002, 10, 1, 21),
        outcome_string_datom(2002, 11, "worker-b", 22),
        outcome_datum_u64(2002, 12, 2, 23),
        outcome_string_datom(2002, 13, "completed", 24),
        outcome_string_datom(2002, 14, "current-worker-b", 25),
    ]
}

fn dependency_datom(entity: u64, value: u64, element: u64) -> Datom {
    datom(
        EntityId::new(entity),
        AttributeId::new(1),
        Value::Entity(EntityId::new(value)),
        OperationKind::Add,
        element,
    )
}

fn heartbeat_entity_datom(entity: u64, attribute: u64, value: u64, element: u64) -> Datom {
    datom(
        EntityId::new(entity),
        AttributeId::new(attribute),
        Value::Entity(EntityId::new(value)),
        OperationKind::LeaseRenew,
        element,
    )
}

fn heartbeat_string_datom(entity: u64, attribute: u64, value: &str, element: u64) -> Datom {
    datom(
        EntityId::new(entity),
        AttributeId::new(attribute),
        Value::String(value.into()),
        OperationKind::LeaseRenew,
        element,
    )
}

fn heartbeat_datum_u64(entity: u64, attribute: u64, value: u64, element: u64) -> Datom {
    datom(
        EntityId::new(entity),
        AttributeId::new(attribute),
        Value::U64(value),
        OperationKind::LeaseRenew,
        element,
    )
}

fn outcome_entity_datom(entity: u64, attribute: u64, value: u64, element: u64) -> Datom {
    datom(
        EntityId::new(entity),
        AttributeId::new(attribute),
        Value::Entity(EntityId::new(value)),
        OperationKind::Annotate,
        element,
    )
}

fn outcome_string_datom(entity: u64, attribute: u64, value: &str, element: u64) -> Datom {
    datom(
        EntityId::new(entity),
        AttributeId::new(attribute),
        Value::String(value.into()),
        OperationKind::Annotate,
        element,
    )
}

fn outcome_datum_u64(entity: u64, attribute: u64, value: u64, element: u64) -> Datom {
    datom(
        EntityId::new(entity),
        AttributeId::new(attribute),
        Value::U64(value),
        OperationKind::Annotate,
        element,
    )
}

fn datom(
    entity: EntityId,
    attribute: AttributeId,
    value: Value,
    op: OperationKind,
    element: u64,
) -> Datom {
    Datom {
        entity,
        attribute,
        value,
        op,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy: None,
    }
}
