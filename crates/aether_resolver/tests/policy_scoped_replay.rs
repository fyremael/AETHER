use aether_ast::{
    AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, PolicyContext,
    PolicyEnvelope, PolicyScope, ReplicaId, TemporalView, Value,
};
use aether_resolver::{
    certify_history_dependencies, HistoryDependencyViolation, HistoryDependencyViolationReason,
    JournalDependencyKind, MaterializedResolver, ResolveError, ResolvedValue, Resolver,
    ScopedReplay,
};
use aether_schema::{AttributeClass, AttributeSchema, Schema, ValueType};

const STATUS: AttributeId = AttributeId(1);
const STEPS: AttributeId = AttributeId(2);

fn schema() -> Schema {
    let mut schema = Schema::new("v1");
    schema
        .register_attribute(AttributeSchema {
            id: STATUS,
            name: "task.status".into(),
            class: AttributeClass::ScalarLww,
            value_type: ValueType::String,
        })
        .expect("register status attribute");
    schema
        .register_attribute(AttributeSchema {
            id: STEPS,
            name: "task.steps".into(),
            class: AttributeClass::SequenceRga,
            value_type: ValueType::String,
        })
        .expect("register steps attribute");
    schema
}

fn datom(element: u64, op: OperationKind, value: &str, policy: Option<PolicyEnvelope>) -> Datom {
    Datom {
        entity: EntityId::new(1),
        attribute: STATUS,
        value: Value::String(value.into()),
        op,
        element: ElementId::new(element),
        replica: ReplicaId::new(1),
        causal_context: Default::default(),
        provenance: DatomProvenance::default(),
        policy,
    }
}

fn executor_policy() -> PolicyEnvelope {
    PolicyEnvelope {
        capabilities: vec!["executor".into()],
        visibilities: Vec::new(),
    }
}

fn executor_scope() -> PolicyScope {
    PolicyScope::new(PolicyContext {
        capabilities: vec!["executor".into()],
        visibilities: Vec::new(),
    })
}

fn with_dependencies(mut datom: Datom, parents: &[u64], frontier: &[u64]) -> Datom {
    datom.provenance.parent_datom_ids = parents.iter().copied().map(ElementId::new).collect();
    datom.causal_context.frontier = frontier.iter().copied().map(ElementId::new).collect();
    datom
}

fn sequence_insert(
    element: u64,
    value: &str,
    anchors: &[u64],
    policy: Option<PolicyEnvelope>,
) -> Datom {
    let mut datom = datom(element, OperationKind::InsertAfter, value, policy);
    datom.attribute = STEPS;
    with_dependencies(datom, anchors, &[])
}

fn assert_scoped_dependency_error(error: &ResolveError, element: u64, kind: JournalDependencyKind) {
    assert!(matches!(
        error,
        ResolveError::UnavailableScopedDependency {
            element: actual_element,
            kind: actual_kind,
        } if *actual_element == ElementId::new(element) && *actual_kind == kind
    ));
}

#[test]
fn public_current_ignores_hidden_retract_and_reports_visible_cut() {
    let history = vec![
        datom(1, OperationKind::Assert, "open", None),
        datom(2, OperationKind::Retract, "open", Some(executor_policy())),
    ];

    let public = MaterializedResolver
        .replay_scoped(
            &schema(),
            &history,
            TemporalView::Current,
            PolicyScope::public(),
        )
        .expect("resolve public current snapshot");
    assert_eq!(public.visible_cut(), Some(ElementId::new(1)));
    assert_eq!(public.state().as_of, Some(ElementId::new(1)));
    assert_eq!(
        public
            .state()
            .entity(&EntityId::new(1))
            .and_then(|entity| entity.attribute(&STATUS)),
        Some(&ResolvedValue::Scalar(Some(Value::String("open".into()))))
    );

    let privileged = MaterializedResolver
        .replay_scoped(&schema(), &history, TemporalView::Current, executor_scope())
        .expect("resolve privileged current snapshot");
    assert_eq!(privileged.visible_cut(), Some(ElementId::new(2)));
    assert_eq!(privileged.state().as_of, Some(ElementId::new(2)));
    assert_eq!(
        privileged
            .state()
            .entity(&EntityId::new(1))
            .and_then(|entity| entity.attribute(&STATUS)),
        Some(&ResolvedValue::Scalar(None))
    );
}

#[test]
fn as_of_selects_authority_prefix_before_policy_projection() {
    let history = vec![
        datom(1, OperationKind::Assert, "queued", None),
        datom(
            2,
            OperationKind::Assert,
            "protected-running",
            Some(executor_policy()),
        ),
        datom(3, OperationKind::Assert, "complete", None),
        datom(4, OperationKind::Assert, "later", None),
    ];

    let replay = ScopedReplay::new(
        &history,
        TemporalView::AsOf(ElementId::new(3)),
        PolicyScope::public(),
    )
    .expect("project public as-of replay");
    assert_eq!(
        replay
            .datoms()
            .iter()
            .map(|datom| datom.element)
            .collect::<Vec<_>>(),
        vec![ElementId::new(1), ElementId::new(3)]
    );
    assert_eq!(replay.visible_cut(), Some(ElementId::new(3)));

    let snapshot = MaterializedResolver
        .resolve_scoped(&schema(), &replay)
        .expect("resolve projected as-of replay");
    assert_eq!(snapshot.state().as_of, Some(ElementId::new(3)));
    assert_eq!(
        snapshot
            .state()
            .entity(&EntityId::new(1))
            .and_then(|entity| entity.attribute(&STATUS)),
        Some(&ResolvedValue::Scalar(Some(Value::String(
            "complete".into()
        ))))
    );
}

#[test]
fn hidden_and_nonexistent_as_of_elements_have_the_same_error_surface() {
    let hidden_history = vec![
        datom(1, OperationKind::Assert, "queued", None),
        datom(
            2,
            OperationKind::Assert,
            "protected-running",
            Some(executor_policy()),
        ),
    ];
    let absent_history = vec![datom(1, OperationKind::Assert, "queued", None)];

    let hidden = MaterializedResolver
        .replay_scoped(
            &schema(),
            &hidden_history,
            TemporalView::AsOf(ElementId::new(2)),
            PolicyScope::public(),
        )
        .expect_err("hidden cut must not be addressable");
    let absent = MaterializedResolver
        .replay_scoped(
            &schema(),
            &absent_history,
            TemporalView::AsOf(ElementId::new(2)),
            PolicyScope::public(),
        )
        .expect_err("nonexistent cut must not be addressable");

    assert!(matches!(
        &hidden,
        ResolveError::UnknownElementId(element) if *element == ElementId::new(2)
    ));
    assert!(matches!(
        &absent,
        ResolveError::UnknownElementId(element) if *element == ElementId::new(2)
    ));
    assert_eq!(hidden.to_string(), absent.to_string());
}

#[test]
fn public_child_cannot_cite_hidden_or_missing_provenance_parent() {
    let child = with_dependencies(datom(2, OperationKind::Assert, "child", None), &[1], &[]);
    let hidden_history = vec![
        datom(
            1,
            OperationKind::Assert,
            "hidden-parent",
            Some(executor_policy()),
        ),
        child.clone(),
    ];
    let missing_history = vec![child];

    let hidden = MaterializedResolver
        .replay_scoped(
            &schema(),
            &hidden_history,
            TemporalView::Current,
            PolicyScope::public(),
        )
        .expect_err("public child must not cite a hidden parent");
    let missing = MaterializedResolver
        .replay_scoped(
            &schema(),
            &missing_history,
            TemporalView::Current,
            PolicyScope::public(),
        )
        .expect_err("public child must not cite a missing parent");

    assert_scoped_dependency_error(&hidden, 2, JournalDependencyKind::ProvenanceParent);
    assert_scoped_dependency_error(&missing, 2, JournalDependencyKind::ProvenanceParent);
    assert_eq!(hidden.to_string(), missing.to_string());
}

#[test]
fn protected_child_may_cite_public_parent() {
    let history = vec![
        datom(1, OperationKind::Assert, "public-parent", None),
        with_dependencies(
            datom(
                2,
                OperationKind::Assert,
                "protected-child",
                Some(executor_policy()),
            ),
            &[1],
            &[],
        ),
    ];

    let public = MaterializedResolver
        .replay_scoped(
            &schema(),
            &history,
            TemporalView::Current,
            PolicyScope::public(),
        )
        .expect("public projection excludes the protected child");
    assert_eq!(public.visible_cut(), Some(ElementId::new(1)));

    let privileged = MaterializedResolver
        .replay_scoped(&schema(), &history, TemporalView::Current, executor_scope())
        .expect("protected child may cite public parent");
    assert_eq!(privileged.visible_cut(), Some(ElementId::new(2)));
    assert_eq!(
        privileged
            .state()
            .entity(&EntityId::new(1))
            .and_then(|entity| entity.attribute(&STATUS)),
        Some(&ResolvedValue::Scalar(Some(Value::String(
            "protected-child".into()
        ))))
    );
    assert!(certify_history_dependencies(&history).is_valid());
}

#[test]
fn invalid_hidden_child_is_ignored_until_its_scope_can_see_it() {
    let history = vec![
        datom(1, OperationKind::Assert, "public", None),
        with_dependencies(
            datom(
                2,
                OperationKind::Assert,
                "protected-child",
                Some(executor_policy()),
            ),
            &[99],
            &[],
        ),
    ];

    let public = MaterializedResolver
        .replay_scoped(
            &schema(),
            &history,
            TemporalView::Current,
            PolicyScope::public(),
        )
        .expect("hidden invalid child must not affect public replay");
    assert_eq!(public.visible_cut(), Some(ElementId::new(1)));

    let privileged = MaterializedResolver
        .replay_scoped(&schema(), &history, TemporalView::Current, executor_scope())
        .expect_err("visible invalid child must fail closed");
    assert_scoped_dependency_error(&privileged, 2, JournalDependencyKind::ProvenanceParent);

    let certification = certify_history_dependencies(&history);
    assert_eq!(
        certification.violations,
        vec![HistoryDependencyViolation {
            element: ElementId::new(2),
            kind: JournalDependencyKind::ProvenanceParent,
            dependency: Some(ElementId::new(99)),
            reason: HistoryDependencyViolationReason::MissingReference,
        }]
    );
}

#[test]
fn provenance_and_causal_dependencies_must_precede_the_child() {
    let history = vec![
        with_dependencies(datom(1, OperationKind::Assert, "child", None), &[2], &[3]),
        datom(2, OperationKind::Assert, "later-parent", None),
        datom(3, OperationKind::Assert, "later-frontier", None),
    ];

    let error = MaterializedResolver
        .replay_scoped(
            &schema(),
            &history,
            TemporalView::Current,
            PolicyScope::public(),
        )
        .expect_err("forward provenance parent must fail first");
    assert_scoped_dependency_error(&error, 1, JournalDependencyKind::ProvenanceParent);

    let certification = certify_history_dependencies(&history);
    assert_eq!(
        certification.violations,
        vec![
            HistoryDependencyViolation {
                element: ElementId::new(1),
                kind: JournalDependencyKind::ProvenanceParent,
                dependency: Some(ElementId::new(2)),
                reason: HistoryDependencyViolationReason::ForwardReference,
            },
            HistoryDependencyViolation {
                element: ElementId::new(1),
                kind: JournalDependencyKind::CausalFrontier,
                dependency: Some(ElementId::new(3)),
                reason: HistoryDependencyViolationReason::ForwardReference,
            },
        ]
    );
}

#[test]
fn public_causal_frontier_cannot_cite_hidden_state() {
    let history = vec![
        datom(
            1,
            OperationKind::Assert,
            "hidden-frontier",
            Some(executor_policy()),
        ),
        with_dependencies(
            datom(2, OperationKind::Assert, "public-child", None),
            &[],
            &[1],
        ),
    ];

    let error = MaterializedResolver
        .replay_scoped(
            &schema(),
            &history,
            TemporalView::Current,
            PolicyScope::public(),
        )
        .expect_err("public causal frontier must not cite hidden state");
    assert_scoped_dependency_error(&error, 2, JournalDependencyKind::CausalFrontier);
    assert_eq!(
        certify_history_dependencies(&history).violations,
        vec![HistoryDependencyViolation {
            element: ElementId::new(2),
            kind: JournalDependencyKind::CausalFrontier,
            dependency: Some(ElementId::new(1)),
            reason: HistoryDependencyViolationReason::PolicyNotClosed,
        }]
    );
}

#[test]
fn sequence_anchor_must_exist_precede_and_match_the_sequence() {
    let forward = vec![
        sequence_insert(1, "child", &[2], None),
        sequence_insert(2, "later-anchor", &[], None),
    ];
    let missing = vec![sequence_insert(1, "child", &[99], None)];
    let wrong_target = vec![
        datom(1, OperationKind::Assert, "not-a-sequence-entry", None),
        sequence_insert(2, "child", &[1], None),
    ];

    for (history, expected_element, expected_reason) in [
        (
            &forward,
            1,
            HistoryDependencyViolationReason::ForwardReference,
        ),
        (
            &missing,
            1,
            HistoryDependencyViolationReason::MissingReference,
        ),
        (
            &wrong_target,
            2,
            HistoryDependencyViolationReason::InvalidSequenceAnchor,
        ),
    ] {
        let error = MaterializedResolver
            .replay_scoped(
                &schema(),
                history,
                TemporalView::Current,
                PolicyScope::public(),
            )
            .expect_err("invalid sequence anchor must fail closed");
        assert_scoped_dependency_error(
            &error,
            expected_element,
            JournalDependencyKind::SequenceAnchor,
        );
        assert!(certify_history_dependencies(history)
            .violations
            .iter()
            .any(|violation| violation.reason == expected_reason));
    }
}

#[test]
fn sequence_anchor_policy_may_only_flow_from_public_to_protected() {
    let accepted = vec![
        sequence_insert(1, "public-parent", &[], None),
        sequence_insert(2, "protected-child", &[1], Some(executor_policy())),
    ];
    let rejected = vec![
        sequence_insert(1, "protected-parent", &[], Some(executor_policy())),
        sequence_insert(2, "public-child", &[1], None),
    ];

    assert!(certify_history_dependencies(&accepted).is_valid());
    MaterializedResolver
        .replay_scoped(
            &schema(),
            &accepted,
            TemporalView::Current,
            executor_scope(),
        )
        .expect("protected sequence child may cite public parent");

    let error = MaterializedResolver
        .replay_scoped(
            &schema(),
            &rejected,
            TemporalView::Current,
            PolicyScope::public(),
        )
        .expect_err("public sequence child must not cite hidden parent");
    assert_scoped_dependency_error(&error, 2, JournalDependencyKind::SequenceAnchor);
    assert_eq!(
        certify_history_dependencies(&rejected).violations,
        vec![HistoryDependencyViolation {
            element: ElementId::new(2),
            kind: JournalDependencyKind::SequenceAnchor,
            dependency: Some(ElementId::new(1)),
            reason: HistoryDependencyViolationReason::PolicyNotClosed,
        }]
    );
}

#[test]
fn sequence_insert_after_requires_an_anchor_after_the_visible_bootstrap() {
    let history = vec![
        sequence_insert(1, "first", &[], None),
        sequence_insert(2, "second", &[], None),
    ];

    let error = MaterializedResolver
        .replay_scoped(
            &schema(),
            &history,
            TemporalView::Current,
            PolicyScope::public(),
        )
        .expect_err("second visible sequence entry requires an anchor");
    assert_scoped_dependency_error(&error, 2, JournalDependencyKind::SequenceAnchor);
    assert_eq!(
        certify_history_dependencies(&history).violations,
        vec![HistoryDependencyViolation {
            element: ElementId::new(2),
            kind: JournalDependencyKind::SequenceAnchor,
            dependency: None,
            reason: HistoryDependencyViolationReason::MissingSequenceAnchor,
        }]
    );
}

#[test]
fn malformed_sequence_anchor_is_typed_and_fully_certified() {
    let history = vec![
        sequence_insert(1, "first", &[], None),
        sequence_insert(2, "second", &[1, 99], None),
    ];

    let error = MaterializedResolver
        .replay_scoped(
            &schema(),
            &history,
            TemporalView::Current,
            PolicyScope::public(),
        )
        .expect_err("multi-anchor sequence entry must fail closed");
    assert_scoped_dependency_error(&error, 2, JournalDependencyKind::SequenceAnchor);
    assert_eq!(
        certify_history_dependencies(&history).violations,
        vec![
            HistoryDependencyViolation {
                element: ElementId::new(2),
                kind: JournalDependencyKind::SequenceAnchor,
                dependency: None,
                reason: HistoryDependencyViolationReason::MalformedSequenceAnchor {
                    parent_count: 2,
                },
            },
            HistoryDependencyViolation {
                element: ElementId::new(2),
                kind: JournalDependencyKind::SequenceAnchor,
                dependency: Some(ElementId::new(99)),
                reason: HistoryDependencyViolationReason::MissingReference,
            },
        ]
    );
}

#[test]
fn certification_is_deterministic_and_does_not_mutate_history() {
    let history = vec![
        with_dependencies(datom(3, OperationKind::Assert, "missing", None), &[99], &[]),
        with_dependencies(datom(2, OperationKind::Assert, "forward", None), &[], &[4]),
        datom(4, OperationKind::Assert, "frontier", None),
    ];
    let original = history.clone();

    let first = certify_history_dependencies(&history);
    let second = certify_history_dependencies(&history);

    assert_eq!(history, original);
    assert_eq!(first, second);
    assert_eq!(first.datom_count, 3);
    assert_eq!(first.violations.len(), 2);
    assert!(!first.is_valid());
}
