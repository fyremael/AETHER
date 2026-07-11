use aether_ast::{
    AggregateFunction, AggregateTerm, Atom, EntityId, ExtensionalFact, Literal, PolicyContext,
    PolicyEnvelope, PolicyScope, PredicateId, PredicateRef, RuleAst, RuleId, RuleProgram,
    TemporalView, Term, Value, Variable,
};
use aether_resolver::{MaterializedResolver, ResolvedSnapshot, Resolver};
use aether_rules::{DefaultRuleCompiler, ScopedRuleCompiler};
use aether_runtime::{RuntimeError, ScopedRuleRuntime, SemiNaiveRuntime};
use aether_schema::{PredicateSignature, Schema, ValueType};

fn predicate(id: u64, name: &str, fields: &[ValueType]) -> (PredicateRef, PredicateSignature) {
    (
        PredicateRef {
            id: PredicateId::new(id),
            name: name.to_string(),
            arity: fields.len(),
        },
        PredicateSignature {
            id: PredicateId::new(id),
            name: name.to_string(),
            fields: fields.to_vec(),
        },
    )
}

fn atom(predicate: &PredicateRef, variables: &[&str]) -> Atom {
    Atom {
        predicate: predicate.clone(),
        terms: variables
            .iter()
            .map(|variable| Term::Variable(Variable::new(*variable)))
            .collect(),
    }
}

fn aggregate(function: AggregateFunction, variable: &str) -> Term {
    Term::Aggregate(AggregateTerm {
        function,
        variable: Variable::new(variable),
    })
}

fn restricted_policy() -> PolicyEnvelope {
    PolicyEnvelope {
        capabilities: vec!["restricted".into()],
        visibilities: Vec::new(),
    }
}

fn restricted_scope() -> PolicyScope {
    PolicyScope::new(PolicyContext {
        capabilities: vec!["restricted".into()],
        visibilities: Vec::new(),
    })
}

fn fact(predicate: &PredicateRef, values: Vec<Value>, restricted: bool) -> ExtensionalFact {
    ExtensionalFact {
        predicate: predicate.clone(),
        values,
        policy: restricted.then(restricted_policy),
        provenance: None,
    }
}

fn schema(signatures: impl IntoIterator<Item = PredicateSignature>) -> Schema {
    let mut schema = Schema::new("policy-scoped-execution-v1");
    for signature in signatures {
        schema
            .register_predicate(signature)
            .expect("register predicate");
    }
    schema
}

fn empty_snapshot(schema: &Schema, scope: PolicyScope) -> ResolvedSnapshot {
    MaterializedResolver
        .replay_scoped(schema, &[], TemporalView::Current, scope)
        .expect("construct empty scoped snapshot")
}

#[test]
fn hidden_malformed_fact_cannot_change_public_compiler_success() {
    let (input, input_signature) = predicate(1, "input", &[ValueType::String]);
    let schema = schema([input_signature]);
    let program = RuleProgram {
        predicates: vec![input.clone()],
        rules: Vec::new(),
        materialized: Vec::new(),
        facts: vec![
            fact(&input, vec![Value::String("visible".into())], false),
            fact(&input, vec![Value::U64(7)], true),
        ],
    };

    let public = DefaultRuleCompiler
        .compile_scoped(&schema, &program, PolicyScope::public())
        .expect("hidden malformed fact must be projected before validation");
    assert_eq!(public.compiled().facts.len(), 1);
    assert_eq!(
        public.compiled().facts[0].values,
        vec![Value::String("visible".into())]
    );

    let privileged = DefaultRuleCompiler.compile_scoped(&schema, &program, restricted_scope());
    assert!(
        privileged.is_err(),
        "visible malformed fact must still fail"
    );
}

#[test]
fn hidden_negative_fact_cannot_suppress_a_public_derivation() {
    let (candidate, candidate_signature) = predicate(1, "candidate", &[ValueType::Entity]);
    let (blocked, blocked_signature) = predicate(2, "blocked", &[ValueType::Entity]);
    let (ready, ready_signature) = predicate(3, "ready", &[ValueType::Entity]);
    let schema = schema([candidate_signature, blocked_signature, ready_signature]);
    let program = RuleProgram {
        predicates: vec![candidate.clone(), blocked.clone(), ready.clone()],
        rules: vec![RuleAst {
            id: RuleId::new(1),
            head: atom(&ready, &["task"]),
            body: vec![
                Literal::Positive(atom(&candidate, &["task"])),
                Literal::Negative(atom(&blocked, &["task"])),
            ],
        }],
        materialized: vec![ready.id],
        facts: vec![
            fact(&candidate, vec![Value::Entity(EntityId::new(1))], false),
            fact(&blocked, vec![Value::Entity(EntityId::new(2))], false),
            fact(&blocked, vec![Value::Entity(EntityId::new(1))], true),
        ],
    };

    let public_scope = PolicyScope::public();
    let public_program = DefaultRuleCompiler
        .compile_scoped(&schema, &program, public_scope.clone())
        .expect("compile public program");
    let public = SemiNaiveRuntime
        .evaluate_scoped(empty_snapshot(&schema, public_scope), public_program)
        .expect("evaluate public program");
    assert!(public.derived().tuples.iter().any(|tuple| {
        tuple.tuple.predicate == ready.id
            && tuple.tuple.values == vec![Value::Entity(EntityId::new(1))]
    }));

    let privileged_scope = restricted_scope();
    let privileged_program = DefaultRuleCompiler
        .compile_scoped(&schema, &program, privileged_scope.clone())
        .expect("compile privileged program");
    let privileged = SemiNaiveRuntime
        .evaluate_scoped(
            empty_snapshot(&schema, privileged_scope),
            privileged_program,
        )
        .expect("evaluate privileged program");
    assert!(!privileged.derived().tuples.iter().any(|tuple| {
        tuple.tuple.predicate == ready.id
            && tuple.tuple.values == vec![Value::Entity(EntityId::new(1))]
    }));
}

#[test]
fn hidden_recursive_edge_cannot_change_public_closure_or_iteration_metadata() {
    let (edge, edge_signature) = predicate(1, "edge", &[ValueType::Entity, ValueType::Entity]);
    let (path, path_signature) = predicate(2, "path", &[ValueType::Entity, ValueType::Entity]);
    let schema = schema([edge_signature, path_signature]);
    let public_edge = fact(
        &edge,
        vec![
            Value::Entity(EntityId::new(1)),
            Value::Entity(EntityId::new(2)),
        ],
        false,
    );
    let hidden_edge = fact(
        &edge,
        vec![
            Value::Entity(EntityId::new(2)),
            Value::Entity(EntityId::new(3)),
        ],
        true,
    );
    let rules = vec![
        RuleAst {
            id: RuleId::new(1),
            head: atom(&path, &["x", "y"]),
            body: vec![Literal::Positive(atom(&edge, &["x", "y"]))],
        },
        RuleAst {
            id: RuleId::new(2),
            head: atom(&path, &["x", "z"]),
            body: vec![
                Literal::Positive(atom(&path, &["x", "y"])),
                Literal::Positive(atom(&edge, &["y", "z"])),
            ],
        },
    ];
    let full = RuleProgram {
        predicates: vec![edge.clone(), path.clone()],
        rules: rules.clone(),
        materialized: vec![path.id],
        facts: vec![public_edge.clone(), hidden_edge],
    };
    let projected_control = RuleProgram {
        predicates: vec![edge.clone(), path.clone()],
        rules,
        materialized: vec![path.id],
        facts: vec![public_edge],
    };

    let scope = PolicyScope::public();
    let full_program = DefaultRuleCompiler
        .compile_scoped(&schema, &full, scope.clone())
        .expect("compile projected full program");
    let control_program = DefaultRuleCompiler
        .compile_scoped(&schema, &projected_control, scope.clone())
        .expect("compile projection control");
    let full_result = SemiNaiveRuntime
        .evaluate_scoped(empty_snapshot(&schema, scope.clone()), full_program)
        .expect("evaluate projected full program");
    let control_result = SemiNaiveRuntime
        .evaluate_scoped(empty_snapshot(&schema, scope), control_program)
        .expect("evaluate projection control");

    assert_eq!(full_result.derived(), control_result.derived());
    assert!(!full_result.derived().tuples.iter().any(|tuple| {
        tuple.tuple.predicate == path.id
            && tuple.tuple.values
                == vec![
                    Value::Entity(EntityId::new(1)),
                    Value::Entity(EntityId::new(3)),
                ]
    }));
}

#[test]
fn hidden_rows_do_not_change_public_count_sum_min_or_max() {
    let (score, score_signature) = predicate(1, "score", &[ValueType::U64]);
    let (summary, summary_signature) = predicate(
        2,
        "summary",
        &[
            ValueType::U64,
            ValueType::U64,
            ValueType::U64,
            ValueType::U64,
        ],
    );
    let schema = schema([score_signature, summary_signature]);
    let program = RuleProgram {
        predicates: vec![score.clone(), summary.clone()],
        rules: vec![RuleAst {
            id: RuleId::new(1),
            head: Atom {
                predicate: summary.clone(),
                terms: vec![
                    aggregate(AggregateFunction::Count, "value"),
                    aggregate(AggregateFunction::Sum, "value"),
                    aggregate(AggregateFunction::Min, "value"),
                    aggregate(AggregateFunction::Max, "value"),
                ],
            },
            body: vec![Literal::Positive(atom(&score, &["value"]))],
        }],
        materialized: vec![summary.id],
        facts: vec![
            fact(&score, vec![Value::U64(2)], false),
            fact(&score, vec![Value::U64(7)], false),
            fact(&score, vec![Value::U64(100)], true),
        ],
    };

    let public_scope = PolicyScope::public();
    let public_program = DefaultRuleCompiler
        .compile_scoped(&schema, &program, public_scope.clone())
        .expect("compile public aggregates");
    let public = SemiNaiveRuntime
        .evaluate_scoped(empty_snapshot(&schema, public_scope), public_program)
        .expect("evaluate public aggregates");
    assert!(public.derived().tuples.iter().any(|tuple| {
        tuple.tuple.predicate == summary.id
            && tuple.tuple.values
                == vec![Value::U64(2), Value::U64(9), Value::U64(2), Value::U64(7)]
    }));

    let privileged_scope = restricted_scope();
    let privileged_program = DefaultRuleCompiler
        .compile_scoped(&schema, &program, privileged_scope.clone())
        .expect("compile privileged aggregates");
    let privileged = SemiNaiveRuntime
        .evaluate_scoped(
            empty_snapshot(&schema, privileged_scope),
            privileged_program,
        )
        .expect("evaluate privileged aggregates");
    assert!(privileged.derived().tuples.iter().any(|tuple| {
        tuple.tuple.predicate == summary.id
            && tuple.tuple.values
                == vec![
                    Value::U64(3),
                    Value::U64(109),
                    Value::U64(2),
                    Value::U64(100),
                ]
    }));
}

#[test]
fn snapshot_and_program_scope_mismatch_fails_closed() {
    let (input, input_signature) = predicate(1, "input", &[ValueType::String]);
    let schema = schema([input_signature]);
    let program = RuleProgram {
        predicates: vec![input],
        rules: Vec::new(),
        materialized: Vec::new(),
        facts: Vec::new(),
    };
    let privileged_program = DefaultRuleCompiler
        .compile_scoped(&schema, &program, restricted_scope())
        .expect("compile privileged empty program");
    let error = SemiNaiveRuntime
        .evaluate_scoped(
            empty_snapshot(&schema, PolicyScope::public()),
            privileged_program,
        )
        .expect_err("scope mismatch must fail");

    assert!(matches!(error, RuntimeError::PolicyScopeMismatch));
}
