use crate::{ApiError, ENGINE_SEMANTICS_VERSION};
use aether_ast::{Datom, PartitionCut, PolicyScope, RuleProgram, TemporalView};
use aether_plan::ScopedProgram;
use aether_resolver::{
    MaterializedResolver, ResolveError, ResolvedSnapshot, Resolver, ScopedReplay,
};
use aether_rules::{DefaultRuleCompiler, ScopedRuleCompiler};
use aether_runtime::{EvaluationBundle, RuntimeLimits, SemiNaiveRuntime};
use aether_schema::Schema;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fmt;

#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) struct EvaluationKey([u8; 32]);

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct FederationIdentityMaterial {
    pub(crate) leader_epochs: Vec<(String, u64)>,
    pub(crate) visible_prefix_digests: Vec<(String, String)>,
    pub(crate) imported_execution_ids: Vec<(String, String)>,
}

impl EvaluationKey {
    pub(crate) fn to_hex(&self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

impl fmt::Debug for EvaluationKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EvaluationKey")
            .field("algorithm", &"sha256")
            .field("redacted", &true)
            .finish()
    }
}

pub(crate) fn project_history(
    history: &[Datom],
    scope: PolicyScope,
) -> Result<Vec<Datom>, ApiError> {
    project_history_at_view(history, TemporalView::Current, scope)
}

pub(crate) fn project_history_at_view(
    history: &[Datom],
    view: TemporalView,
    scope: PolicyScope,
) -> Result<Vec<Datom>, ApiError> {
    Ok(ScopedReplay::new(history, view, scope)
        .map_err(public_resolve_error)?
        .datoms()
        .to_vec())
}

pub(crate) fn resolve_snapshot(
    schema: &Schema,
    history: &[Datom],
    view: TemporalView,
    scope: PolicyScope,
) -> Result<ResolvedSnapshot, ApiError> {
    MaterializedResolver
        .replay_scoped(schema, history, view, scope)
        .map_err(public_resolve_error)
}

pub(crate) struct ScopedEvaluationBuilder<'a> {
    namespace: String,
    schema: &'a Schema,
    history: &'a [Datom],
    program: ScopedProgram,
    federation: FederationIdentityMaterial,
}

impl<'a> ScopedEvaluationBuilder<'a> {
    pub(crate) fn new_in_namespace(
        namespace: impl Into<String>,
        schema: &'a Schema,
        history: &'a [Datom],
        program: &RuleProgram,
        scope: PolicyScope,
    ) -> Result<Self, ApiError> {
        let program = DefaultRuleCompiler.compile_scoped(schema, program, scope)?;
        Ok(Self {
            namespace: namespace.into(),
            schema,
            history,
            program,
            federation: FederationIdentityMaterial::default(),
        })
    }

    pub(crate) fn with_federation_identity(
        mut self,
        mut federation: FederationIdentityMaterial,
    ) -> Self {
        federation.leader_epochs.sort();
        federation.visible_prefix_digests.sort();
        federation.imported_execution_ids.sort();
        self.federation = federation;
        self
    }

    pub(crate) fn program(&self) -> &ScopedProgram {
        &self.program
    }

    pub(crate) fn evaluate_with_key(
        &self,
        view: TemporalView,
    ) -> Result<(EvaluationKey, EvaluationBundle), ApiError> {
        self.evaluate_with_key_and_limits(view, RuntimeLimits::UNBOUNDED)
    }

    pub(crate) fn evaluate_with_key_and_limits(
        &self,
        view: TemporalView,
        limits: RuntimeLimits,
    ) -> Result<(EvaluationKey, EvaluationBundle), ApiError> {
        let replay = ScopedReplay::new(self.history, view, self.program.scope().clone())
            .map_err(public_resolve_error)?;
        let key = build_evaluation_key(
            &self.namespace,
            self.schema,
            &self.program,
            &replay,
            &self.federation,
        )?;
        let snapshot = MaterializedResolver
            .resolve_scoped(self.schema, &replay)
            .map_err(public_resolve_error)?;
        let evaluation =
            SemiNaiveRuntime.evaluate_scoped_with_limits(snapshot, self.program.clone(), limits)?;
        Ok((key, evaluation))
    }
}

fn public_resolve_error(error: ResolveError) -> ApiError {
    match error {
        ResolveError::UnknownElementId(element) => {
            ApiError::Validation(format!("unknown element {}", element.0))
        }
        other => ApiError::Resolve(other),
    }
}

fn build_evaluation_key(
    namespace: &str,
    schema: &Schema,
    program: &ScopedProgram,
    replay: &ScopedReplay,
    federation: &FederationIdentityMaterial,
) -> Result<EvaluationKey, ApiError> {
    let mut imported_cuts = program
        .compiled()
        .facts
        .iter()
        .filter_map(|fact| fact.provenance.as_ref())
        .flat_map(|provenance| provenance.imported_cuts.iter().cloned())
        .collect::<Vec<_>>();
    imported_cuts.sort_by(|left, right| {
        left.partition
            .cmp(&right.partition)
            .then_with(|| left.as_of.cmp(&right.as_of))
    });
    imported_cuts.dedup();

    let mut attributes = schema.attributes.values().collect::<Vec<_>>();
    attributes.sort_by_key(|attribute| attribute.id);
    let mut predicates = schema.predicates.values().collect::<Vec<_>>();
    predicates.sort_by_key(|predicate| predicate.id);

    let schema_material = (&schema.version, attributes, predicates);
    let scope_material = program.scope().context();
    let view_material = replay.requested_view();
    let visible_history_material = replay.datoms();
    let program_material = program.compiled();
    let imported_material: Vec<PartitionCut> = imported_cuts;

    let mut hasher = Sha256::new();
    hash_component(&mut hasher, "namespace", &namespace)?;
    hash_component(&mut hasher, "scope", scope_material)?;
    hash_component(&mut hasher, "view", view_material)?;
    hash_component(&mut hasher, "visible_history", &visible_history_material)?;
    hash_component(&mut hasher, "schema", &schema_material)?;
    hash_component(&mut hasher, "program", program_material)?;
    hash_component(&mut hasher, "imported_cuts", &imported_material)?;
    hash_component(&mut hasher, "federation", federation)?;
    hash_component(
        &mut hasher,
        "engine_semantics_version",
        &ENGINE_SEMANTICS_VERSION,
    )?;

    Ok(EvaluationKey(hasher.finalize().into()))
}

fn hash_component<T: Serialize + ?Sized>(
    hasher: &mut Sha256,
    label: &str,
    value: &T,
) -> Result<(), ApiError> {
    let encoded = serde_json::to_vec(value).map_err(|error| {
        ApiError::Validation(format!("evaluation key encoding failed: {error}"))
    })?;
    hasher.update((label.len() as u64).to_be_bytes());
    hasher.update(label.as_bytes());
    hasher.update((encoded.len() as u64).to_be_bytes());
    hasher.update(encoded);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{build_evaluation_key, EvaluationKey, FederationIdentityMaterial};
    use aether_ast::{
        AttributeId, Datom, DatomProvenance, ElementId, EntityId, OperationKind, PolicyContext,
        PolicyEnvelope, PolicyScope, ReplicaId, RuleProgram, TemporalView, Value,
    };
    use aether_resolver::ScopedReplay;
    use aether_rules::{DefaultRuleCompiler, ScopedRuleCompiler};
    use aether_schema::Schema;

    fn datom(element: u64, hidden: bool) -> Datom {
        Datom {
            entity: EntityId::new(1),
            attribute: AttributeId::new(1),
            value: Value::String(format!("value-{element}")),
            op: OperationKind::Assert,
            element: ElementId::new(element),
            replica: ReplicaId::new(1),
            causal_context: Default::default(),
            provenance: DatomProvenance::default(),
            policy: hidden.then(|| PolicyEnvelope {
                capabilities: vec!["restricted".into()],
                visibilities: Vec::new(),
            }),
        }
    }

    fn key(
        namespace: &str,
        history: &[Datom],
        view: TemporalView,
        scope: PolicyScope,
    ) -> EvaluationKey {
        let schema = Schema::new("key-v1");
        let program = DefaultRuleCompiler
            .compile_scoped(&schema, &RuleProgram::default(), scope.clone())
            .expect("compile empty scoped program");
        let replay = ScopedReplay::new(history, view, scope).expect("construct scoped replay");
        build_evaluation_key(
            namespace,
            &schema,
            &program,
            &replay,
            &FederationIdentityMaterial::default(),
        )
        .expect("build evaluation key")
    }

    #[test]
    fn hidden_only_tail_does_not_change_public_evaluation_key() {
        let visible = vec![datom(1, false)];
        let mixed = vec![datom(1, false), datom(2, true)];

        let visible_key = key(
            "alpha",
            &visible,
            TemporalView::Current,
            PolicyScope::public(),
        );
        let mixed_key = key(
            "alpha",
            &mixed,
            TemporalView::Current,
            PolicyScope::public(),
        );

        assert_eq!(visible_key, mixed_key);
        assert_eq!(visible_key.0.len(), 32);
    }

    #[test]
    fn namespace_scope_and_requested_view_are_key_material() {
        let history = vec![datom(1, false), datom(2, true)];
        let public = key(
            "alpha",
            &history,
            TemporalView::Current,
            PolicyScope::public(),
        );
        let other_namespace = key(
            "beta",
            &history,
            TemporalView::Current,
            PolicyScope::public(),
        );
        let restricted = key(
            "alpha",
            &history,
            TemporalView::Current,
            PolicyScope::new(PolicyContext {
                capabilities: vec!["restricted".into()],
                visibilities: Vec::new(),
            }),
        );
        let as_of = key(
            "alpha",
            &history,
            TemporalView::AsOf(ElementId::new(1)),
            PolicyScope::public(),
        );

        assert_ne!(public, other_namespace);
        assert_ne!(public, restricted);
        assert_ne!(public, as_of);
    }
}
