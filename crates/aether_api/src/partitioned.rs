use crate::{
    ApiError, AppendRequest, CurrentStateRequest, HistoryRequest, InMemoryKernelService,
    KernelService,
};
use aether_ast::{Datom, FederatedCut, PartitionCut, PartitionId, PolicyContext};
use aether_resolver::ResolvedState;
use aether_schema::Schema;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default)]
pub struct PartitionedInMemoryKernelService {
    partitions: IndexMap<PartitionId, InMemoryKernelService>,
}

impl PartitionedInMemoryKernelService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn append_partition(
        &mut self,
        request: PartitionAppendRequest,
    ) -> Result<PartitionAppendResponse, ApiError> {
        let PartitionAppendRequest { partition, datoms } = request;
        let response = self
            .partitions
            .entry(partition.clone())
            .or_default()
            .append(AppendRequest { datoms })?;
        Ok(PartitionAppendResponse {
            partition,
            appended: response.appended,
        })
    }

    pub fn partition_history(
        &self,
        request: PartitionHistoryRequest,
    ) -> Result<PartitionHistoryResponse, ApiError> {
        let service = self.partition_service(&request.cut.partition)?;
        let datoms = match request.cut.as_of {
            Some(element) => {
                let full_history = service
                    .history(HistoryRequest {
                        policy_context: None,
                    })?
                    .datoms;
                let end = full_history
                    .iter()
                    .position(|datom| datom.element == element)
                    .ok_or_else(|| {
                        ApiError::Validation(format!(
                            "unknown element {} for partition {}",
                            element, request.cut.partition
                        ))
                    })?;
                filter_partition_datoms(
                    full_history[..=end].to_vec(),
                    request.policy_context.as_ref(),
                )
            }
            None => {
                service
                    .history(HistoryRequest {
                        policy_context: request.policy_context.clone(),
                    })?
                    .datoms
            }
        };

        Ok(PartitionHistoryResponse {
            cut: request.cut,
            datoms,
        })
    }

    pub fn partition_state(
        &self,
        request: PartitionStateRequest,
    ) -> Result<PartitionStateResponse, ApiError> {
        let service = self.partition_service(&request.cut.partition)?;
        let state = match request.cut.as_of {
            Some(element) => {
                service
                    .as_of(crate::AsOfRequest {
                        schema: request.schema,
                        datoms: Vec::new(),
                        at: element,
                        policy_context: request.policy_context,
                    })?
                    .state
            }
            None => {
                service
                    .current_state(CurrentStateRequest {
                        schema: request.schema,
                        datoms: Vec::new(),
                        policy_context: request.policy_context,
                    })?
                    .state
            }
        };

        Ok(PartitionStateResponse {
            cut: request.cut,
            state,
        })
    }

    pub fn federated_history(
        &self,
        request: FederatedHistoryRequest,
    ) -> Result<FederatedHistoryResponse, ApiError> {
        let cut = validate_federated_cut(request.cut)?;
        let mut partitions = Vec::with_capacity(cut.cuts.len());
        for partition_cut in cut.cuts {
            partitions.push(self.partition_history(PartitionHistoryRequest {
                cut: partition_cut,
                policy_context: request.policy_context.clone(),
            })?);
        }
        Ok(FederatedHistoryResponse { partitions })
    }

    fn partition_service(
        &self,
        partition: &PartitionId,
    ) -> Result<&InMemoryKernelService, ApiError> {
        self.partitions
            .get(partition)
            .ok_or_else(|| ApiError::Validation(format!("unknown partition {}", partition)))
    }
}

fn validate_federated_cut(cut: FederatedCut) -> Result<FederatedCut, ApiError> {
    let normalized = cut.normalized();
    for pair in normalized.cuts.windows(2) {
        if pair[0].partition == pair[1].partition {
            return Err(ApiError::Validation(format!(
                "federated cut contains duplicate partition {}",
                pair[0].partition
            )));
        }
    }
    Ok(normalized)
}

fn filter_partition_datoms(
    datoms: Vec<Datom>,
    policy_context: Option<&PolicyContext>,
) -> Vec<Datom> {
    datoms
        .into_iter()
        .filter(|datom| aether_ast::policy_allows(policy_context, datom.policy.as_ref()))
        .collect()
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PartitionAppendRequest {
    pub partition: PartitionId,
    pub datoms: Vec<Datom>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionAppendResponse {
    pub partition: PartitionId,
    pub appended: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionHistoryRequest {
    pub cut: PartitionCut,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PartitionHistoryResponse {
    pub cut: PartitionCut,
    pub datoms: Vec<Datom>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PartitionStateRequest {
    pub cut: PartitionCut,
    pub schema: Schema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PartitionStateResponse {
    pub cut: PartitionCut,
    pub state: ResolvedState,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedHistoryRequest {
    pub cut: FederatedCut,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_context: Option<PolicyContext>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FederatedHistoryResponse {
    pub partitions: Vec<PartitionHistoryResponse>,
}

#[cfg(test)]
mod tests {
    use super::{
        FederatedHistoryRequest, PartitionAppendRequest, PartitionHistoryRequest,
        PartitionStateRequest, PartitionedInMemoryKernelService,
    };
    use aether_ast::{
        AttributeId, Datom, DatomProvenance, ElementId, EntityId, FederatedCut, OperationKind,
        PartitionCut, PartitionId, PolicyEnvelope, ReplicaId, Value,
    };
    use aether_resolver::ResolvedValue;
    use aether_schema::{AttributeClass, AttributeSchema, Schema, ValueType};

    #[test]
    fn partitioned_service_keeps_local_truth_exact_per_partition() {
        let mut service = PartitionedInMemoryKernelService::new();
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("tenant-a"),
                datoms: vec![
                    sample_datom(1, 1, "tenant-a-open", 1, None),
                    sample_datom(1, 1, "tenant-a-running", 2, None),
                ],
            })
            .expect("append tenant-a");
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("tenant-b"),
                datoms: vec![sample_datom(1, 1, "tenant-b-ready", 1, None)],
            })
            .expect("append tenant-b");

        let tenant_a_current = service
            .partition_state(PartitionStateRequest {
                cut: PartitionCut::current("tenant-a"),
                schema: schema(),
                policy_context: None,
            })
            .expect("tenant-a current state");
        let tenant_a_as_of = service
            .partition_state(PartitionStateRequest {
                cut: PartitionCut::as_of("tenant-a", ElementId::new(1)),
                schema: schema(),
                policy_context: None,
            })
            .expect("tenant-a as-of state");
        let tenant_b_history = service
            .partition_history(PartitionHistoryRequest {
                cut: PartitionCut::current("tenant-b"),
                policy_context: None,
            })
            .expect("tenant-b history");

        assert_eq!(
            tenant_a_current
                .state
                .entity(&EntityId::new(1))
                .and_then(|entity| entity.attribute(&AttributeId::new(1))),
            Some(&ResolvedValue::Scalar(Some(Value::String(
                "tenant-a-running".into()
            ))))
        );
        assert_eq!(
            tenant_a_as_of
                .state
                .entity(&EntityId::new(1))
                .and_then(|entity| entity.attribute(&AttributeId::new(1))),
            Some(&ResolvedValue::Scalar(Some(Value::String(
                "tenant-a-open".into()
            ))))
        );
        assert_eq!(tenant_b_history.datoms.len(), 1);
        assert_eq!(
            tenant_b_history.datoms[0].value,
            Value::String("tenant-b-ready".into())
        );
    }

    #[test]
    fn federated_history_requires_explicit_unique_partition_cuts() {
        let mut service = PartitionedInMemoryKernelService::new();
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("tenant-a"),
                datoms: vec![sample_datom(1, 1, "alpha", 1, None)],
            })
            .expect("append tenant-a");
        service
            .append_partition(PartitionAppendRequest {
                partition: PartitionId::new("tenant-b"),
                datoms: vec![sample_datom(
                    1,
                    1,
                    "beta",
                    1,
                    Some(PolicyEnvelope {
                        capabilities: vec!["ops".into()],
                        visibilities: Vec::new(),
                    }),
                )],
            })
            .expect("append tenant-b");

        let federated = service
            .federated_history(FederatedHistoryRequest {
                cut: FederatedCut {
                    cuts: vec![
                        PartitionCut::current("tenant-b"),
                        PartitionCut::current("tenant-a"),
                    ],
                },
                policy_context: None,
            })
            .expect("federated history");
        assert_eq!(
            federated
                .partitions
                .iter()
                .map(|partition| partition.cut.to_string())
                .collect::<Vec<_>>(),
            vec![
                "tenant-a@current".to_string(),
                "tenant-b@current".to_string()
            ]
        );
        assert_eq!(federated.partitions[0].datoms.len(), 1);
        assert!(federated.partitions[1].datoms.is_empty());

        let duplicate_partition = service.federated_history(FederatedHistoryRequest {
            cut: FederatedCut {
                cuts: vec![
                    PartitionCut::current("tenant-a"),
                    PartitionCut::as_of("tenant-a", ElementId::new(1)),
                ],
            },
            policy_context: None,
        });
        assert!(matches!(
            duplicate_partition,
            Err(crate::ApiError::Validation(message))
                if message == "federated cut contains duplicate partition tenant-a"
        ));
    }

    #[test]
    fn unknown_partition_is_rejected_cleanly() {
        let service = PartitionedInMemoryKernelService::new();
        let error = service.partition_history(PartitionHistoryRequest {
            cut: PartitionCut::current("missing"),
            policy_context: None,
        });
        assert!(matches!(
            error,
            Err(crate::ApiError::Validation(message))
                if message == "unknown partition missing"
        ));
    }

    fn schema() -> Schema {
        let mut schema = Schema::new("partitioned-v1");
        schema
            .register_attribute(AttributeSchema {
                id: AttributeId::new(1),
                name: "task.status".into(),
                class: AttributeClass::ScalarLww,
                value_type: ValueType::String,
            })
            .expect("register status attribute");
        schema
    }

    fn sample_datom(
        entity: u64,
        attribute: u64,
        value: &str,
        element: u64,
        policy: Option<PolicyEnvelope>,
    ) -> Datom {
        Datom {
            entity: EntityId::new(entity),
            attribute: AttributeId::new(attribute),
            value: Value::String(value.into()),
            op: OperationKind::Assert,
            element: ElementId::new(element),
            replica: ReplicaId::new(1),
            causal_context: Default::default(),
            provenance: DatomProvenance::default(),
            policy,
        }
    }
}
