from __future__ import annotations

from typing import Any

from .models import (
    ArtifactReference,
    Datom,
    DatomProvenance,
    PolicyContext,
    PolicyEnvelope,
    SourceRef,
    VectorRecordMetadata,
)


COORDINATION_PILOT_PRE_HEARTBEAT_ELEMENT = 5
COORDINATION_PILOT_AUTHORIZED_AS_OF_ELEMENT = 9


def make_source_ref(uri: str = "", digest: str | None = None) -> SourceRef:
    return SourceRef(uri=uri, digest=digest)


def make_provenance(
    *,
    author_principal: str = "",
    agent_id: str = "",
    tool_id: str = "",
    session_id: str = "",
    source_uri: str = "",
    source_digest: str | None = None,
    parent_datom_ids: list[int] | None = None,
    confidence: float = 1.0,
    trust_domain: str = "",
    schema_version: str = "",
) -> DatomProvenance:
    return DatomProvenance(
        author_principal=author_principal,
        agent_id=agent_id,
        tool_id=tool_id,
        session_id=session_id,
        source_ref=make_source_ref(source_uri, source_digest),
        parent_datom_ids=list(parent_datom_ids or []),
        confidence=confidence,
        trust_domain=trust_domain,
        schema_version=schema_version,
    )


def make_policy(
    *,
    capability: str | None = None,
    visibility: str | None = None,
) -> PolicyEnvelope:
    return PolicyEnvelope(capability=capability, visibility=visibility)


def make_policy_context(
    *,
    capabilities: list[str] | None = None,
    visibilities: list[str] | None = None,
) -> PolicyContext:
    return PolicyContext(
        capabilities=list(capabilities or []),
        visibilities=list(visibilities or []),
    )


def value_string(value: str) -> dict[str, str]:
    return {"String": value}


def value_entity(value: int) -> dict[str, int]:
    return {"Entity": value}


def value_u64(value: int) -> dict[str, int]:
    return {"U64": value}


def make_datom(
    *,
    entity: int,
    attribute: int,
    value: dict[str, Any],
    element: int,
    op: str = "Assert",
    replica: int = 1,
    frontier: list[int] | None = None,
    provenance: DatomProvenance | None = None,
    policy: PolicyEnvelope | None = None,
) -> Datom:
    return Datom(
        entity=entity,
        attribute=attribute,
        value=value,
        op=op,
        element=element,
        replica=replica,
        causal_context={"frontier": list(frontier or [])},
        provenance=provenance or make_provenance(),
        policy=policy,
    )


def make_artifact_reference(
    *,
    sidecar_id: str,
    artifact_id: str,
    entity: int,
    uri: str,
    media_type: str,
    byte_length: int,
    registered_at: int,
    digest: str | None = None,
    metadata: dict[str, Any] | None = None,
    provenance: DatomProvenance | None = None,
    policy: PolicyEnvelope | None = None,
) -> ArtifactReference:
    return ArtifactReference(
        sidecar_id=sidecar_id,
        artifact_id=artifact_id,
        entity=entity,
        uri=uri,
        media_type=media_type,
        byte_length=byte_length,
        registered_at=registered_at,
        digest=digest,
        metadata=dict(metadata or {}),
        provenance=provenance or make_provenance(),
        policy=policy,
    )


def make_vector_record(
    *,
    sidecar_id: str,
    vector_id: str,
    entity: int,
    embedding_ref: str,
    dimensions: int,
    metric: str,
    registered_at: int,
    source_artifact_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    provenance: DatomProvenance | None = None,
    policy: PolicyEnvelope | None = None,
) -> VectorRecordMetadata:
    return VectorRecordMetadata(
        sidecar_id=sidecar_id,
        vector_id=vector_id,
        entity=entity,
        embedding_ref=embedding_ref,
        dimensions=dimensions,
        metric=metric,
        registered_at=registered_at,
        source_artifact_id=source_artifact_id,
        metadata=dict(metadata or {}),
        provenance=provenance or make_provenance(),
        policy=policy,
    )


def coordination_pilot_seed_history() -> list[Datom]:
    """Return the canonical coordination pilot history used by reports/tests."""

    return [
        make_datom(
            entity=1,
            attribute=1,
            value=value_entity(2),
            element=1,
            op="Add",
        ),
        make_datom(entity=2, attribute=2, value=value_string("done"), element=2),
        make_datom(
            entity=1,
            attribute=3,
            value=value_string("worker-a"),
            element=3,
            op="Claim",
        ),
        make_datom(
            entity=1,
            attribute=4,
            value=value_u64(1),
            element=4,
            op="LeaseOpen",
        ),
        make_datom(
            entity=1,
            attribute=5,
            value=value_string("active"),
            element=5,
            op="LeaseOpen",
        ),
        make_datom(
            entity=1001,
            attribute=6,
            value=value_entity(1),
            element=6,
            op="LeaseRenew",
        ),
        make_datom(
            entity=1001,
            attribute=7,
            value=value_string("worker-a"),
            element=7,
            op="LeaseRenew",
        ),
        make_datom(
            entity=1001,
            attribute=8,
            value=value_u64(1),
            element=8,
            op="LeaseRenew",
        ),
        make_datom(
            entity=1001,
            attribute=9,
            value=value_u64(100),
            element=9,
            op="LeaseRenew",
        ),
        make_datom(
            entity=1,
            attribute=3,
            value=value_string("worker-b"),
            element=10,
            op="Claim",
        ),
        make_datom(
            entity=1,
            attribute=4,
            value=value_u64(2),
            element=11,
            op="LeaseRenew",
        ),
        make_datom(
            entity=1002,
            attribute=6,
            value=value_entity(1),
            element=12,
            op="LeaseRenew",
        ),
        make_datom(
            entity=1002,
            attribute=7,
            value=value_string("worker-b"),
            element=13,
            op="LeaseRenew",
        ),
        make_datom(
            entity=1002,
            attribute=8,
            value=value_u64(2),
            element=14,
            op="LeaseRenew",
        ),
        make_datom(
            entity=1002,
            attribute=9,
            value=value_u64(200),
            element=15,
            op="LeaseRenew",
        ),
        make_datom(
            entity=2001,
            attribute=10,
            value=value_entity(1),
            element=16,
            op="Annotate",
        ),
        make_datom(
            entity=2001,
            attribute=11,
            value=value_string("worker-a"),
            element=17,
            op="Annotate",
        ),
        make_datom(
            entity=2001,
            attribute=12,
            value=value_u64(1),
            element=18,
            op="Annotate",
        ),
        make_datom(
            entity=2001,
            attribute=13,
            value=value_string("completed"),
            element=19,
            op="Annotate",
        ),
        make_datom(
            entity=2001,
            attribute=14,
            value=value_string("stale-worker-a"),
            element=20,
            op="Annotate",
        ),
        make_datom(
            entity=2002,
            attribute=10,
            value=value_entity(1),
            element=21,
            op="Annotate",
        ),
        make_datom(
            entity=2002,
            attribute=11,
            value=value_string("worker-b"),
            element=22,
            op="Annotate",
        ),
        make_datom(
            entity=2002,
            attribute=12,
            value=value_u64(2),
            element=23,
            op="Annotate",
        ),
        make_datom(
            entity=2002,
            attribute=13,
            value=value_string("completed"),
            element=24,
            op="Annotate",
        ),
        make_datom(
            entity=2002,
            attribute=14,
            value=value_string("current-worker-b"),
            element=25,
            op="Annotate",
        ),
    ]
