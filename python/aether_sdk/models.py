from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


JsonValue = dict[str, Any] | list[Any] | str | int | float | bool | None


@dataclass(slots=True)
class SourceRef:
    uri: str = ""
    digest: str | None = None


@dataclass(slots=True)
class DatomProvenance:
    author_principal: str = ""
    agent_id: str = ""
    tool_id: str = ""
    session_id: str = ""
    source_ref: SourceRef = field(default_factory=SourceRef)
    parent_datom_ids: list[int] = field(default_factory=list)
    confidence: float = 1.0
    trust_domain: str = ""
    schema_version: str = ""


@dataclass(slots=True)
class PolicyEnvelope:
    capability: str | None = None
    visibility: str | None = None


@dataclass(slots=True)
class PolicyContext:
    capabilities: list[str] = field(default_factory=list)
    visibilities: list[str] = field(default_factory=list)


@dataclass(slots=True)
class Datom:
    entity: int
    attribute: int
    value: JsonValue
    op: str
    element: int
    replica: int = 1
    causal_context: dict[str, Any] = field(
        default_factory=lambda: {"frontier": []}
    )
    provenance: DatomProvenance = field(default_factory=DatomProvenance)
    policy: PolicyEnvelope | None = None


@dataclass(slots=True)
class CurrentStateRequest:
    schema: dict[str, Any]
    datoms: list[Datom | dict[str, Any]] = field(default_factory=list)
    policy_context: PolicyContext | dict[str, Any] | None = None


@dataclass(slots=True)
class AsOfRequest:
    schema: dict[str, Any]
    at: int
    datoms: list[Datom | dict[str, Any]] = field(default_factory=list)
    policy_context: PolicyContext | dict[str, Any] | None = None


@dataclass(slots=True)
class RunDocumentRequest:
    dsl: str
    policy_context: PolicyContext | dict[str, Any] | None = None


@dataclass(slots=True)
class ArtifactReference:
    sidecar_id: str
    artifact_id: str
    entity: int
    uri: str
    media_type: str
    byte_length: int
    registered_at: int
    digest: str | None = None
    metadata: dict[str, JsonValue] = field(default_factory=dict)
    provenance: DatomProvenance = field(default_factory=DatomProvenance)
    policy: PolicyEnvelope | None = None


@dataclass(slots=True)
class GetArtifactReferenceRequest:
    sidecar_id: str
    artifact_id: str
    policy_context: PolicyContext | dict[str, Any] | None = None


@dataclass(slots=True)
class VectorRecordMetadata:
    sidecar_id: str
    vector_id: str
    entity: int
    embedding_ref: str
    dimensions: int
    metric: str
    registered_at: int
    source_artifact_id: str | None = None
    metadata: dict[str, JsonValue] = field(default_factory=dict)
    provenance: DatomProvenance = field(default_factory=DatomProvenance)
    policy: PolicyEnvelope | None = None


@dataclass(slots=True)
class RegisterVectorRecordRequest:
    record: VectorRecordMetadata | dict[str, Any]
    embedding: list[float]


@dataclass(slots=True)
class VectorFactProjection:
    predicate: dict[str, Any]
    query_entity: int


@dataclass(slots=True)
class SearchVectorsRequest:
    sidecar_id: str
    query_embedding: list[float]
    top_k: int
    metric: str
    as_of: int | None = None
    projection: VectorFactProjection | dict[str, Any] | None = None
    policy_context: PolicyContext | dict[str, Any] | None = None
