from __future__ import annotations

import json
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any
from urllib import error, request

from .models import (
    ArtifactReference,
    AsOfRequest,
    CurrentStateRequest,
    Datom,
    GetArtifactReferenceRequest,
    PolicyContext,
    RegisterVectorRecordRequest,
    RunDocumentRequest,
    SearchVectorsRequest,
    VectorRecordMetadata,
)


JsonValue = dict[str, Any] | list[Any] | str | int | float | bool | None


@dataclass
class AetherApiError(Exception):
    status_code: int
    message: str
    payload: JsonValue | None = None

    def __str__(self) -> str:
        return f"AETHER API error ({self.status_code}): {self.message}"


class AetherClient:
    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str | None = None,
        namespace: str | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._bearer_token = bearer_token
        self._namespace = namespace
        self._timeout_seconds = timeout_seconds

    def health(self) -> dict[str, Any]:
        return self._request_json("GET", "/health")

    def status(self) -> dict[str, Any]:
        return self._request_json("GET", "/v1/status")

    def audit_log(self) -> dict[str, Any]:
        return self._request_json("GET", "/v1/audit")

    def history(self) -> dict[str, Any]:
        return self._request_json("GET", "/v1/history")

    def append(
        self,
        datoms: list[dict[str, Any] | Datom],
        *,
        schema_ref: dict[str, Any] | None = None,
        expected_cut: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/append",
            {
                "schema_ref": schema_ref,
                "expected_cut": expected_cut,
                "idempotency_key": idempotency_key,
                "datoms": datoms,
            },
        )

    def append_dry_run(
        self,
        datoms: list[dict[str, Any] | Datom],
        *,
        schema_ref: dict[str, Any] | None = None,
        expected_cut: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/append/dry-run",
            {
                "schema_ref": schema_ref,
                "expected_cut": expected_cut,
                "idempotency_key": idempotency_key,
                "datoms": datoms,
            },
        )

    def append_receipts(self) -> dict[str, Any]:
        return self._request_json("GET", "/v1/append/receipts")

    def schema_catalog(self) -> dict[str, Any]:
        return self._request_json("GET", "/v1/schema")

    def register_schema(
        self,
        schema: dict[str, Any],
        *,
        predecessor: dict[str, Any] | None = None,
        compatibility: str = "exact",
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/schema/register",
            {
                "schema": schema,
                "predecessor": predecessor,
                "compatibility": compatibility,
            },
        )

    def activate_schema(
        self,
        schema_ref: dict[str, Any],
        *,
        expected_active: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/schema/activate",
            {"schema_ref": schema_ref, "expected_active": expected_active},
        )

    def current_state(
        self,
        *,
        schema: dict[str, Any],
        datoms: list[dict[str, Any] | Datom] | None = None,
        policy_context: PolicyContext | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = CurrentStateRequest(
            schema=schema,
            datoms=list(datoms or []),
            policy_context=policy_context,
        )
        return self._request_json(
            "POST",
            "/v1/state/current",
            payload,
        )

    def as_of(
        self,
        *,
        schema: dict[str, Any],
        at: int,
        datoms: list[dict[str, Any] | Datom] | None = None,
        policy_context: PolicyContext | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = AsOfRequest(
            schema=schema,
            datoms=list(datoms or []),
            at=at,
            policy_context=policy_context,
        )
        return self._request_json(
            "POST",
            "/v1/state/as-of",
            payload,
        )

    def parse_document(self, dsl: str) -> dict[str, Any]:
        return self._request_json("POST", "/v1/documents/parse", {"dsl": dsl})

    def run_document(
        self,
        dsl: str,
        *,
        policy_context: PolicyContext | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/documents/run",
            RunDocumentRequest(dsl=dsl, policy_context=policy_context),
        )

    def run_named_query(
        self,
        dsl: str,
        *,
        query_name: str,
        policy_context: PolicyContext | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = self.run_document(dsl, policy_context=policy_context)
        for query in response.get("queries", []):
            if query.get("name") == query_name:
                result = query.get("result", query)
                execution_id = query.get("execution_id")
                receipt = next(
                    (
                        item
                        for item in response.get("executions", [])
                        if item.get("manifest", {}).get("execution_id") == execution_id
                    ),
                    None,
                )
                handles = {
                    binding["local_tuple_id"]: binding["handle"]
                    for binding in (receipt or {}).get("trace_handles", [])
                }
                for row in result.get("rows", []):
                    tuple_id = row.get("tuple_id")
                    if tuple_id in handles:
                        row["execution_id"] = execution_id
                        row["trace_handle"] = handles[tuple_id]
                return result
        raise AetherApiError(
            404,
            f"named query not found: {query_name}",
            {"query_name": query_name},
        )

    def resolve_trace_handle(
        self,
        handle: str,
        *,
        policy_context: PolicyContext | dict[str, Any] | None = None,
        verify_replay: bool = False,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "handle": handle,
            "verify_replay": verify_replay,
        }
        if policy_context is not None:
            payload["policy_context"] = policy_context
        return self._request_json("POST", "/v1/explanations/resolve", payload)

    def coordination_report(
        self,
        *,
        policy_context: PolicyContext | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if policy_context is not None:
            payload["policy_context"] = policy_context
        return self._request_json(
            "POST",
            "/v1/reports/pilot/coordination",
            payload,
        )

    def coordination_delta_report(
        self,
        *,
        left: dict[str, Any] | None = None,
        right: dict[str, Any] | None = None,
        policy_context: PolicyContext | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "left": left or {"kind": "current"},
            "right": right or {"kind": "current"},
        }
        if policy_context is not None:
            payload["policy_context"] = policy_context
        return self._request_json(
            "POST",
            "/v1/reports/pilot/coordination-delta",
            payload,
        )

    def register_artifact_reference(
        self,
        reference: dict[str, Any] | ArtifactReference,
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/sidecars/artifacts/register",
            {"reference": reference},
        )

    def get_artifact_reference(
        self,
        *,
        sidecar_id: str,
        artifact_id: str,
        policy_context: PolicyContext | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/sidecars/artifacts/get",
            GetArtifactReferenceRequest(
                sidecar_id=sidecar_id,
                artifact_id=artifact_id,
                policy_context=policy_context,
            ),
        )

    def register_vector_record(
        self,
        *,
        record: dict[str, Any] | VectorRecordMetadata,
        embedding: list[float],
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/sidecars/vectors/register",
            RegisterVectorRecordRequest(record=record, embedding=embedding),
        )

    def search_vectors(
        self,
        request_payload: dict[str, Any] | SearchVectorsRequest,
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/sidecars/vectors/search",
            request_payload,
        )

    def _request_json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(_jsonable(payload)).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if self._bearer_token:
            headers["Authorization"] = f"Bearer {self._bearer_token}"
        if self._namespace:
            headers["X-Aether-Namespace"] = self._namespace

        request_object = request.Request(
            f"{self._base_url}{path}",
            data=body,
            method=method,
            headers=headers,
        )

        try:
            with request.urlopen(request_object, timeout=self._timeout_seconds) as response:
                raw_body = response.read().decode("utf-8")
                return json.loads(raw_body) if raw_body else {}
        except error.HTTPError as exc:
            payload_text = exc.read().decode("utf-8")
            payload_json: JsonValue | None
            try:
                payload_json = json.loads(payload_text) if payload_text else None
            except json.JSONDecodeError:
                payload_json = payload_text or None
            message = (
                payload_json.get("error", payload_text)
                if isinstance(payload_json, dict)
                else payload_text or exc.reason
            )
            raise AetherApiError(exc.code, str(message), payload_json) from exc


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {
            key: _jsonable(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value
