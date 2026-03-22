from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib import error, request


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
        timeout_seconds: float = 10.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._bearer_token = bearer_token
        self._timeout_seconds = timeout_seconds

    def health(self) -> dict[str, Any]:
        return self._request_json("GET", "/health")

    def history(self) -> dict[str, Any]:
        return self._request_json("GET", "/v1/history")

    def append(self, datoms: list[dict[str, Any]]) -> dict[str, Any]:
        return self._request_json("POST", "/v1/append", {"datoms": datoms})

    def current_state(
        self,
        *,
        schema: dict[str, Any],
        datoms: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/state/current",
            {"schema": schema, "datoms": datoms or []},
        )

    def as_of(
        self,
        *,
        schema: dict[str, Any],
        at: int,
        datoms: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/state/as-of",
            {"schema": schema, "datoms": datoms or [], "at": at},
        )

    def parse_document(self, dsl: str) -> dict[str, Any]:
        return self._request_json("POST", "/v1/documents/parse", {"dsl": dsl})

    def run_document(self, dsl: str) -> dict[str, Any]:
        return self._request_json("POST", "/v1/documents/run", {"dsl": dsl})

    def explain_tuple(self, tuple_id: int) -> dict[str, Any]:
        return self._request_json("POST", "/v1/explain/tuple", {"tuple_id": tuple_id})

    def register_artifact_reference(self, reference: dict[str, Any]) -> dict[str, Any]:
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
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/sidecars/artifacts/get",
            {"sidecar_id": sidecar_id, "artifact_id": artifact_id},
        )

    def register_vector_record(
        self,
        *,
        record: dict[str, Any],
        embedding: list[float],
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v1/sidecars/vectors/register",
            {"record": record, "embedding": embedding},
        )

    def search_vectors(self, request_payload: dict[str, Any]) -> dict[str, Any]:
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
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if self._bearer_token:
            headers["Authorization"] = f"Bearer {self._bearer_token}"

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

