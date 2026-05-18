from __future__ import annotations

from dataclasses import asdict
from time import time
from typing import Any

from .core import Claim, Pattern, TupleRecord, match_pattern, new_claim_id, new_tuple_id


class InMemoryBackend:
    """Single-shard backend for tests, notebooks, and demos.

    This backend mirrors tuple facade semantics, but it is not an authority
    engine. The Rust AETHER kernel remains the source of semantic truth.
    """

    def __init__(self) -> None:
        self._records: list[TupleRecord] = []
        self._claims: dict[str, Claim] = {}
        self._completed: dict[str, TupleRecord] = {}
        self._events: list[dict[str, Any]] = []
        self._lease_epoch = 0

    def out(self, fields: tuple[Any, ...], *, payload: dict[str, Any] | None = None, metadata: dict[str, Any] | None = None) -> TupleRecord:
        record = TupleRecord(tuple_id=new_tuple_id(), fields=fields, payload=dict(payload or {}), metadata=dict(metadata or {}))
        self._records.append(record)
        self._events.append({"event": "tuple_open", "record": asdict(record)})
        return record

    def read(self, pattern: Pattern, *, limit: int | None = None) -> list[TupleRecord]:
        rows: list[TupleRecord] = []
        for record in self._records:
            if record.tuple_id in self._completed:
                continue
            current_claim = self._claims.get(record.tuple_id)
            if current_claim is not None and not self._claim_expired(current_claim):
                continue
            if match_pattern(record.fields, pattern):
                rows.append(record)
                if limit is not None and len(rows) >= limit:
                    break
        return rows

    def claim(self, pattern: Pattern, *, owner: str, ttl_seconds: float | None = None) -> Claim | None:
        available = self.read(pattern, limit=1)
        if not available:
            return None
        record = available[0]
        self._lease_epoch += 1
        claim = Claim(
            claim_id=new_claim_id(),
            tuple_id=record.tuple_id,
            owner=owner,
            lease_epoch=self._lease_epoch,
            expires_at=(time() + ttl_seconds) if ttl_seconds is not None else None,
        )
        self._claims[record.tuple_id] = claim
        self._events.append({"event": "tuple_claim", "claim": asdict(claim)})
        return claim

    def release(self, claim: Claim, *, reason: str = "released") -> None:
        current = self._claims.get(claim.tuple_id)
        if current == claim:
            del self._claims[claim.tuple_id]
            self._events.append({"event": "tuple_release", "claim": asdict(claim), "reason": reason})

    def complete(self, claim: Claim, *, result: dict[str, Any] | None = None) -> TupleRecord:
        current = self._claims.get(claim.tuple_id)
        if current != claim:
            raise RuntimeError("claim is not current")
        source = self._find_tuple(claim.tuple_id)
        completed = TupleRecord(
            tuple_id=new_tuple_id("completion"),
            fields=("completion", claim.tuple_id, claim.owner, claim.lease_epoch),
            payload=dict(result or {}),
            metadata={"source_tuple_id": claim.tuple_id, "claim_id": claim.claim_id},
        )
        self._completed[source.tuple_id] = completed
        self._records.append(completed)
        self._events.append({"event": "tuple_complete", "claim": asdict(claim), "record": asdict(completed)})
        return completed

    def explain(self, tuple_id: str) -> dict[str, Any]:
        related = [event for event in self._events if tuple_id in str(event)]
        return {"tuple_id": tuple_id, "events": related}

    def events(self) -> list[dict[str, Any]]:
        return list(self._events)

    def _find_tuple(self, tuple_id: str) -> TupleRecord:
        for record in self._records:
            if record.tuple_id == tuple_id:
                return record
        raise KeyError(tuple_id)

    @staticmethod
    def _claim_expired(claim: Claim) -> bool:
        return claim.expires_at is not None and claim.expires_at <= time()


class AetherHttpBackend:
    """Placeholder for the real AETHER HTTP integration.

    It deliberately refuses to resolve reads or claims until those operations are
    backed by AETHER kernel rules or service endpoints. This keeps Python from
    becoming a shadow resolver.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def out(self, fields: tuple[Any, ...], *, payload: dict[str, Any] | None = None, metadata: dict[str, Any] | None = None) -> TupleRecord:
        record = TupleRecord(tuple_id=new_tuple_id(), fields=fields, payload=dict(payload or {}), metadata=dict(metadata or {}))
        envelope = {"tuple_id": record.tuple_id, "fields": list(record.fields), "payload": record.payload, "metadata": record.metadata}
        if hasattr(self._client, "append"):
            self._client.append([{"entity": 9000000, "attribute": 9000001, "value": {"String": str(envelope)}, "op": "Assert", "element": 9000000, "replica": 1, "causal_context": {"frontier": []}}])
        return record

    def read(self, pattern: Pattern, *, limit: int | None = None) -> list[TupleRecord]:
        raise NotImplementedError("AetherHttpBackend.read requires kernel-backed tuple_visible rules")

    def claim(self, pattern: Pattern, *, owner: str, ttl_seconds: float | None = None) -> Claim | None:
        raise NotImplementedError("AetherHttpBackend.claim requires kernel-backed claim rules")

    def release(self, claim: Claim, *, reason: str = "released") -> None:
        raise NotImplementedError("AetherHttpBackend.release requires kernel-backed release rules")

    def complete(self, claim: Claim, *, result: dict[str, Any] | None = None) -> TupleRecord:
        raise NotImplementedError("AetherHttpBackend.complete requires kernel-backed completion rules")

    def explain(self, tuple_id: str) -> dict[str, Any]:
        return {"tuple_id": tuple_id, "note": "use AETHER explain once tuple facade facts are derived by kernel rules"}
