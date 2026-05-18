from __future__ import annotations

from dataclasses import dataclass, field
from time import time
from typing import Any, Protocol
from uuid import uuid4

Pattern = tuple[Any, ...]


@dataclass(slots=True, frozen=True)
class TupleRecord:
    """A facade tuple as seen by Python clients.

    ``fields`` intentionally keeps the Linda-like user shape. Metadata remains
    outside the fields so matching stays simple and predictable.
    """

    tuple_id: str
    fields: tuple[Any, ...]
    payload: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time)


@dataclass(slots=True, frozen=True)
class Claim:
    """A leased claim over a tuple.

    This is the AETHER-safe replacement for destructive Linda ``in``. The tuple
    remains part of replayable history; the claim records ownership and a lease
    epoch so stale completions can be fenced by the semantic kernel.
    """

    claim_id: str
    tuple_id: str
    owner: str
    lease_epoch: int
    expires_at: float | None = None


class TupleBackend(Protocol):
    def out(
        self,
        fields: tuple[Any, ...],
        *,
        payload: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TupleRecord: ...

    def read(self, pattern: Pattern, *, limit: int | None = None) -> list[TupleRecord]: ...

    def claim(
        self,
        pattern: Pattern,
        *,
        owner: str,
        ttl_seconds: float | None = None,
    ) -> Claim | None: ...

    def release(self, claim: Claim, *, reason: str = "released") -> None: ...

    def complete(self, claim: Claim, *, result: dict[str, Any] | None = None) -> TupleRecord: ...

    def explain(self, tuple_id: str) -> dict[str, Any]: ...


class TupleSpace:
    """Small Pythonic facade over an AETHER-compatible backend."""

    def __init__(self, backend: TupleBackend) -> None:
        self._backend = backend

    def out(
        self,
        *fields: Any,
        payload: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TupleRecord:
        if not fields:
            raise ValueError("tuple must contain at least one field")
        return self._backend.out(tuple(fields), payload=payload, metadata=metadata)

    def read(self, pattern: Pattern, *, limit: int | None = None) -> list[TupleRecord]:
        return self._backend.read(pattern, limit=limit)

    def claim(
        self,
        pattern: Pattern,
        *,
        owner: str,
        ttl_seconds: float | None = None,
    ) -> Claim | None:
        if not owner:
            raise ValueError("owner is required for claim")
        return self._backend.claim(pattern, owner=owner, ttl_seconds=ttl_seconds)

    def in_(
        self,
        pattern: Pattern,
        *,
        owner: str,
        ttl_seconds: float | None = None,
    ) -> Claim | None:
        """Compatibility spelling for Linda ``in``.

        This does not delete. It creates a leased claim so AETHER can preserve
        append-only replay, provenance, and stale-result fencing.
        """

        return self.claim(pattern, owner=owner, ttl_seconds=ttl_seconds)

    def release(self, claim: Claim, *, reason: str = "released") -> None:
        self._backend.release(claim, reason=reason)

    def complete(self, claim: Claim, *, result: dict[str, Any] | None = None) -> TupleRecord:
        return self._backend.complete(claim, result=result)

    def explain(self, tuple_id: str) -> dict[str, Any]:
        return self._backend.explain(tuple_id)


def new_tuple_id(prefix: str = "tuple") -> str:
    return f"{prefix}-{uuid4().hex[:16]}"


def new_claim_id(prefix: str = "claim") -> str:
    return f"{prefix}-{uuid4().hex[:16]}"


def match_pattern(fields: tuple[Any, ...], pattern: Pattern) -> bool:
    if len(fields) != len(pattern):
        return False
    return all(expected is None or expected == actual for actual, expected in zip(fields, pattern))
