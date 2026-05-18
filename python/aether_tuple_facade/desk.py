from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .core import Claim, TupleRecord, TupleSpace


@dataclass(slots=True, frozen=True)
class DeskTask:
    """Operator-facing view of a task tuple."""

    tuple_id: str
    case_id: str
    action: str
    priority: int
    payload: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class DeskEvidence:
    """Operator-facing view of an evidence tuple."""

    tuple_id: str
    case_id: str
    label: str
    uri: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class DeskSummary:
    """Small state report for an operator or notebook."""

    open_tasks: int
    open_evidence: int
    completed: int
    priorities: dict[int, int]


class CoordinationDesk:
    """Useful workflow utility built on the tuple-space facade.

    The desk gives a concrete shape to the facade: cases accumulate evidence,
    tasks become claimable work, and completions retain a provenance trail.
    It is designed for demos, notebooks, and early agent experiments before the
    same semantics are moved under kernel-backed AETHER rules.
    """

    def __init__(self, space: TupleSpace) -> None:
        self.space = space

    def add_evidence(
        self,
        case_id: str,
        label: str,
        *,
        uri: str,
        payload: dict[str, Any] | None = None,
    ) -> DeskEvidence:
        record = self.space.out(
            "evidence",
            case_id,
            label,
            payload={"uri": uri, **dict(payload or {})},
        )
        return DeskEvidence(
            tuple_id=record.tuple_id,
            case_id=case_id,
            label=label,
            uri=uri,
            payload=record.payload,
        )

    def submit_task(
        self,
        case_id: str,
        action: str,
        *,
        priority: int = 0,
        payload: dict[str, Any] | None = None,
        evidence: list[DeskEvidence | str] | None = None,
    ) -> DeskTask:
        evidence_ids = [item.tuple_id if isinstance(item, DeskEvidence) else item for item in evidence or []]
        record = self.space.out(
            "task",
            case_id,
            action,
            priority,
            payload={"evidence_ids": evidence_ids, **dict(payload or {})},
        )
        return self._to_task(record)

    def ready_tasks(self, *, case_id: str | None = None, action: str | None = None) -> list[DeskTask]:
        pattern = ("task", case_id, action, None)
        tasks = [self._to_task(record) for record in self.space.read(pattern)]
        return sorted(tasks, key=lambda task: (-task.priority, task.case_id, task.action))

    def claim_next(
        self,
        *,
        owner: str,
        case_id: str | None = None,
        action: str | None = None,
        ttl_seconds: float | None = 60.0,
    ) -> Claim | None:
        tasks = self.ready_tasks(case_id=case_id, action=action)
        for task in tasks:
            claim = self.space.claim(("task", task.case_id, task.action, task.priority), owner=owner, ttl_seconds=ttl_seconds)
            if claim is not None:
                return claim
        return None

    def complete_claim(self, claim: Claim, *, result: dict[str, Any]) -> TupleRecord:
        return self.space.complete(claim, result=result)

    def evidence_for(self, case_id: str) -> list[DeskEvidence]:
        records = self.space.read(("evidence", case_id, None))
        return [self._to_evidence(record) for record in records]

    def summary(self) -> DeskSummary:
        tasks = self.ready_tasks()
        evidence = self.space.read(("evidence", None, None))
        completions = self.space.read(("completion", None, None, None))
        priorities: dict[int, int] = {}
        for task in tasks:
            priorities[task.priority] = priorities.get(task.priority, 0) + 1
        return DeskSummary(
            open_tasks=len(tasks),
            open_evidence=len(evidence),
            completed=len(completions),
            priorities=priorities,
        )

    def explain(self, tuple_id: str) -> dict[str, Any]:
        return self.space.explain(tuple_id)

    @staticmethod
    def _to_task(record: TupleRecord) -> DeskTask:
        kind, case_id, action, priority = record.fields
        if kind != "task":
            raise ValueError(f"not a task tuple: {record.fields!r}")
        return DeskTask(
            tuple_id=record.tuple_id,
            case_id=str(case_id),
            action=str(action),
            priority=int(priority),
            payload=dict(record.payload),
            metadata=dict(record.metadata),
        )

    @staticmethod
    def _to_evidence(record: TupleRecord) -> DeskEvidence:
        kind, case_id, label = record.fields
        if kind != "evidence":
            raise ValueError(f"not an evidence tuple: {record.fields!r}")
        return DeskEvidence(
            tuple_id=record.tuple_id,
            case_id=str(case_id),
            label=str(label),
            uri=str(record.payload.get("uri", "")),
            payload=dict(record.payload),
        )
