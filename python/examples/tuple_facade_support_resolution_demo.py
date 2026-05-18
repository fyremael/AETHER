from __future__ import annotations

from pprint import pprint

from aether_tuple_facade import InMemoryBackend, TupleSpace


def main() -> None:
    space = TupleSpace(InMemoryBackend())

    case = space.out("case", "case-501", "duplicate-charge", payload={"priority": "high"})
    evidence = space.out("evidence", "case-501", "migration-credit-runbook", payload={"source": "sidecar://runbook/17"})
    task = space.out(
        "task",
        "case-501",
        "draft-resolution",
        payload={"requires": [case.tuple_id, evidence.tuple_id]},
        metadata={"demo": "support-resolution"},
    )

    print("ready tasks")
    pprint(space.read(("task", "case-501", None)))

    claim = space.claim(("task", "case-501", "draft-resolution"), owner="triage-agent", ttl_seconds=30)
    assert claim is not None
    print("claim")
    pprint(claim)

    completion = space.complete(
        claim,
        result={"resolution": "apply-migration-credit", "artifact": "draft://resolution-901"},
    )
    print("completion")
    pprint(completion)

    print("explanation")
    pprint(space.explain(task.tuple_id))


if __name__ == "__main__":
    main()
