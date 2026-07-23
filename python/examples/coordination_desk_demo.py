from __future__ import annotations

from pprint import pprint

from aether_tuple_facade import CoordinationDesk, InMemoryBackend, TupleSpace


def main() -> None:
    desk = CoordinationDesk(TupleSpace(InMemoryBackend()))

    runbook = desk.add_evidence(
        "case-501",
        "migration-credit-runbook",
        uri="sidecar://runbook/migration-credit",
        payload={"confidence": 0.93},
    )
    prior_case = desk.add_evidence(
        "case-501",
        "similar-prior-case",
        uri="sidecar://case/227",
        payload={"match_score": 0.88},
    )

    desk.submit_task(
        "case-501",
        "apply-migration-credit",
        priority=9,
        evidence=[runbook, prior_case],
        payload={"customer_tier": "pro", "requested_by": "support-agent"},
    )
    desk.submit_task(
        "case-501",
        "escalate-to-billing-specialist",
        priority=4,
        evidence=[prior_case],
    )

    print("=== Queue summary before claim ===")
    pprint(desk.summary())

    print("\n=== Ready tasks, highest priority first ===")
    pprint(desk.ready_tasks(case_id="case-501"))

    claim = desk.claim_next(owner="lead-ana", case_id="case-501")
    assert claim is not None
    print("\n=== Claimed work ===")
    pprint(claim)

    completion = desk.complete_claim(
        claim,
        result={
            "selected_resolution": "apply-migration-credit",
            "artifact": "draft://resolution-901",
            "operator_note": "Evidence supports migration credit path.",
        },
    )
    print("\n=== Completion ===")
    pprint(completion)

    print("\n=== Queue summary after completion ===")
    pprint(desk.summary())

    print("\n=== Explain selected task ===")
    pprint(desk.explain(claim.tuple_id))


if __name__ == "__main__":
    main()
