import unittest

from aether_tuple_facade import CoordinationDesk, InMemoryBackend, TupleSpace


class CoordinationDeskTests(unittest.TestCase):
    def test_submit_claim_complete_and_explain(self):
        desk = CoordinationDesk(TupleSpace(InMemoryBackend()))
        evidence = desk.add_evidence(
            "case-501",
            "migration-credit-runbook",
            uri="sidecar://runbook/17",
        )
        task = desk.submit_task(
            "case-501",
            "apply-migration-credit",
            priority=9,
            evidence=[evidence],
        )

        self.assertEqual([task], desk.ready_tasks(case_id="case-501"))
        claim = desk.claim_next(owner="triage-agent", case_id="case-501")
        self.assertIsNotNone(claim)
        self.assertEqual([], desk.ready_tasks(case_id="case-501"))

        completion = desk.complete_claim(
            claim,
            result={"status": "accepted", "artifact": "draft://resolution-901"},
        )

        self.assertEqual(("completion", task.tuple_id, "triage-agent", 1), completion.fields)
        self.assertIn("tuple_complete", [event["event"] for event in desk.explain(task.tuple_id)["events"]])

    def test_claim_next_picks_highest_priority(self):
        desk = CoordinationDesk(TupleSpace(InMemoryBackend()))
        low = desk.submit_task("case-1", "low", priority=1)
        high = desk.submit_task("case-2", "high", priority=10)

        claim = desk.claim_next(owner="planner")

        self.assertIsNotNone(claim)
        self.assertEqual(high.tuple_id, claim.tuple_id)
        self.assertEqual([low], desk.ready_tasks())

    def test_summary_counts_visible_work(self):
        desk = CoordinationDesk(TupleSpace(InMemoryBackend()))
        desk.add_evidence("case-1", "runbook", uri="sidecar://runbook/1")
        desk.submit_task("case-1", "draft", priority=3)
        desk.submit_task("case-2", "escalate", priority=3)
        claim = desk.claim_next(owner="operator", case_id="case-1")
        desk.complete_claim(claim, result={"status": "done"})

        summary = desk.summary()

        self.assertEqual(1, summary.open_tasks)
        self.assertEqual(1, summary.open_evidence)
        self.assertEqual(1, summary.completed)
        self.assertEqual({3: 1}, summary.priorities)


if __name__ == "__main__":
    unittest.main()
