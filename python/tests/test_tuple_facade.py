import unittest

from aether_tuple_facade import InMemoryBackend, TupleSpace


class TupleFacadeTests(unittest.TestCase):
    def test_out_and_read_support_wildcards(self):
        space = TupleSpace(InMemoryBackend())
        task = space.out("task", "case-501", "draft-response", payload={"priority": 7})
        space.out("evidence", "case-501", "runbook")

        rows = space.read(("task", None, "draft-response"))

        self.assertEqual([task], rows)

    def test_in_is_a_leased_claim_not_delete(self):
        space = TupleSpace(InMemoryBackend())
        task = space.out("task", "case-501", "draft-response")

        claim = space.in_(("task", None, None), owner="triage-agent")

        self.assertIsNotNone(claim)
        self.assertEqual(task.tuple_id, claim.tuple_id)
        self.assertEqual([], space.read(("task", None, None)))
        explanation = space.explain(task.tuple_id)
        self.assertEqual(["tuple_open", "tuple_claim"], [event["event"] for event in explanation["events"]])

    def test_complete_fences_stale_claims(self):
        backend = InMemoryBackend()
        space = TupleSpace(backend)
        task = space.out("task", "case-501", "draft-response")
        claim = space.claim(("task", None, None), owner="triage-agent")

        completion = space.complete(claim, result={"status": "done"})

        self.assertEqual(("completion", task.tuple_id, "triage-agent", 1), completion.fields)
        self.assertEqual([], space.read(("task", None, None)))
        self.assertIn("tuple_complete", [event["event"] for event in space.explain(task.tuple_id)["events"]])


if __name__ == "__main__":
    unittest.main()
