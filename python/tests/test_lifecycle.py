import unittest

from afep import lifecycle
from afep.v1 import task_pb2


class LifecycleTests(unittest.TestCase):
    def test_legal_transitions(self):
        legal = [
            (lifecycle.UNSPECIFIED, lifecycle.PROBE),
            (lifecycle.PROBE, lifecycle.BID),
            (lifecycle.BID, lifecycle.COMMIT),
            (lifecycle.COMMIT, lifecycle.RUNNING),
            (lifecycle.RUNNING, lifecycle.DONE),
            (lifecycle.RUNNING, lifecycle.FAILED),
            (lifecycle.FAILED, lifecycle.ESCALATED),
            (lifecycle.ESCALATED, lifecycle.PROBE),
            (lifecycle.BID, lifecycle.PROBE),
            (lifecycle.PROBE, lifecycle.PROBE),  # idempotent
        ]
        for frm, to in legal:
            self.assertTrue(lifecycle.can(frm, to), f"{frm} -> {to} should be legal")

    def test_illegal_transitions(self):
        illegal = [
            (lifecycle.UNSPECIFIED, lifecycle.BID),
            (lifecycle.PROBE, lifecycle.COMMIT),
            (lifecycle.PROBE, lifecycle.DONE),
            (lifecycle.DONE, lifecycle.PROBE),
            (lifecycle.DONE, lifecycle.FAILED),
            (lifecycle.COMMIT, lifecycle.BID),
            (lifecycle.ESCALATED, lifecycle.DONE),
        ]
        for frm, to in illegal:
            self.assertFalse(lifecycle.can(frm, to), f"{frm} -> {to} should be illegal")

    def test_apply_success_stamps(self):
        env = task_pb2.TaskEnvelope(task_id="t1", state=lifecycle.PROBE)
        lifecycle.apply(env, lifecycle.BID)
        self.assertEqual(env.state, lifecycle.BID)
        self.assertGreater(env.updated_at_unix, 0)

    def test_apply_failure_does_not_mutate(self):
        env = task_pb2.TaskEnvelope(task_id="t1", state=lifecycle.BID)
        with self.assertRaises(lifecycle.LifecycleError):
            lifecycle.apply(env, lifecycle.DONE)
        self.assertEqual(env.state, lifecycle.BID)

    def test_terminal(self):
        self.assertTrue(lifecycle.terminal(lifecycle.DONE))
        self.assertFalse(lifecycle.terminal(lifecycle.RUNNING))


if __name__ == "__main__":
    unittest.main()
