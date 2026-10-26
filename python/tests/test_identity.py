import unittest

from afep import identity
from afep.v1 import task_pb2


def _card(seed: bytes, card_id: str = "acme/agent"):
    key = identity.from_seed(seed)
    def mutate(c):
        c.security_clearances.append("tenant:acme")
        c.metadata["k"] = "v"

    card = identity.new_card(card_id, "Agent", key, mutate)
    return key, card


class CardTests(unittest.TestCase):
    def test_roundtrip(self):
        key, card = _card(b"\x11" * 32)
        identity.verify_card(card)  # must not raise

    def test_tamper_fails(self):
        _, card = _card(b"\x11" * 32)
        card.display_name = "mallory"
        with self.assertRaises(identity.IdentityError):
            identity.verify_card(card)

    def test_seed_restore(self):
        seed, pub = identity.generate()
        key = identity.from_seed(seed)
        self.assertEqual(key.public_key().public_bytes_raw(), pub)

    def test_short_seed(self):
        with self.assertRaises(identity.IdentityError):
            identity.from_seed(b"short")

    def test_sign_key_mismatch(self):
        k1 = identity.from_seed(b"\x11" * 32)
        k2 = identity.from_seed(b"\x22" * 32)
        card = identity.new_card("a/x", "x", k1)
        with self.assertRaises(identity.IdentityError):
            identity.sign_card(card, k2)


class BidTests(unittest.TestCase):
    def test_roundtrip_and_tamper(self):
        key, card = _card(b"\x33" * 32, "acme/provider")
        bid = task_pb2.Bid(agent_id="acme/provider", score=100, task_id="t1")
        identity.sign_bid(bid, key)
        identity.verify_bid(bid, card)
        bid.score = 999
        with self.assertRaises(identity.IdentityError):
            identity.verify_bid(bid, card)

    def test_unsigned_rejected(self):
        _, card = _card(b"\x33" * 32, "acme/provider")
        bid = task_pb2.Bid(agent_id="acme/provider")
        with self.assertRaises(identity.IdentityError):
            identity.verify_bid(bid, card)


class EnvelopeTests(unittest.TestCase):
    def _env(self) -> task_pb2.TaskEnvelope:
        env = task_pb2.TaskEnvelope(
            task_id="task-1", root_task_id="task-1", tenant="acme",
            state=task_pb2.TASK_STATE_PROBE, capability="vision.tag", hops=1,
        )
        env.deadline.start_unix = 1793000000
        env.deadline.expires_unix = 1793003600
        return env

    def test_roundtrip(self):
        key, card = _card(b"\x44" * 32, "acme/issuer")
        env = self._env()
        identity.sign_envelope(env, key)
        identity.verify_envelope(env, card)

    def test_tamper_hops_fails(self):
        key, card = _card(b"\x44" * 32, "acme/issuer")
        env = self._env()
        identity.sign_envelope(env, key)
        env.hops = 2
        with self.assertRaises(identity.IdentityError):
            identity.verify_envelope(env, card)

    def test_unsigned_rejected(self):
        _, card = _card(b"\x44" * 32, "acme/issuer")
        with self.assertRaises(identity.IdentityError):
            identity.verify_envelope(self._env(), card)


if __name__ == "__main__":
    unittest.main()
