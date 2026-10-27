"""Byte-verifies conformance/vectors.json against the Python SDK.

Runs inside the normal suite (unittest discover): schema or
signed-core drift fails both languages until generate.py is run on
purpose as part of a versioned release.
"""
import json
import os
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.join(_HERE, "..", "..")
import sys

sys.path.insert(0, os.path.join(_ROOT, "python"))

from afep import identity  # noqa: E402
from afep.v1 import agent_card_pb2, task_pb2  # noqa: E402

with open(os.path.join(_ROOT, "conformance", "vectors.json")) as f:
    V = json.load(f)


class TestConformanceVectors(unittest.TestCase):
    def setUp(self):
        self.key = identity.from_seed(bytes.fromhex(V["seed_hex"]))

    def test_key_matches_seed(self):
        self.assertEqual(
            self.key.public_key().public_bytes_raw().hex(), V["card"]["public_key_hex"]
        )

    def test_card_vector(self):
        c = V["card"]
        card = agent_card_pb2.AgentCard(
            id=c["id"], display_name=c["display_name"], issued_at_unix=c["issued_at_unix"]
        )
        card.public_key = bytes.fromhex(c["public_key_hex"])
        body = identity._with_cleared_signature(card, "signature")
        self.assertEqual(body.hex(), c["signed_body_hex"], "card signed body drifted")
        card.signature = bytes.fromhex(c["signature_hex"])
        identity.verify_card(card)

    def test_envelope_vector_and_tail_exclusion(self):
        e = V["envelope"]
        env = task_pb2.TaskEnvelope(
            task_id=e["task_id"], root_task_id=e["root_task_id"], tenant=e["tenant"],
            issuer_agent=e["issuer_agent"], capability=e["capability"],
            payload_type=e["payload_type"], payload=bytes.fromhex(e["payload_hex"]),
            state=e["state"],
        )
        env.deadline.start_unix = e["deadline_start_unix"]
        env.deadline.expires_unix = e["deadline_expires_unix"]
        core = identity._envelope_body(env)
        self.assertEqual(core.hex(), e["signed_core_hex"], "signed core drifted")
        env.signature = bytes.fromhex(e["signature_hex"])
        issuer = agent_card_pb2.AgentCard(
            id=V["card"]["id"], display_name=V["card"]["display_name"],
            issued_at_unix=V["card"]["issued_at_unix"],
        )
        issuer.public_key = bytes.fromhex(V["card"]["public_key_hex"])
        issuer.signature = bytes.fromhex(V["card"]["signature_hex"])
        identity.verify_envelope(env, issuer)
        # relay-mutable tail must not touch the signed core
        env.state = task_pb2.TASK_STATE_ESCALATED
        env.hops = 7
        env.assignee_agent = "someone/else"
        self.assertEqual(
            identity._envelope_body(env).hex(), e["signed_core_hex"],
            "tail mutation changed the signed core",
        )

    def test_bid_vector(self):
        b = V["bid"]
        bid = task_pb2.Bid(
            agent_id=b["agent_id"], task_id=b["task_id"], score=b["score"],
            rationale=b["rationale"], proposed_deadline_unix=b["proposed_deadline_unix"],
        )
        body = identity._with_cleared_signature(bid, "signature")
        self.assertEqual(body.hex(), b["signed_body_hex"], "bid signed body drifted")
        bid.signature = bytes.fromhex(b["signature_hex"])
        issuer = agent_card_pb2.AgentCard(
            id=V["card"]["id"], display_name=V["card"]["display_name"],
            issued_at_unix=V["card"]["issued_at_unix"],
        )
        issuer.public_key = bytes.fromhex(V["card"]["public_key_hex"])
        issuer.signature = bytes.fromhex(V["card"]["signature_hex"])
        identity.verify_bid(bid, issuer)

    def test_subject_and_dedup_formulas(self):
        s = V["subject"]
        built = f"afep.{s['domain']}.{s['agent']}.{s['verb']}"
        self.assertEqual(built, s["subject"], "subject formula drifted")
        dedup = f"{V['envelope']['task_id']}:TASK_STATE_PROBE:0"
        self.assertEqual(dedup, s["dedup_key"], "dedup key formula drifted")


if __name__ == "__main__":
    unittest.main()