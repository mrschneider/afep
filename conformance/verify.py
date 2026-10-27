#!/usr/bin/env python3
"""Byte-verifies conformance/vectors.json against the Python SDK.

Exit 0 = vectors and code agree. Any drift (schema change, signed-core
change, silent re-sign) fails the build until generate.py is run
deliberately as part of a versioned release.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "python"))

from afep.v1 import agent_card_pb2, task_pb2  # noqa: E402
from afep import identity  # noqa: E402


def fail(msg: str) -> None:
    print(f"CONFORMANCE FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    with open(os.path.join(HERE, "vectors.json")) as f:
        v = json.load(f)

    seed = bytes.fromhex(v["seed_hex"])
    key = identity.from_seed(seed)

    # card
    c = v["card"]
    card = agent_card_pb2.AgentCard(id=c["id"], display_name=c["display_name"], issued_at_unix=c["issued_at_unix"])
    card.public_key = bytes.fromhex(c["public_key_hex"])
    body = identity._with_cleared_signature(card, "signature")
    if body.hex() != c["signed_body_hex"]:
        fail("card signed body drifted")
    if key.public_key().public_bytes_raw().hex() != c["public_key_hex"]:
        fail("public key does not match seed")
    card.signature = bytes.fromhex(c["signature_hex"])
    identity.verify_card(card)  # raises on drift

    # envelope (rebuild exactly like generate.py)
    e = v["envelope"]
    env = task_pb2.TaskEnvelope(
        task_id=e["task_id"], root_task_id=e["root_task_id"], tenant=e["tenant"],
        issuer_agent=e["issuer_agent"], capability=e["capability"],
        payload_type=e["payload_type"], payload=bytes.fromhex(e["payload_hex"]),
        state=e["state"],
    )
    env.deadline.start_unix = e["deadline_start_unix"]
    env.deadline.expires_unix = e["deadline_expires_unix"]
    core = identity._envelope_body(env)
    if core.hex() != e["signed_core_hex"]:
        fail("envelope signed core drifted")
    env.signature = bytes.fromhex(e["signature_hex"])
    identity.verify_envelope(env, card)

    # signed-core EXCLUDES the relay-mutable tail: mutating it must not
    # change a single byte of the core.
    env.state = task_pb2.TASK_STATE_ESCALATED
    env.hops = 7
    env.assignee_agent = "someone/else"
    if identity._envelope_body(env).hex() != e["signed_core_hex"]:
        fail("tail mutation changed the signed core (exclusion broken)")

    # bid
    b = v["bid"]
    bid = task_pb2.Bid(agent_id=b["agent_id"], task_id=b["task_id"], score=b["score"],
                       rationale=b["rationale"], proposed_deadline_unix=b["proposed_deadline_unix"])
    body = identity._with_cleared_signature(bid, "signature")
    if body.hex() != b["signed_body_hex"]:
        fail("bid signed body drifted")
    bid.signature = bytes.fromhex(b["signature_hex"])
    identity.verify_bid(bid, card)

    # subject/dedup key literals
    s = v["subject"]
    built = f"afep.{s['domain']}.{s['agent']}.{s['verb']}"
    if built != s["subject"]:
        fail(f"subject formula drifted: {built} != {s['subject']}")
    if f"{s['subject'].split('.')[1]}":  # keep formula explicit
        pass
    expected_dedup = f"{e['task_id']}:TASK_STATE_PROBE:0"
    if expected_dedup != s["dedup_key"]:
        fail(f"dedup key formula drifted: {expected_dedup} != {s['dedup_key']}")

    print("conformance: all vectors verified (python)")


if __name__ == "__main__":
    main()