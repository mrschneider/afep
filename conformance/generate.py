#!/usr/bin/env python3
"""Regenerates conformance/vectors.json from a FIXED test seed.

RUN ONLY after an intentional, versioned change to the signed-core
semantics or the wire schema: the vectors are byte-locked on purpose
(see conformance/verify.py and internal/identity/conformance_test.go).
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "python"))

from afep.v1 import agent_card_pb2, task_pb2  # noqa: E402
from afep import identity  # noqa: E402

# TEST KEY - NOT FOR PRODUCTION. Fixed 32-byte ed25519 seed.
SEED = bytes(range(32))

CARD_ID = "vector/agent"
DISPLAY = "AFEP vector"
ISSUED_AT = 1700000000


def build_envelope() -> task_pb2.TaskEnvelope:
    env = task_pb2.TaskEnvelope(
        task_id="vector-task",
        root_task_id="vector-task",
        tenant="tenant:vector",
        issuer_agent=CARD_ID,
        capability="vector.cap",
        payload_type="application/x-afep-vector",
        payload=b"afep-conformance-payload",
        state=task_pb2.TASK_STATE_PROBE,
    )
    env.deadline.start_unix = 1700000000
    env.deadline.expires_unix = 1700003600
    return env


def build_bid() -> task_pb2.Bid:
    return task_pb2.Bid(
        agent_id=CARD_ID,
        task_id="vector-task",
        score=42,
        rationale="vector",
        proposed_deadline_unix=1700003600,
    )


def main() -> None:
    key = identity.from_seed(SEED)

    card = agent_card_pb2.AgentCard(
        id=CARD_ID, display_name=DISPLAY, issued_at_unix=ISSUED_AT
    )
    card.public_key = key.public_key().public_bytes_raw()
    identity.sign_card(card, key)

    env = build_envelope()
    identity.sign_envelope(env, key)

    bid = build_bid()
    identity.sign_bid(bid, key)

    card_body = identity._with_cleared_signature(card, "signature")
    vectors = {
        "note": "AFEP cross-language conformance vectors. TEST KEY - NOT FOR "
        "PRODUCTION. Byte-locked: regenerate ONLY with this script after an "
        "intentional, versioned signed-core/wire change.",
        "version": 1,
        "seed_hex": SEED.hex(),
        "card": {
            "id": card.id,
            "display_name": card.display_name,
            "issued_at_unix": card.issued_at_unix,
            "public_key_hex": bytes(card.public_key).hex(),
            "signed_body_hex": card_body.hex(),
            "signature_hex": bytes(card.signature).hex(),
        },
        "envelope": {
            "task_id": env.task_id,
            "root_task_id": env.root_task_id,
            "tenant": env.tenant,
            "issuer_agent": env.issuer_agent,
            "capability": env.capability,
            "payload_type": env.payload_type,
            "payload_hex": bytes(env.payload).hex(),
            "deadline_start_unix": env.deadline.start_unix,
            "deadline_expires_unix": env.deadline.expires_unix,
            "state": int(env.state),
            "signed_core_hex": identity._envelope_body(env).hex(),
            "signature_hex": bytes(env.signature).hex(),
        },
        "bid": {
            "agent_id": bid.agent_id,
            "task_id": bid.task_id,
            "score": bid.score,
            "rationale": bid.rationale,
            "proposed_deadline_unix": bid.proposed_deadline_unix,
            "signed_body_hex": identity._with_cleared_signature(bid, "signature").hex(),
            "signature_hex": bytes(bid.signature).hex(),
        },
        "subject": {
            "domain": "vector",
            "agent": "tenant/vector",
            "verb": "envelopes",
            "subject": "afep.vector.tenant/vector.envelopes",
            "dedup_key": "vector-task:TASK_STATE_PROBE:0",
        },
    }
    out = os.path.join(HERE, "vectors.json")
    with open(out, "w") as f:
        json.dump(vectors, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()