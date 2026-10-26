"""Cross-language interop: verifies the committed Go fixtures and (with
AFEP_FIXTURES=1) regenerates the Python ones for the Go suite to check."""

import hashlib
import json
import os
import pathlib
import unittest

from afep import identity, lifecycle
from afep.v1 import agent_card_pb2, task_pb2

TESTDATA = pathlib.Path(__file__).resolve().parent.parent / "testdata"

SEED_PY = hashlib.sha256(b"afep-interop-fixture-python").digest()


def _load(name: str) -> dict | None:
    path = TESTDATA / name
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _fill_card(doc: dict, card: agent_card_pb2.AgentCard) -> None:
    card.id = doc["id"]
    card.display_name = doc["display_name"]
    card.public_key = bytes.fromhex(doc["public_key"])
    ep = doc["endpoint"]
    card.endpoint.grpc_address = ep["grpc_address"]
    card.endpoint.nats_subject_prefix = ep["nats_subject_prefix"]
    card.endpoint.nats_gateways.extend(ep.get("nats_gateways") or [])
    for cap in doc["capabilities"]:
        c = card.capabilities.add()
        c.name = cap["name"]
        c.version = cap["version"]
    card.security_clearances.extend(doc.get("security_clearances") or [])
    for k, v in (doc.get("metadata") or {}).items():
        card.metadata[k] = v
    card.issued_at_unix = doc["issued_at_unix"]
    card.expires_at_unix = doc["expires_at_unix"]
    card.signature = bytes.fromhex(doc["signature"])


def _fill_env(doc: dict, env: task_pb2.TaskEnvelope) -> None:
    env.task_id = doc["task_id"]
    env.root_task_id = doc["root_task_id"]
    env.tenant = doc["tenant"]
    env.issuer_agent = doc["issuer_agent"]
    env.assignee_agent = doc["assignee_agent"]
    env.state = doc["state"]
    env.capability = doc["capability"]
    env.payload_type = doc["payload_type"]
    env.payload = bytes.fromhex(doc["payload"])
    if doc.get("deadline"):
        env.deadline.start_unix = doc["deadline"]["start_unix"]
        env.deadline.expires_unix = doc["deadline"]["expires_unix"]
    if doc.get("escalation"):
        esc = env.escalation
        esc.from_domain = doc["escalation"]["from_domain"]
        esc.to_domain = doc["escalation"]["to_domain"]
        esc.reason_code = doc["escalation"]["reason_code"]
        esc.detail = doc["escalation"]["detail"]
    env.updated_at_unix = doc["updated_at_unix"]
    env.hops = doc["hops"]
    env.signature = bytes.fromhex(doc["signature"])


class GoFixtureTests(unittest.TestCase):
    def test_go_card_verifies(self):
        doc = _load("go_card.json")
        if doc is None:
            self.skipTest("go fixtures not generated yet")
        card = agent_card_pb2.AgentCard()
        _fill_card(doc, card)
        identity.verify_card(card)

    def test_go_envelope_verifies(self):
        doc_card = _load("go_card.json")
        doc_env = _load("go_envelope.json")
        if doc_card is None or doc_env is None:
            self.skipTest("go fixtures not generated yet")
        card = agent_card_pb2.AgentCard()
        _fill_card(doc_card, card)
        env = task_pb2.TaskEnvelope()
        _fill_env(doc_env, env)
        identity.verify_envelope(env, card)


class PythonFixtureTests(unittest.TestCase):
    def _build(self):
        key = identity.from_seed(SEED_PY)
        def mutate(c):
            c.security_clearances.append("tenant:acme")
            cap = c.capabilities.add()
            cap.name = "vision.tag"
            cap.version = "9.9"
            c.endpoint.grpc_address = "py.example.com:7700"
            c.endpoint.nats_subject_prefix = "afep.py.interop"
            c.metadata["description"] = "python interop agent"
            c.metadata["version"] = "3.1.4"
            c.issued_at_unix = 1793000000
            c.expires_at_unix = 1793600000

        card = identity.new_card("py/interop", "Py Interop", key, mutate)
        env = task_pb2.TaskEnvelope(
            task_id="py-task-1", root_task_id="py-root-1", tenant="acme",
            issuer_agent="py/interop", assignee_agent="py/worker",
            state=lifecycle.BID, capability="vision.tag",
            payload_type="application/json", payload=b'{"n":7}',
            updated_at_unix=1793000100, hops=2,
        )
        env.deadline.start_unix = 1793000050
        env.deadline.expires_unix = 1793003650
        esc = env.escalation
        esc.from_domain = "beta"
        esc.to_domain = "alpha"
        esc.reason_code = "NO_BIDS"
        esc.detail = "hops=2"
        identity.sign_envelope(env, key)
        return card, env

    def test_python_fixtures_sign_and_verify(self):
        card, env = self._build()
        identity.verify_card(card)
        identity.verify_envelope(env, card)

    def test_committed_python_fixtures_if_present(self):
        doc_card = _load("python_card.json")
        doc_env = _load("python_envelope.json")
        if doc_card is None or doc_env is None:
            self.skipTest("python fixtures not generated yet")
        card = agent_card_pb2.AgentCard()
        _fill_card(doc_card, card)
        identity.verify_card(card)
        env = task_pb2.TaskEnvelope()
        _fill_env(doc_env, env)
        identity.verify_envelope(env, card)
        # committed fixtures must match a fresh build byte-for-byte
        fresh_card, fresh_env = self._build()
        self.assertEqual(card.SerializeToString(deterministic=True),
                         fresh_card.SerializeToString(deterministic=True))
        self.assertEqual(env.SerializeToString(deterministic=True),
                         fresh_env.SerializeToString(deterministic=True))

    def test_generate_on_demand(self):
        if os.environ.get("AFEP_FIXTURES") != "1":
            self.skipTest("AFEP_FIXTURES not set")
        card, env = self._build()
        TESTDATA.mkdir(parents=True, exist_ok=True)

        def card_doc(c):
            return {
                "id": c.id, "display_name": c.display_name,
                "public_key": c.public_key.hex(),
                "endpoint": {
                    "grpc_address": c.endpoint.grpc_address,
                    "nats_subject_prefix": c.endpoint.nats_subject_prefix,
                    "nats_gateways": list(c.endpoint.nats_gateways),
                },
                "capabilities": [{"name": x.name, "version": x.version} for x in c.capabilities],
                "security_clearances": list(c.security_clearances),
                "metadata": dict(c.metadata),
                "issued_at_unix": c.issued_at_unix,
                "expires_at_unix": c.expires_at_unix,
                "signature": c.signature.hex(),
            }

        def env_doc(e):
            esc = None
            if e.HasField("escalation"):
                esc = {
                    "from_domain": e.escalation.from_domain,
                    "to_domain": e.escalation.to_domain,
                    "reason_code": e.escalation.reason_code,
                    "detail": e.escalation.detail,
                }
            dl = None
            if e.HasField("deadline"):
                dl = {"start_unix": e.deadline.start_unix, "expires_unix": e.deadline.expires_unix}
            return {
                "task_id": e.task_id, "root_task_id": e.root_task_id,
                "tenant": e.tenant, "issuer_agent": e.issuer_agent,
                "assignee_agent": e.assignee_agent, "state": e.state,
                "capability": e.capability, "payload_type": e.payload_type,
                "payload": e.payload.hex(), "deadline": dl, "escalation": esc,
                "updated_at_unix": e.updated_at_unix, "hops": e.hops,
                "signature": e.signature.hex(),
            }

        (TESTDATA / "python_card.json").write_text(json.dumps(card_doc(card), indent=2, sort_keys=True) + "\n")
        (TESTDATA / "python_envelope.json").write_text(json.dumps(env_doc(env), indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    unittest.main()
