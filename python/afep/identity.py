"""ed25519 signing for AFEP AgentCards, Bids and TaskEnvelopes.

Byte-for-byte mirror of internal/identity (Go): signatures cover the
protobuf deterministic serialization with the signature field cleared.
"""

from __future__ import annotations

import time

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from afep.v1 import agent_card_pb2, task_pb2


class IdentityError(Exception):
    """Raised on structural or signature failures."""


def generate() -> tuple[bytes, bytes]:
    """Return (seed32, public_key32) for a fresh identity."""
    key = Ed25519PrivateKey.generate()
    return key.private_bytes_raw(), key.public_key().public_bytes_raw()


def from_seed(seed: bytes) -> Ed25519PrivateKey:
    if len(seed) != 32:
        raise IdentityError(f"seed must be 32 bytes, got {len(seed)}")
    return Ed25519PrivateKey.from_private_bytes(seed)


def new_card(card_id: str, display_name: str, key: Ed25519PrivateKey, mutate=None) -> agent_card_pb2.AgentCard:
    if not card_id:
        raise IdentityError("empty card id")
    card = agent_card_pb2.AgentCard()
    card.id = card_id
    card.display_name = display_name
    card.public_key = key.public_key().public_bytes_raw()
    card.issued_at_unix = int(time.time())
    if mutate is not None:
        mutate(card)
    sign_card(card, key)
    return card


def _with_cleared_signature(msg, field: str) -> bytes:
    saved = bytes(getattr(msg, field))
    setattr(msg, field, b"")
    try:
        return msg.SerializeToString(deterministic=True)
    finally:
        setattr(msg, field, saved)


def sign_card(card: agent_card_pb2.AgentCard, key: Ed25519PrivateKey) -> None:
    if bytes(card.public_key) != key.public_key().public_bytes_raw():
        raise IdentityError("card public key does not match key")
    card.signature = key.sign(_with_cleared_signature(card, "signature"))


def verify_card(card: agent_card_pb2.AgentCard) -> None:
    if not card.id:
        raise IdentityError("card has no id")
    if len(card.public_key) != 32:
        raise IdentityError(f"public key must be 32 bytes, got {len(card.public_key)}")
    if len(card.signature) != 64:
        raise IdentityError(f"signature must be 64 bytes, got {len(card.signature)}")
    pub = Ed25519PublicKey.from_public_bytes(bytes(card.public_key))
    try:
        pub.verify(card.signature, _with_cleared_signature(card, "signature"))
    except InvalidSignature as exc:
        raise IdentityError("bad card signature") from exc


def sign_bid(bid: task_pb2.Bid, key: Ed25519PrivateKey) -> None:
    if not bid.agent_id:
        raise IdentityError("bid has no agent id")
    bid.signature = key.sign(_with_cleared_signature(bid, "signature"))


def verify_bid(bid: task_pb2.Bid, card: agent_card_pb2.AgentCard) -> None:
    if bid.agent_id != card.id:
        raise IdentityError(f"bid agent {bid.agent_id!r} does not match card {card.id!r}")
    if not bid.signature:
        raise IdentityError("bid is unsigned")
    verify_card(card)
    pub = Ed25519PublicKey.from_public_bytes(bytes(card.public_key))
    try:
        pub.verify(bid.signature, _with_cleared_signature(bid, "signature"))
    except InvalidSignature as exc:
        raise IdentityError("bad bid signature") from exc


def _envelope_body(env: task_pb2.TaskEnvelope) -> bytes:
    """Signed core: request essence (ids, tenant, issuer, capability,
    payload, deadline). The lifecycle tail (state, assignee, bids,
    updated_at, escalation, hops) is relay-mutable and excluded —
    byte-parity mirror of internal/identity (Go)."""
    saved_sig = bytes(env.signature)
    saved_state = env.state
    saved_assignee = env.assignee_agent
    saved_updated = env.updated_at_unix
    saved_hops = env.hops
    saved_bids = []
    for b in env.bids:
        m = task_pb2.Bid()
        m.CopyFrom(b)
        saved_bids.append(m)
    saved_esc = task_pb2.Escalation()
    has_esc = env.HasField("escalation")
    if has_esc:
        saved_esc.CopyFrom(env.escalation)
    try:
        env.signature = b""
        env.state = task_pb2.TASK_STATE_UNSPECIFIED
        env.assignee_agent = ""
        env.updated_at_unix = 0
        env.hops = 0
        del env.bids[:]
        env.ClearField("escalation")
        return env.SerializeToString(deterministic=True)
    finally:
        env.signature = saved_sig
        env.state = saved_state
        env.assignee_agent = saved_assignee
        env.updated_at_unix = saved_updated
        env.hops = saved_hops
        env.bids.extend(saved_bids)
        if has_esc:
            env.escalation.CopyFrom(saved_esc)


def sign_envelope(env: task_pb2.TaskEnvelope, key: Ed25519PrivateKey) -> None:
    if not env.task_id:
        raise IdentityError("envelope has no task id")
    env.signature = key.sign(_envelope_body(env))


def verify_envelope(env: task_pb2.TaskEnvelope, issuer: agent_card_pb2.AgentCard) -> None:
    if not env.task_id:
        raise IdentityError("envelope has no task id")
    if not env.signature:
        raise IdentityError("envelope is unsigned")
    verify_card(issuer)
    pub = Ed25519PublicKey.from_public_bytes(bytes(issuer.public_key))
    try:
        pub.verify(env.signature, _envelope_body(env))
    except InvalidSignature as exc:
        raise IdentityError("bad envelope signature") from exc
