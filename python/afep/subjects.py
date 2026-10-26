"""NATS subject scheme — mirror of internal/bus/nats (Go): afep.<domain>.<agent>.<verb>."""

from __future__ import annotations

WILDCARD = "*"

VERB_ENVELOPES = "envelopes"
VERB_BIDS = "bids"
VERB_TELEMETRY = "telemetry"

_INVALID = set(" >.")


def subject(domain: str, agent: str, verb: str) -> str:
    for name, seg in (("domain", domain), ("agent", agent), ("verb", verb)):
        if not seg:
            raise ValueError(f"empty {name} segment")
        if any(ch in _INVALID for ch in seg):
            raise ValueError(f"invalid {name} segment {seg!r}")
    return f"afep.{domain}.{agent}.{verb}"


def parse_subject(value: str) -> tuple[str, str, str]:
    parts = value.split(".")
    if len(parts) != 4 or parts[0] != "afep":
        raise ValueError(f"bad subject {value!r}")
    return parts[1], parts[2], parts[3]
