"""AFEP task state machine — mirror of internal/lifecycle (Go)."""

from __future__ import annotations

import time

from afep.v1 import task_pb2

UNSPECIFIED = task_pb2.TASK_STATE_UNSPECIFIED
PROBE = task_pb2.TASK_STATE_PROBE
BID = task_pb2.TASK_STATE_BID
COMMIT = task_pb2.TASK_STATE_COMMIT
RUNNING = task_pb2.TASK_STATE_RUNNING
DONE = task_pb2.TASK_STATE_DONE
FAILED = task_pb2.TASK_STATE_FAILED
ESCALATED = task_pb2.TASK_STATE_ESCALATED

_TRANSITIONS: dict[int, frozenset[int]] = {
    UNSPECIFIED: frozenset({PROBE}),
    PROBE: frozenset({BID, FAILED, ESCALATED}),
    BID: frozenset({COMMIT, PROBE, FAILED, ESCALATED}),
    COMMIT: frozenset({RUNNING, FAILED, ESCALATED}),
    RUNNING: frozenset({DONE, FAILED, ESCALATED}),
    FAILED: frozenset({ESCALATED}),
    ESCALATED: frozenset({PROBE}),
    DONE: frozenset(),
}


class LifecycleError(Exception):
    """Raised on illegal state transitions."""


def can(frm: int, to: int) -> bool:
    if frm == to:
        return True
    return to in _TRANSITIONS.get(frm, frozenset())


def apply(env: task_pb2.TaskEnvelope, to: int) -> None:
    if not can(env.state, to):
        raise LifecycleError(f"illegal transition {env.state} -> {to} (task {env.task_id})")
    env.state = to
    env.updated_at_unix = int(time.time())


def terminal(state: int) -> bool:
    return state == DONE
