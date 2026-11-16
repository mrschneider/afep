// Package lifecycle implements the AFEP task state machine.
package lifecycle

import (
	"fmt"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

// transitions is a bitmask per source state: bit N is set when N is a legal
// target. Same-state updates are always allowed (idempotent refreshes) and
// are handled in Can, not encoded here. The enum values are dense 0..7, so
// a uint8 covers the whole state space and Can becomes a single bitwise
// AND + shift instead of a slice scan on the hot Apply path.
//
// The bitmasks are the exact encoding of the previous slice table:
//
//	UNSPECIFIED: PROBE
//	PROBE:      BID, FAILED, ESCALATED
//	BID:        COMMIT, PROBE, FAILED, ESCALATED
//	COMMIT:     RUNNING, FAILED, ESCALATED
//	RUNNING:    DONE, FAILED, ESCALATED
//	FAILED:     ESCALATED
//	ESCALATED:  PROBE
//	DONE:       (terminal)
var transitions [8]uint8

func init() {
	const (
		bProbe  = 1 << afepv1.TaskState_TASK_STATE_PROBE
		bBid    = 1 << afepv1.TaskState_TASK_STATE_BID
		bCommit = 1 << afepv1.TaskState_TASK_STATE_COMMIT
		bRun    = 1 << afepv1.TaskState_TASK_STATE_RUNNING
		bDone   = 1 << afepv1.TaskState_TASK_STATE_DONE
		bFail   = 1 << afepv1.TaskState_TASK_STATE_FAILED
		bEsc    = 1 << afepv1.TaskState_TASK_STATE_ESCALATED
	)
	transitions[afepv1.TaskState_TASK_STATE_UNSPECIFIED] = bProbe
	transitions[afepv1.TaskState_TASK_STATE_PROBE] = bBid | bFail | bEsc
	transitions[afepv1.TaskState_TASK_STATE_BID] = bCommit | bProbe | bFail | bEsc
	transitions[afepv1.TaskState_TASK_STATE_COMMIT] = bRun | bFail | bEsc
	transitions[afepv1.TaskState_TASK_STATE_RUNNING] = bDone | bFail | bEsc
	transitions[afepv1.TaskState_TASK_STATE_FAILED] = bEsc
	transitions[afepv1.TaskState_TASK_STATE_ESCALATED] = bProbe
	transitions[afepv1.TaskState_TASK_STATE_DONE] = 0 // terminal
}

// Can reports whether from -> to is a legal transition. Same-state updates
// are always allowed. It is a single bitmask probe (no allocation, no loop)
// so the state-machine gate costs nothing on the per-envelope hot path.
func Can(from, to afepv1.TaskState) bool {
	if from == to {
		return true
	}
	if int(from) < 0 || int(from) >= len(transitions) {
		return false
	}
	return transitions[from]&(1<<uint8(to)) != 0
}

// Apply moves the envelope to to, enforcing the state machine.
func Apply(env *afepv1.TaskEnvelope, to afepv1.TaskState) error {
	if env == nil {
		return fmt.Errorf("afep/lifecycle: nil envelope")
	}
	if !Can(env.GetState(), to) {
		return fmt.Errorf("afep/lifecycle: illegal transition %s -> %s (task %s)", env.GetState(), to, env.GetTaskId())
	}
	env.State = to
	env.UpdatedAtUnix = time.Now().Unix()
	return nil
}

// Terminal reports whether the state has no successors.
func Terminal(state afepv1.TaskState) bool {
	return state == afepv1.TaskState_TASK_STATE_DONE
}
