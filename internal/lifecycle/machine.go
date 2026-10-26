// Package lifecycle implements the AFEP task state machine.
package lifecycle

import (
	"fmt"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

// transitions lists every legal target state per source state.
// Same-state updates are always allowed (idempotent refreshes).
var transitions = map[afepv1.TaskState][]afepv1.TaskState{
	afepv1.TaskState_TASK_STATE_UNSPECIFIED: {
		afepv1.TaskState_TASK_STATE_PROBE, // initial entry
	},
	afepv1.TaskState_TASK_STATE_PROBE: {
		afepv1.TaskState_TASK_STATE_BID,
		afepv1.TaskState_TASK_STATE_FAILED,
		afepv1.TaskState_TASK_STATE_ESCALATED,
	},
	afepv1.TaskState_TASK_STATE_BID: {
		afepv1.TaskState_TASK_STATE_COMMIT,
		afepv1.TaskState_TASK_STATE_PROBE, // re-probe: no acceptable bid
		afepv1.TaskState_TASK_STATE_FAILED,
		afepv1.TaskState_TASK_STATE_ESCALATED,
	},
	afepv1.TaskState_TASK_STATE_COMMIT: {
		afepv1.TaskState_TASK_STATE_RUNNING,
		afepv1.TaskState_TASK_STATE_FAILED,
		afepv1.TaskState_TASK_STATE_ESCALATED,
	},
	afepv1.TaskState_TASK_STATE_RUNNING: {
		afepv1.TaskState_TASK_STATE_DONE,
		afepv1.TaskState_TASK_STATE_FAILED,
		afepv1.TaskState_TASK_STATE_ESCALATED,
	},
	afepv1.TaskState_TASK_STATE_FAILED: {
		afepv1.TaskState_TASK_STATE_ESCALATED,
	},
	afepv1.TaskState_TASK_STATE_ESCALATED: {
		afepv1.TaskState_TASK_STATE_PROBE, // re-entry in the target domain
	},
	afepv1.TaskState_TASK_STATE_DONE: {}, // terminal
}

// Can reports whether from -> to is a legal transition.
func Can(from, to afepv1.TaskState) bool {
	if from == to {
		return true
	}
	for _, next := range transitions[from] {
		if next == to {
			return true
		}
	}
	return false
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
