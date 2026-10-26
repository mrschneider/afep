package a2a

import afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"

// A2A task states (v1.0, eight states + unspecified), as emitted by this
// adapter when AFEP lifecycle states cross into A2A tooling.
const (
	StateUnspecified   = "TASK_STATE_UNSPECIFIED"
	StateSubmitted     = "TASK_STATE_SUBMITTED"
	StateWorking       = "TASK_STATE_WORKING"
	StateInputRequired = "TASK_STATE_INPUT_REQUIRED"
	StateCompleted     = "TASK_STATE_COMPLETED"
	StateFailed        = "TASK_STATE_FAILED"
)

// StateOf maps an AFEP TaskState onto the A2A task-state vocabulary:
//
//	PROBE/BID/COMMIT -> SUBMITTED (acknowledged, being arranged)
//	RUNNING          -> WORKING
//	DONE             -> COMPLETED (terminal)
//	FAILED           -> FAILED (terminal)
//	ESCALATED        -> INPUT_REQUIRED (interrupted: another domain must act)
//	UNSPECIFIED      -> UNSPECIFIED
func StateOf(s afepv1.TaskState) string {
	switch s {
	case afepv1.TaskState_TASK_STATE_PROBE,
		afepv1.TaskState_TASK_STATE_BID,
		afepv1.TaskState_TASK_STATE_COMMIT:
		return StateSubmitted
	case afepv1.TaskState_TASK_STATE_RUNNING:
		return StateWorking
	case afepv1.TaskState_TASK_STATE_DONE:
		return StateCompleted
	case afepv1.TaskState_TASK_STATE_FAILED:
		return StateFailed
	case afepv1.TaskState_TASK_STATE_ESCALATED:
		return StateInputRequired
	default:
		return StateUnspecified
	}
}
