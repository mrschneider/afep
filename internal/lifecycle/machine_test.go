package lifecycle

import (
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

func TestCanTransitions(t *testing.T) {
	legal := []struct{ from, to afepv1.TaskState }{
		{afepv1.TaskState_TASK_STATE_UNSPECIFIED, afepv1.TaskState_TASK_STATE_PROBE},
		{afepv1.TaskState_TASK_STATE_PROBE, afepv1.TaskState_TASK_STATE_BID},
		{afepv1.TaskState_TASK_STATE_BID, afepv1.TaskState_TASK_STATE_COMMIT},
		{afepv1.TaskState_TASK_STATE_COMMIT, afepv1.TaskState_TASK_STATE_RUNNING},
		{afepv1.TaskState_TASK_STATE_RUNNING, afepv1.TaskState_TASK_STATE_DONE},
		{afepv1.TaskState_TASK_STATE_RUNNING, afepv1.TaskState_TASK_STATE_FAILED},
		{afepv1.TaskState_TASK_STATE_FAILED, afepv1.TaskState_TASK_STATE_ESCALATED},
		{afepv1.TaskState_TASK_STATE_ESCALATED, afepv1.TaskState_TASK_STATE_PROBE},
		{afepv1.TaskState_TASK_STATE_BID, afepv1.TaskState_TASK_STATE_PROBE},
		{afepv1.TaskState_TASK_STATE_PROBE, afepv1.TaskState_TASK_STATE_PROBE}, // idempotent
	}
	for _, tc := range legal {
		if !Can(tc.from, tc.to) {
			t.Errorf("expected legal: %s -> %s", tc.from, tc.to)
		}
	}

	illegal := []struct{ from, to afepv1.TaskState }{
		{afepv1.TaskState_TASK_STATE_PROBE, afepv1.TaskState_TASK_STATE_COMMIT},
		{afepv1.TaskState_TASK_STATE_PROBE, afepv1.TaskState_TASK_STATE_DONE},
		{afepv1.TaskState_TASK_STATE_DONE, afepv1.TaskState_TASK_STATE_PROBE},
		{afepv1.TaskState_TASK_STATE_DONE, afepv1.TaskState_TASK_STATE_FAILED},
		{afepv1.TaskState_TASK_STATE_COMMIT, afepv1.TaskState_TASK_STATE_BID},
		{afepv1.TaskState_TASK_STATE_ESCALATED, afepv1.TaskState_TASK_STATE_DONE},
		{afepv1.TaskState_TASK_STATE_UNSPECIFIED, afepv1.TaskState_TASK_STATE_BID},
	}
	for _, tc := range illegal {
		if Can(tc.from, tc.to) {
			t.Errorf("expected illegal: %s -> %s", tc.from, tc.to)
		}
	}
}

func TestApply(t *testing.T) {
	env := &afepv1.TaskEnvelope{TaskId: "t1", State: afepv1.TaskState_TASK_STATE_PROBE}
	if err := Apply(env, afepv1.TaskState_TASK_STATE_BID); err != nil {
		t.Fatalf("legal apply failed: %v", err)
	}
	if env.GetState() != afepv1.TaskState_TASK_STATE_BID {
		t.Fatalf("state = %s, want BID", env.GetState())
	}
	if env.GetUpdatedAtUnix() == 0 {
		t.Fatal("updated_at not stamped")
	}
	if err := Apply(env, afepv1.TaskState_TASK_STATE_DONE); err == nil {
		t.Fatal("illegal apply must fail")
	}
	if env.GetState() != afepv1.TaskState_TASK_STATE_BID {
		t.Fatal("failed apply must not mutate state")
	}
	if err := Apply(nil, afepv1.TaskState_TASK_STATE_PROBE); err == nil {
		t.Fatal("nil envelope must fail")
	}
}

func TestTerminal(t *testing.T) {
	if !Terminal(afepv1.TaskState_TASK_STATE_DONE) {
		t.Fatal("DONE must be terminal")
	}
	if Terminal(afepv1.TaskState_TASK_STATE_RUNNING) {
		t.Fatal("RUNNING must not be terminal")
	}
}
