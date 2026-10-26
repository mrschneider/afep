package taskstore

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

func TestPersistenceAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "tasks.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	env := &afepv1.TaskEnvelope{
		TaskId:     "t1",
		RootTaskId: "t1",
		State:      afepv1.TaskState_TASK_STATE_BID,
		Capability: "vision.tag",
		Tenant:     "acme",
	}
	if err := s.Put(env); err != nil {
		t.Fatal(err)
	}

	// reopen = restart
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := s2.Get("t1")
	if !ok {
		t.Fatal("task lost across reopen")
	}
	if got.GetCapability() != "vision.tag" || got.GetState() != afepv1.TaskState_TASK_STATE_BID {
		t.Fatalf("roundtrip = %v", got)
	}
	if s2.Len() != 1 {
		t.Fatalf("len = %d", s2.Len())
	}
}

func TestPruneTerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour).Unix()
	_ = s.Put(&afepv1.TaskEnvelope{TaskId: "done-old", State: afepv1.TaskState_TASK_STATE_DONE, UpdatedAtUnix: old})
	_ = s.Put(&afepv1.TaskEnvelope{TaskId: "done-new", State: afepv1.TaskState_TASK_STATE_DONE, UpdatedAtUnix: time.Now().Unix()})
	_ = s.Put(&afepv1.TaskEnvelope{TaskId: "live-old", State: afepv1.TaskState_TASK_STATE_RUNNING, UpdatedAtUnix: old})

	removed, err := s.PruneTerminal(24*time.Hour, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if _, ok := s.Get("done-old"); ok {
		t.Fatal("old terminal task must be pruned")
	}
	if _, ok := s.Get("done-new"); !ok {
		t.Fatal("recent terminal task must survive")
	}
	if _, ok := s.Get("live-old"); !ok {
		t.Fatal("non-terminal task must survive")
	}
	// prune is persisted too
	s2, _ := Open(path)
	if _, ok := s2.Get("done-old"); ok {
		t.Fatal("prune must survive reopen")
	}
}

func TestPutRejectsIDless(t *testing.T) {
	s, _ := Open(filepath.Join(t.TempDir(), "t.json"))
	if err := s.Put(&afepv1.TaskEnvelope{}); err == nil {
		t.Fatal("id-less envelope must fail")
	}
	if err := s.Put(nil); err == nil {
		t.Fatal("nil envelope must fail")
	}
}

func TestAtomicFilePresent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tasks.json")
	s, _ := Open(path)
	_ = s.Put(&afepv1.TaskEnvelope{TaskId: "x", State: afepv1.TaskState_TASK_STATE_PROBE})
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("state file missing: %v", err)
	}
	leftovers, _ := filepath.Glob(filepath.Join(dir, ".tasks-*.tmp"))
	if len(leftovers) != 0 {
		t.Fatalf("tmp leftovers: %v", leftovers)
	}
}
