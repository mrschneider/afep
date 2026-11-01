package taskstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	if err := s.Close(); err != nil {
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
	if err := s.Close(); err != nil {
		t.Fatal(err)
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
	t.Cleanup(func() { _ = s.Close() })
	_ = s.Put(&afepv1.TaskEnvelope{TaskId: "x", State: afepv1.TaskState_TASK_STATE_PROBE})
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	// The incremental design writes the delta companion on a delta-only
	// flush (the base is only rewritten on compaction).
	if _, err := os.Stat(s.deltaPath()); err != nil {
		t.Fatalf("delta file missing after flush: %v", err)
	}
	leftovers, _ := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if len(leftovers) != 0 {
		t.Fatalf("tmp leftovers: %v", leftovers)
	}
}

// TestV1Migration checks that a legacy v1 JSON snapshot loads, round-trips
// its tasks, and is re-written in the v2 binary format.
func TestV1Migration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tasks.json")
	v1 := `{"version":1,"tasks":{"t1":{"task_id":"t1","capability":"vision.tag","state":"TASK_STATE_BID","tenant":"acme"}}}`
	if err := os.WriteFile(path, []byte(v1), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if s.Len() != 1 {
		t.Fatalf("len = %d, want 1", s.Len())
	}
	got, ok := s.Get("t1")
	if !ok || got.GetCapability() != "vision.tag" || got.GetState() != afepv1.TaskState_TASK_STATE_BID {
		t.Fatalf("migrated task = %v, ok=%v", got, ok)
	}
	// The migration write happens synchronously at Open: the file must
	// now be v2 binary, not JSON.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data[:8], v2Magic[:]) {
		t.Fatalf("file not migrated to v2: %q", data[:min(16, len(data))])
	}
	// A v1 file with a bogus version must be rejected, not migrated.
	badPath := filepath.Join(dir, "bad.json")
	_ = os.WriteFile(badPath, []byte(`{"version":3,"tasks":{}}`), 0o600)
	if _, err := Open(badPath); err == nil {
		t.Fatal("bogus v1 version must be rejected")
	}
	// A v2 file with a broken count must be rejected too.
	v2corrupt := encodeV2(map[string][]byte{"x": []byte{0x0a, 0x01}})
	binary.LittleEndian.PutUint32(v2corrupt[12:16], 5)
	corruptPath := filepath.Join(dir, "corrupt.bin")
	_ = os.WriteFile(corruptPath, v2corrupt, 0o600)
	if _, err := Open(corruptPath); err == nil {
		t.Fatal("corrupt v2 snapshot must be rejected")
	}
}

// TestRoundtripMany exercises the v2 encode/decode path with a realistic
// task set: every task must survive a Close/Open cycle byte-identical.
func TestRoundtripMany(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	n := 250
	for i := 0; i < n; i++ {
		env := &afepv1.TaskEnvelope{
			TaskId:      fmt.Sprintf("task-%03d", i),
			RootTaskId:  fmt.Sprintf("root-%03d", i%7),
			Tenant:      "acme",
			State:       afepv1.TaskState_TASK_STATE_PROBE,
			Capability:  fmt.Sprintf("cap.%d", i%3),
			PayloadType: "application/x-afep-echo+proto",
			Payload:     []byte(fmt.Sprintf(`{"n":%d}`, i)),
			Hops:        uint32(i % 4),
		}
		if err := s.Put(env); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.Len() != n {
		t.Fatalf("len = %d, want %d", s2.Len(), n)
	}
	for i := 0; i < n; i++ {
		got, ok := s2.Get(fmt.Sprintf("task-%03d", i))
		if !ok {
			t.Fatalf("task-%03d lost", i)
		}
		if !bytes.Equal(got.GetPayload(), []byte(fmt.Sprintf(`{"n":%d}`, i))) {
			t.Fatalf("task-%03d payload = %q", i, got.GetPayload())
		}
	}
}

// TestDeltaFlushOnlyWritesDelta verifies the incremental design: after a
// base is on disk, a flush with a few puts rewrites ONLY the delta file —
// the base file must be byte-identical.
func TestDeltaFlushOnlyWritesDelta(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	// Seed enough entries to force a compaction: the base file now
	// exists and holds 150 entries.
	for i := 0; i < 150; i++ {
		_ = s.Put(&afepv1.TaskEnvelope{TaskId: fmt.Sprintf("seed-%d", i), State: afepv1.TaskState_TASK_STATE_PROBE})
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	baseBefore, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Put(&afepv1.TaskEnvelope{TaskId: "changed", State: afepv1.TaskState_TASK_STATE_RUNNING})
	_ = s.Put(&afepv1.TaskEnvelope{TaskId: "seed-0", State: afepv1.TaskState_TASK_STATE_DONE})
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	baseAfter, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(baseBefore, baseAfter) {
		t.Fatal("base file must not change on a delta-only flush")
	}
	delta, err := os.ReadFile(s.deltaPath())
	if err != nil {
		t.Fatal("delta file missing after flush")
	}
	if !bytes.Equal(delta[:8], deltaMagic[:]) {
		t.Fatalf("delta magic = %q", delta[:8])
	}
}

// TestDeltaRoundtripAndTombstone verifies puts, re-puts and prunes survive
// a Close/Open cycle through the delta path.
func TestDeltaRoundtripAndTombstone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour).Unix()
	_ = s.Put(&afepv1.TaskEnvelope{TaskId: "a", State: afepv1.TaskState_TASK_STATE_DONE, UpdatedAtUnix: old})
	_ = s.Put(&afepv1.TaskEnvelope{TaskId: "b", State: afepv1.TaskState_TASK_STATE_DONE, UpdatedAtUnix: old})
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	// Mutate one (refresh age), prune the other (tombstone into the delta).
	_ = s.Put(&afepv1.TaskEnvelope{TaskId: "a", State: afepv1.TaskState_TASK_STATE_DONE, UpdatedAtUnix: time.Now().Unix()})
	if n, err := s.PruneTerminal(24*time.Hour, time.Now()); err != nil || n != 1 {
		t.Fatalf("prune = %d, %v", n, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.Len() != 1 {
		t.Fatalf("len = %d, want 1 (only the surviving task)", s2.Len())
	}
	// The pruned task must be gone after reopen.
	if _, ok := s2.Get("b"); ok {
		t.Fatal("tombstoned task must not survive reopen")
	}
	// The mutated task keeps its NEW state.
	got, ok := s2.Get("a")
	if !ok || got.GetUpdatedAtUnix() == old {
		t.Fatalf("mutated task = %v", got)
	}
}

// TestCompactionFoldsDeltaIntoBase verifies the threshold triggers a full
// rewrite: the delta is truncated and the base alone carries everything.
func TestCompactionFoldsDeltaIntoBase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	// 150 fresh puts in an empty store: delta (150) >= compactMinEntries
	// and 150*4 >= 150 total -> compaction on the next flush.
	for i := 0; i < 150; i++ {
		_ = s.Put(&afepv1.TaskEnvelope{TaskId: fmt.Sprintf("c-%03d", i), State: afepv1.TaskState_TASK_STATE_PROBE})
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	// The base now holds all 150; the delta is empty.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data[:8], v2Magic[:]) {
		t.Fatalf("base magic = %q", data[:8])
	}
	count := binary.LittleEndian.Uint32(data[12:16])
	if count != 150 {
		t.Fatalf("base count = %d, want 150", count)
	}
	delta, err := os.ReadFile(s.deltaPath())
	if err != nil {
		t.Fatal(err)
	}
	if n := binary.LittleEndian.Uint32(delta[12:16]); n != 0 {
		t.Fatalf("delta count after compaction = %d, want 0", n)
	}
	// And a reopen from the base alone sees everything.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.Len() != 150 {
		t.Fatalf("len after compaction reopen = %d, want 150", s2.Len())
	}
}

// TestCrashBetweenBaseAndDelta simulates a compaction interrupted after the
// new base landed but before the delta was cleared: the stale delta must
// be applied as a no-op, never corrupting the state.
func TestCrashBetweenBaseAndDelta(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tasks.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 150; i++ {
		_ = s.Put(&afepv1.TaskEnvelope{TaskId: fmt.Sprintf("k-%03d", i), State: afepv1.TaskState_TASK_STATE_PROBE})
	}
	if err := s.Flush(); err != nil { // compacts: base=150, delta=empty
		t.Fatal(err)
	}
	_ = s.Put(&afepv1.TaskEnvelope{TaskId: "k-000", State: afepv1.TaskState_TASK_STATE_RUNNING})
	if err := s.Flush(); err != nil { // delta = {k-000 put}
		t.Fatal(err)
	}
	// Force one more compaction so the base already contains the update…
	for i := 150; i < 300; i++ {
		_ = s.Put(&afepv1.TaskEnvelope{TaskId: fmt.Sprintf("m-%03d", i)})
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	// …then emulate a crash leaving the (now stale) delta file behind:
	// rewrite the delta with k-000's CURRENT bytes (already in the base).
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	env, _ := (&taskstoreProbe{}).get(path, "k-000")
	stale := encodeDelta([]deltaEntry{{id: "k-000", raw: env}})
	if err := os.WriteFile(path+".delta", stale, 0o600); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.Len() != 300 {
		t.Fatalf("len with stale delta = %d, want 300", s2.Len())
	}
	got, ok := s2.Get("k-000")
	if !ok || got.GetState() != afepv1.TaskState_TASK_STATE_RUNNING {
		t.Fatalf("k-000 = %v", got)
	}
}

// taskstoreProbe is a tiny helper to read one entry back from a file.
type taskstoreProbe struct{}

func (taskstoreProbe) get(path, id string) ([]byte, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	raws, err := decodeV2(data)
	if err != nil {
		return nil, false
	}
	raw, ok := raws[id]
	return raw, ok
}
