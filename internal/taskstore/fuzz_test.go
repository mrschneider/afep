package taskstore

import (
	"os"
	"path/filepath"
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

// FuzzTaskstoreOpen feeds arbitrary bytes as the persisted snapshot:
// Open must reject garbage without panicking, and a store that does
// open must stay usable (Len/Get/Put including the flush path).
func FuzzTaskstoreOpen(f *testing.F) {
	// v1 (JSON) and v2 (binary) shapes, plus raw garbage.
	f.Add([]byte("{}"))
	f.Add([]byte(`{"version":1,"tasks":{}}`))
	f.Add([]byte(`{"version":1,"tasks":{"x":{"task_id":"x"}}}`))
	f.Add([]byte(`{"version":99}`))
	f.Add([]byte("AFEPST02")) // v2 magic, truncated
	f.Add(encodeV2(map[string][]byte{"x": []byte{0x0a, 0x01, 0x78}}))
	f.Add([]byte{0x00, 0xff, 0x13})
	f.Fuzz(func(t *testing.T, data []byte) {
		// Per-iteration dir: the 4 fuzz workers share f.TempDir() and a
		// single path would race on the same file. t.TempDir() is cheap
		// (one mkdir) compared to the fsyncs Close would trigger.
		path := filepath.Join(t.TempDir(), "tasks.json")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		s, err := Open(path)
		if err != nil {
			return // rejection is fine; panics are not
		}
		s.Len()
		if _, ok := s.Get("anything"); ok {
			t.Fatal("unexpected hit on fuzzed store")
		}
		_ = s.Put(&afepv1.TaskEnvelope{TaskId: "fuzz"})
		// Stop the writer goroutine directly (same package) instead of
		// calling Close, which would trigger compact → 2× writeAtomic
		// (4 fsyncs) — too slow at the thousands-of-exec/sec rate the
		// fuzz engine sustains. Without this, each Open leaks a ticker
		// goroutine and the accumulated fsyncs saturate the disk.
		s.writeStop()
		<-s.writeDone
	})
}
