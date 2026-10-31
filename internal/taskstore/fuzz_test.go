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
	f.Add([]byte("{}"))
	f.Add([]byte(`{"version":1,"tasks":{}}`))
	f.Add([]byte(`{"version":1,"tasks":{"x":{"task_id":"x"}}}`))
	f.Add([]byte(`{"version":99}`))
	f.Add([]byte{0x00, 0xff, 0x13})
	path := filepath.Join(f.TempDir(), "tasks.json")
	f.Fuzz(func(t *testing.T, data []byte) {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		s, err := Open(path)
		if err != nil {
			return // rejection is fine; panics are not
		}
		t.Cleanup(func() { _ = s.Close() })
		s.Len()
		if _, ok := s.Get("anything"); ok {
			t.Fatal("unexpected hit on fuzzed store")
		}
		_ = s.Put(&afepv1.TaskEnvelope{TaskId: "fuzz"})
	})
}
