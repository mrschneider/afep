package taskstore

import (
	"path/filepath"
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

// BenchmarkPut measures the per-envelope Put cost (store-owned clone +
// protojson encode + dirty flag). The atomic file write now runs in the
// background writer, coalesced per flush window, so it is not on this path.
func BenchmarkPut(b *testing.B) {
	path := filepath.Join(b.TempDir(), "tasks.json")
	s, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Put(&afepv1.TaskEnvelope{TaskId: "bench", Hops: uint32(i)}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPutFlush10k measures the coalesced flush cost with a 10k-task
// store: one O(N) snapshot per flush window, not per Put.
func BenchmarkPutFlush10k(b *testing.B) {
	path := filepath.Join(b.TempDir(), "tasks.json")
	s, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	for i := 0; i < 10_000; i++ {
		if err := s.Put(&afepv1.TaskEnvelope{TaskId: "seed", Hops: uint32(i)}); err != nil {
			b.Fatal(err)
		}
	}
	if err := s.Flush(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Put(&afepv1.TaskEnvelope{TaskId: "bench", Hops: uint32(i)}); err != nil {
			b.Fatal(err)
		}
	}
}
