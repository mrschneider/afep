package taskstore

import (
	"path/filepath"
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

// BenchmarkPut measures the real flush path (cached protojson encode +
// atomic tmp+rename write) per envelope.
func BenchmarkPut(b *testing.B) {
	path := filepath.Join(b.TempDir(), "tasks.json")
	s, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Put(&afepv1.TaskEnvelope{TaskId: "bench", Hops: uint32(i)}); err != nil {
			b.Fatal(err)
		}
	}
}
