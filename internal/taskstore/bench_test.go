package taskstore

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
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

// BenchmarkDeltaFlush10k is the headline number: with a 10k-task store and
// ONE put per iteration, the flush cost must stay proportional to the
// delta (a few hundred bytes), not to the store (2.2 MB in v2).
func BenchmarkDeltaFlush10k(b *testing.B) {
	path := filepath.Join(b.TempDir(), "tasks.json")
	s, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	for i := 0; i < 10_000; i++ {
		_ = s.Put(&afepv1.TaskEnvelope{TaskId: "seed-" + itoaBench(i), Hops: uint32(i)})
	}
	if err := s.Flush(); err != nil { // compacts the 10k seeds into the base
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Put(&afepv1.TaskEnvelope{TaskId: "bench-" + itoaBench(i), Hops: uint32(i)})
		if err := s.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func itoaBench(i int) string {
	return fmt.Sprintf("%d", i)
}

// BenchmarkPutConcurrent stresses Put from many goroutines at once. Before
// the lock-scope fix the expensive clone+marshal ran INSIDE the write lock,
// so concurrent Puts serialized on it; now the copy/encode runs outside and
// only the O(1) map insert is critical, so throughput should scale with
// GOMAXPROCS. Each goroutine writes to its own id space to avoid the
// maxTasks cap.
func BenchmarkPutConcurrent(b *testing.B) {
	path := filepath.Join(b.TempDir(), "tasks.json")
	s, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	// A roomy cap so the concurrent writers are not rejected mid-bench.
	s.SetMaxTasks(1 << 20)
	var seq atomic.Int64
	env := func(id int64) *afepv1.TaskEnvelope {
		return &afepv1.TaskEnvelope{TaskId: fmt.Sprintf("t-%d", id), Hops: uint32(id % 100), Payload: make([]byte, 256)}
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := s.Put(env(seq.Add(1))); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPutConcurrentLarge is the realistic case: envelopes carrying
// multi-KB tool-output payloads. The clone+marshal cost scales with payload,
// so moving it out of the write lock matters here in a way the 256B case
// (where encode is sub-microsecond) does not.
func BenchmarkPutConcurrentLarge(b *testing.B) {
	path := filepath.Join(b.TempDir(), "tasks.json")
	s, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	s.SetMaxTasks(1 << 20)
	var seq atomic.Int64
	payload := make([]byte, 4<<10) // 4 KiB
	env := func(id int64) *afepv1.TaskEnvelope {
		return &afepv1.TaskEnvelope{TaskId: fmt.Sprintf("t-%d", id), Hops: uint32(id % 100), Payload: payload}
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := s.Put(env(seq.Add(1))); err != nil {
				b.Fatal(err)
			}
		}
	})
}
