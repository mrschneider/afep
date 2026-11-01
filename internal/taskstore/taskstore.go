// Package taskstore persists TaskEnvelopes across restarts using atomic
// file writes (tmp + rename) and an incremental write-behind design.
//
// The state is split across two files:
//
//	base   <path>        full snapshot (v2 format)
//	delta  <path>.delta  entries changed since the base (v3 format)
//
// Puts and prunes update the in-memory map immediately and record the
// change in the delta; a single background writer coalesces dirty windows
// into at most one flush per window. A flush costs O(delta), not O(store):
// while the delta is small only the delta file is rewritten (the base is
// left on disk untouched); when the delta grows past a fraction of the
// store the writer compacts — folding base ∪ delta into a new base and
// truncating the delta. Flush and Close are synchronous durability
// barriers.
//
// Crash safety: both files are self-contained and replaced atomically.
// Compaction writes the new base BEFORE clearing the delta, so a crash in
// between leaves a stale delta whose entries are already in the new base —
// a no-op on load.
//
// Base format v2 (length-prefixed protobuf binary):
//
//	magic "AFEPST02" | version u32le | count u32le |
//	{ idLen u32le | id | envLen u32le | env-proto }*
//
// Delta format v3 (put/tombstone ops, applied over the base in order):
//
//	magic "AFEPD003" | version u32le | count u32le |
//	{ idLen u32le | id | op u8 (1=put, 2=tombstone) | bodyLen u32le | body }*
//
// v1 JSON files are read transparently at boot and re-written as the v2
// base (in-place migration).
package taskstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/metrics"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// On-disk versions: v2 is the current base format, v1 (protojson) is
// read-only input, deltaVersion is the current delta format.
const (
	version      = 2
	versionV1    = 1
	deltaVersion = 3
)

var (
	v2Magic    = [8]byte{'A', 'F', 'E', 'P', 'S', 'T', '0', '2'}
	deltaMagic = [8]byte{'A', 'F', 'E', 'P', 'D', '0', '0', '3'}
)

// Delta op codes in the v3 delta format.
const (
	opPut  byte = 1
	opTomb byte = 2
)

// Compaction threshold: fold the delta into the base once it covers more
// than 1/compactFraction of the store (and at least compactMinEntries, so
// a small store never pays for a full rewrite on a handful of puts).
const (
	compactFraction   = 4
	compactMinEntries = 128
)

// defaultFlushInterval bounds how long a put can stay in memory before it
// is guaranteed to hit disk. It coalesces bursts (many puts in one window
// become one write) while keeping the steady-state durability lag small.
const defaultFlushInterval = 100 * time.Millisecond

// diskStateV1 is the legacy on-disk format (read-only at boot).
type diskStateV1 struct {
	Version int                        `json:"version"`
	Tasks   map[string]json.RawMessage `json:"tasks"`
}

// deltaOp marks an id in the pending delta: a put (bytes live in raws) or
// a tombstone (the entry was removed since the base).
type deltaOp struct {
	tomb bool
}

// deltaEntry is one decoded/encoded v3 delta operation.
type deltaEntry struct {
	id   string
	tomb bool
	raw  []byte
}

// Store is an in-memory task map backed by a base snapshot plus an
// incremental delta file.
type Store struct {
	mu    sync.RWMutex
	path  string
	tasks map[string]*afepv1.TaskEnvelope
	// raws caches the binary encoding of each stored envelope so flushes
	// emit cached bytes and never re-encode.
	raws map[string][]byte
	// delta holds the entries changed since the base was last written
	// (puts and tombstones); a flush rewrites only these.
	delta map[string]deltaOp
	// maxTasks caps stored envelopes (0 = unlimited): prune-age=0
	// used to leave the store without any bound at all.
	maxTasks int

	// Write-behind flusher state. dirty is set by Put/PruneTerminal and
	// consumed by the single writer goroutine (startWriter), which does
	// one O(delta) write (or an occasional compaction) per dirty window.
	// flushMu serializes the actual disk writes so the writer and a
	// concurrent Flush/Close cannot interleave temp-file/rename ops.
	dirty         atomic.Bool
	flushInterval time.Duration
	flushMu       sync.Mutex
	writeStop     context.CancelFunc
	writeDone     chan struct{}
}

// deltaPath is the companion file holding the pending ops.
func (s *Store) deltaPath() string { return s.path + ".delta" }

// Open loads the store from path (base) plus its delta companion; missing
// files start fresh. v1 JSON bases load transparently and are re-written
// as the v2 base immediately. A base-only or delta-only file is valid (an
// empty base); a CORRUPT delta is not — it must fail loudly, never be
// dropped.
func Open(path string) (*Store, error) {
	s := &Store{
		path:          path,
		tasks:         make(map[string]*afepv1.TaskEnvelope),
		raws:          make(map[string][]byte),
		delta:         make(map[string]deltaOp),
		flushInterval: defaultFlushInterval,
	}
	baseData, baseErr := os.ReadFile(path)
	if baseErr != nil && !errors.Is(baseErr, os.ErrNotExist) {
		return nil, fmt.Errorf("afep/taskstore: read: %w", baseErr)
	}
	deltaData, deltaErr := os.ReadFile(s.deltaPath())
	if deltaErr != nil && !errors.Is(deltaErr, os.ErrNotExist) {
		return nil, fmt.Errorf("afep/taskstore: read delta: %w", deltaErr)
	}
	if len(baseData) == 0 {
		baseData = nil
	}
	if len(deltaData) == 0 {
		deltaData = nil
	}
	migrated := false
	if baseData != nil {
		if bytes.Equal(baseData[:min(8, len(baseData))], v2Magic[:]) {
			raws, err := decodeV2(baseData)
			if err != nil {
				return nil, fmt.Errorf("afep/taskstore: parse %s: %w", path, err)
			}
			if err := s.loadRaws(raws); err != nil {
				return nil, fmt.Errorf("afep/taskstore: %w", err)
			}
		} else {
			raws, m, err := loadV1(baseData)
			if err != nil {
				return nil, fmt.Errorf("afep/taskstore: parse %s: %w", path, err)
			}
			if err := s.loadRaws(raws); err != nil {
				return nil, fmt.Errorf("afep/taskstore: %w", err)
			}
			migrated = m
		}
	}
	if deltaData != nil {
		ops, err := decodeDelta(deltaData)
		if err != nil {
			return nil, fmt.Errorf("afep/taskstore: parse %s: %w", s.deltaPath(), err)
		}
		for _, op := range ops {
			if op.tomb {
				delete(s.tasks, op.id)
				delete(s.raws, op.id)
				s.delta[op.id] = deltaOp{tomb: true}
			} else {
				env := &afepv1.TaskEnvelope{}
				if err := proto.Unmarshal(op.raw, env); err != nil {
					return nil, fmt.Errorf("afep/taskstore: task %s: %w", op.id, err)
				}
				s.tasks[op.id] = env
				s.raws[op.id] = op.raw // byte-preserved: no re-encode at boot
				s.delta[op.id] = deltaOp{}
			}
		}
	}
	s.startWriter()
	if migrated {
		// One-shot format migration: write the v2 base synchronously so
		// the file is never left in a format this version cannot read.
		if err := s.compact(); err != nil {
			return nil, fmt.Errorf("afep/taskstore: migrate %s: %w", path, err)
		}
	}
	return s, nil
}

// loadRaws installs decoded base entries (called by Open under no lock).
func (s *Store) loadRaws(raws map[string][]byte) error {
	for id, raw := range raws {
		env := &afepv1.TaskEnvelope{}
		if err := proto.Unmarshal(raw, env); err != nil {
			return fmt.Errorf("task %s: %w", id, err)
		}
		s.tasks[id] = env
		s.raws[id] = raw
	}
	return nil
}

// loadV1 parses a legacy v1 JSON snapshot and returns each task re-encoded
// as a plain binary proto (ready for the v2 cache). The second return is
// true when the input was a v1 file, i.e. a migration write is owed.
func loadV1(data []byte) (map[string][]byte, bool, error) {
	var st diskStateV1
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, false, err
	}
	if st.Version != versionV1 {
		return nil, false, fmt.Errorf("unsupported version %d", st.Version)
	}
	out := make(map[string][]byte, len(st.Tasks))
	for id, raw := range st.Tasks {
		env := &afepv1.TaskEnvelope{}
		if err := (protojson.UnmarshalOptions{}).Unmarshal(raw, env); err != nil {
			return nil, false, fmt.Errorf("task %s: %w", id, err)
		}
		// Re-encode once at boot so the base (and every later flush)
		// emits binary, not JSON.
		bin, err := proto.Marshal(env)
		if err != nil {
			return nil, false, fmt.Errorf("task %s: %w", id, err)
		}
		out[id] = bin
	}
	return out, true, nil
}

// startWriter launches the single background flusher. It coalesces every
// dirty window into one O(delta) write (or an occasional compaction) and
// stops on writeStop.
func (s *Store) startWriter() {
	ctx, cancel := context.WithCancel(context.Background())
	s.writeStop = cancel
	s.writeDone = make(chan struct{})
	go func() {
		defer close(s.writeDone)
		t := time.NewTicker(s.flushInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				// Swap atomically claims the dirty flag so a put that
				// lands after the claim is picked up by the next window.
				if s.dirty.Swap(false) {
					_ = s.flush()
				}
			}
		}
	}()
}

// Put stores or replaces an envelope. The in-memory map is the source of
// truth and is updated synchronously; the durable delta is updated by the
// background writer within one flush window. Call Flush or Close for a
// synchronous durability barrier.
func (s *Store) Put(env *afepv1.TaskEnvelope) error {
	if env == nil || env.GetTaskId() == "" {
		return fmt.Errorf("afep/taskstore: envelope needs a task id")
	}
	s.mu.Lock()
	id := env.GetTaskId()
	if _, exists := s.tasks[id]; !exists && s.maxTasks > 0 && len(s.tasks) >= s.maxTasks {
		s.mu.Unlock()
		return fmt.Errorf("afep/taskstore: task limit %d reached", s.maxTasks)
	}
	// Store-owned copy: callers may keep mutating their envelope while
	// flushes read ours — one clone keeps memory, the raws cache and the
	// file in lockstep (and kills the Probe/Commit aliasing race).
	owned := proto.Clone(env).(*afepv1.TaskEnvelope)
	// proto.Marshal runs once here for THIS envelope; flushes emit the
	// cached bytes instead of re-encoding.
	raw, err := proto.Marshal(owned)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("afep/taskstore: marshal %s: %w", id, err)
	}
	s.tasks[id] = owned
	s.raws[id] = raw
	s.delta[id] = deltaOp{} // put; bytes live in raws
	s.mu.Unlock()
	s.dirty.Store(true)
	return nil
}

// Get returns the stored envelope (callers own the returned value).
func (s *Store) Get(id string) (*afepv1.TaskEnvelope, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	env, ok := s.tasks[id]
	if !ok {
		return nil, false
	}
	// The doc promises callers own the value: hand out a copy so a
	// mutation never aliases the store (or its raws cache).
	return proto.Clone(env).(*afepv1.TaskEnvelope), true
}

// Len reports how many tasks are stored.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tasks)
}

// SetMaxTasks caps how many envelopes the store accepts (0 = unlimited).
func (s *Store) SetMaxTasks(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxTasks = n
}

// PruneTerminal drops DONE/FAILED envelopes older than maxAge (by
// updated_at). It returns the number of removed tasks. Removals are durable
// within one flush window (or immediately on Flush/Close) as tombstones in
// the delta.
func (s *Store) PruneTerminal(maxAge time.Duration, now time.Time) (int, error) {
	s.mu.Lock()
	removed := 0
	for id, env := range s.tasks {
		terminal := env.GetState() == afepv1.TaskState_TASK_STATE_DONE ||
			env.GetState() == afepv1.TaskState_TASK_STATE_FAILED
		age := now.Sub(time.Unix(env.GetUpdatedAtUnix(), 0))
		if terminal && age > maxAge {
			delete(s.tasks, id)
			delete(s.raws, id)
			s.delta[id] = deltaOp{tomb: true}
			removed++
		}
	}
	s.mu.Unlock()
	if removed > 0 {
		s.dirty.Store(true)
	}
	return removed, nil
}

// Flush writes the pending delta to disk synchronously (atomic tmp+rename,
// base untouched) and is the explicit durability barrier for callers that
// cannot rely on the background window. It is a no-op when nothing has
// changed since the last flush.
func (s *Store) Flush() error {
	return s.flush()
}

// Close stops the background writer and performs a final compaction,
// guaranteeing every accepted put is on disk in a single self-contained
// base file. It is safe to call once.
func (s *Store) Close() error {
	if s.writeStop != nil {
		s.writeStop()
		<-s.writeDone
	}
	// Final barrier: one durable base, empty delta.
	return s.compact()
}

// flush persists the pending delta. The disk I/O runs outside the state
// lock (only the small delta map is snapshotted under it) so a slow fsync
// never blocks Put/Get.
func (s *Store) flush() error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	start := time.Now()
	defer func() { metrics.TaskStoreFlushSeconds.Observe(time.Since(start).Nanoseconds()) }()
	s.mu.RLock()
	n := len(s.delta)
	if n == 0 {
		s.mu.RUnlock()
		return nil // nothing pending: the base already holds everything
	}
	ops := make([]deltaEntry, 0, n)
	for id, op := range s.delta {
		if op.tomb {
			ops = append(ops, deltaEntry{id: id, tomb: true})
		} else {
			ops = append(ops, deltaEntry{id: id, raw: s.raws[id]})
		}
	}
	total := len(s.tasks)
	s.mu.RUnlock()
	metrics.TaskStoreDeltaEntries.Set(int64(n))
	// Delta large relative to the store: compaction amortizes to one
	// O(N) write per compactMinEntries+ changes instead of O(N) per
	// window.
	if n >= compactMinEntries && n*compactFraction >= total {
		return s.compactLocked()
	}
	return s.writeDelta(ops)
}

// compact folds base ∪ delta into a new base file, then truncates the
// delta. The base is durable BEFORE the delta is cleared: a crash in
// between leaves a stale delta that load() applies as a no-op.
func (s *Store) compact() error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	return s.compactLocked()
}

// compactLocked is compact without taking flushMu (already held).
func (s *Store) compactLocked() error {
	start := time.Now()
	defer func() { metrics.TaskStoreFlushSeconds.Observe(time.Since(start).Nanoseconds()) }()
	s.mu.Lock()
	full := make(map[string][]byte, len(s.raws))
	for k, v := range s.raws {
		full[k] = v
	}
	for id, op := range s.delta {
		if op.tomb {
			delete(full, id)
		} else {
			full[id] = s.raws[id]
		}
	}
	s.delta = make(map[string]deltaOp)
	s.mu.Unlock()
	metrics.TaskStoreDeltaEntries.Set(0)
	metrics.TaskStoreCompactions.Inc()
	data := encodeV2(full)
	if err := s.writeAtomic(s.path, data); err != nil {
		return err
	}
	return s.writeAtomic(s.deltaPath(), encodeDelta(nil))
}

// writeDelta atomically replaces the delta companion file.
func (s *Store) writeDelta(ops []deltaEntry) error {
	return s.writeAtomic(s.deltaPath(), encodeDelta(ops))
}

// encodeV2 renders the base snapshot in the on-disk format (see package
// doc).
func encodeV2(tasks map[string][]byte) []byte {
	out := make([]byte, 16, 16+8*len(tasks))
	copy(out, v2Magic[:])
	binary.LittleEndian.PutUint32(out[8:12], version)
	binary.LittleEndian.PutUint32(out[12:16], uint32(len(tasks)))
	for id, raw := range tasks {
		var hdr [8]byte
		binary.LittleEndian.PutUint32(hdr[:4], uint32(len(id)))
		binary.LittleEndian.PutUint32(hdr[4:], uint32(len(raw)))
		out = append(out, hdr[:]...)
		out = append(out, id...)
		out = append(out, raw...)
	}
	return out
}

// decodeV2 parses the base format with full bounds checking: the fuzz
// target feeds arbitrary bytes, so a malformed snapshot must fail cleanly,
// never panic.
func decodeV2(data []byte) (map[string][]byte, error) {
	if len(data) < 16 || !bytes.Equal(data[:8], v2Magic[:]) {
		return nil, errors.New("bad magic")
	}
	if ver := binary.LittleEndian.Uint32(data[8:12]); ver != version {
		return nil, fmt.Errorf("unsupported version %d", ver)
	}
	count := binary.LittleEndian.Uint32(data[12:16])
	// Reject absurd counts up front (8 bytes of header per entry).
	if count > uint32(len(data)-16)/8 {
		return nil, errors.New("count exceeds payload")
	}
	out := make(map[string][]byte, count)
	pos := 16
	for i := uint32(0); i < count; i++ {
		if pos+8 > len(data) {
			return nil, errors.New("truncated entry header")
		}
		idLen := binary.LittleEndian.Uint32(data[pos : pos+4])
		envLen := binary.LittleEndian.Uint32(data[pos+4 : pos+8])
		pos += 8
		if idLen > uint32(len(data)-pos) {
			return nil, errors.New("truncated task id")
		}
		id := string(data[pos : pos+int(idLen)])
		pos += int(idLen)
		if envLen > uint32(len(data)-pos) {
			return nil, errors.New("truncated task body")
		}
		raw := data[pos : pos+int(envLen)]
		pos += int(envLen)
		out[id] = raw
	}
	return out, nil
}

// encodeDelta renders the pending ops in the v3 delta format.
func encodeDelta(ops []deltaEntry) []byte {
	out := make([]byte, 16, 16+13*len(ops))
	copy(out, deltaMagic[:])
	binary.LittleEndian.PutUint32(out[8:12], deltaVersion)
	binary.LittleEndian.PutUint32(out[12:16], uint32(len(ops)))
	for _, op := range ops {
		var hdr [9]byte
		binary.LittleEndian.PutUint32(hdr[:4], uint32(len(op.id)))
		if op.tomb {
			hdr[4] = opTomb
		} else {
			hdr[4] = opPut
			binary.LittleEndian.PutUint32(hdr[5:], uint32(len(op.raw)))
		}
		out = append(out, hdr[:]...)
		out = append(out, op.id...)
		out = append(out, op.raw...)
	}
	return out
}

// decodeDelta parses the v3 delta format with full bounds checking.
func decodeDelta(data []byte) ([]deltaEntry, error) {
	if len(data) < 16 || !bytes.Equal(data[:8], deltaMagic[:]) {
		return nil, errors.New("bad magic")
	}
	if ver := binary.LittleEndian.Uint32(data[8:12]); ver != deltaVersion {
		return nil, fmt.Errorf("unsupported version %d", ver)
	}
	count := binary.LittleEndian.Uint32(data[12:16])
	// Reject absurd counts up front (9 bytes of header per entry).
	if count > uint32(len(data)-16)/9 {
		return nil, errors.New("count exceeds payload")
	}
	out := make([]deltaEntry, 0, count)
	pos := 16
	for i := uint32(0); i < count; i++ {
		if pos+9 > len(data) {
			return nil, errors.New("truncated entry header")
		}
		idLen := binary.LittleEndian.Uint32(data[pos : pos+4])
		opc := data[pos+4]
		bodyLen := binary.LittleEndian.Uint32(data[pos+5 : pos+9])
		pos += 9
		if idLen > uint32(len(data)-pos) {
			return nil, errors.New("truncated task id")
		}
		id := string(data[pos : pos+int(idLen)])
		pos += int(idLen)
		if bodyLen > uint32(len(data)-pos) {
			return nil, errors.New("truncated entry body")
		}
		body := data[pos : pos+int(bodyLen)]
		pos += int(bodyLen)
		if opc == opTomb {
			out = append(out, deltaEntry{id: id, tomb: true})
		} else if opc == opPut {
			out = append(out, deltaEntry{id: id, raw: body})
		} else {
			return nil, fmt.Errorf("unknown op %d", opc)
		}
	}
	return out, nil
}

// writeAtomic performs the tmp+rename+dir-fsync sequence for name.
func (s *Store) writeAtomic(name string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(name), 0o700); err != nil {
		return fmt.Errorf("afep/taskstore: mkdir: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(name), filepath.Base(name)+".*.tmp")
	if err != nil {
		return fmt.Errorf("afep/taskstore: tmp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("afep/taskstore: write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("afep/taskstore: sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("afep/taskstore: close: %w", err)
	}
	if err := os.Rename(tmpName, name); err != nil {
		return fmt.Errorf("afep/taskstore: rename: %w", err)
	}
	// fsync the directory entry: without it the rename itself can be
	// lost on crash, resurrecting the previous snapshot.
	d, err := os.Open(filepath.Dir(name))
	if err != nil {
		return fmt.Errorf("afep/taskstore: open dir: %w", err)
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return fmt.Errorf("afep/taskstore: sync dir: %w", err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("afep/taskstore: close dir: %w", err)
	}
	return nil
}
