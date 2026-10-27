// Package taskstore persists TaskEnvelopes across restarts using atomic
// file writes (tmp + rename).
package taskstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const version = 1

type diskState struct {
	Version int                        `json:"version"`
	Tasks   map[string]json.RawMessage `json:"tasks"`
}

// Store is an in-memory task map backed by a JSON file.
type Store struct {
	mu    sync.RWMutex
	path  string
	tasks map[string]*afepv1.TaskEnvelope
	// raws caches the protojson encoding of each stored envelope so a
	// flush re-serializes only the task that changed, never all of them.
	raws map[string]json.RawMessage
	// maxTasks caps stored envelopes (0 = unlimited): prune-age=0
	// used to leave the store without any bound at all.
	maxTasks int
}

// Open loads the store from path; a missing file starts fresh.
func Open(path string) (*Store, error) {
	s := &Store{
		path:  path,
		tasks: make(map[string]*afepv1.TaskEnvelope),
		raws:  make(map[string]json.RawMessage),
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("afep/taskstore: read: %w", err)
	}
	var st diskState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("afep/taskstore: parse %s: %w", path, err)
	}
	if st.Version != version {
		return nil, fmt.Errorf("afep/taskstore: unsupported version %d", st.Version)
	}
	unmarshal := protojson.UnmarshalOptions{DiscardUnknown: false}
	for id, raw := range st.Tasks {
		env := &afepv1.TaskEnvelope{}
		if err := unmarshal.Unmarshal(raw, env); err != nil {
			return nil, fmt.Errorf("afep/taskstore: task %s: %w", id, err)
		}
		s.tasks[id] = env
		s.raws[id] = raw // byte-preserved: no re-encode at boot
	}
	return s, nil
}

// Put stores or replaces an envelope and flushes atomically.
func (s *Store) Put(env *afepv1.TaskEnvelope) error {
	if env == nil || env.GetTaskId() == "" {
		return fmt.Errorf("afep/taskstore: envelope needs a task id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := env.GetTaskId()
	if _, exists := s.tasks[id]; !exists && s.maxTasks > 0 && len(s.tasks) >= s.maxTasks {
		return fmt.Errorf("afep/taskstore: task limit %d reached", s.maxTasks)
	}
	// Store-owned copy: callers may keep mutating their envelope while
	// flushes read ours — one clone keeps memory, the raws cache and the
	// file in lockstep (and kills the Probe/Commit aliasing race).
	owned := proto.Clone(env).(*afepv1.TaskEnvelope)
	// protojson runs once here for THIS envelope; flushes below emit
	// the cached bytes instead of re-encoding every stored task.
	raw, err := (protojson.MarshalOptions{}).Marshal(owned)
	if err != nil {
		return fmt.Errorf("afep/taskstore: marshal %s: %w", id, err)
	}
	s.tasks[id] = owned
	s.raws[id] = raw
	return s.flushLocked()
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
// updated_at). It returns the number of removed tasks.
func (s *Store) PruneTerminal(maxAge time.Duration, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for id, env := range s.tasks {
		terminal := env.GetState() == afepv1.TaskState_TASK_STATE_DONE ||
			env.GetState() == afepv1.TaskState_TASK_STATE_FAILED
		age := now.Sub(time.Unix(env.GetUpdatedAtUnix(), 0))
		if terminal && age > maxAge {
			delete(s.tasks, id)
			delete(s.raws, id)
			removed++
		}
	}
	if removed > 0 {
		return removed, s.flushLocked()
	}
	return 0, nil
}

func (s *Store) flushLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("afep/taskstore: mkdir: %w", err)
	}
	// Same full-state snapshot semantics; the per-task protojson cost
	// was O(all tasks) per Put and now lives only in Put itself.
	st := diskState{Version: version, Tasks: s.raws}
	data, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("afep/taskstore: encode: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".tasks-*.tmp")
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
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("afep/taskstore: rename: %w", err)
	}
	// fsync the directory entry: without it the rename itself can be
	// lost on crash, resurrecting the previous snapshot.
	d, err := os.Open(filepath.Dir(s.path))
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
