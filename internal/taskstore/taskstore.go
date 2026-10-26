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
}

// Open loads the store from path; a missing file starts fresh.
func Open(path string) (*Store, error) {
	s := &Store{path: path, tasks: make(map[string]*afepv1.TaskEnvelope)}
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
	s.tasks[env.GetTaskId()] = env
	return s.flushLocked()
}

// Get returns the stored envelope (callers own the returned value).
func (s *Store) Get(id string) (*afepv1.TaskEnvelope, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	env, ok := s.tasks[id]
	return env, ok
}

// Len reports how many tasks are stored.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tasks)
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
	st := diskState{Version: version, Tasks: make(map[string]json.RawMessage, len(s.tasks))}
	marshal := protojson.MarshalOptions{}
	for id, env := range s.tasks {
		raw, err := marshal.Marshal(env)
		if err != nil {
			return fmt.Errorf("afep/taskstore: marshal %s: %w", id, err)
		}
		st.Tasks[id] = raw
	}
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
	return nil
}
