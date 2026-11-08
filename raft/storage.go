package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// ErrInvalidState indicates that a decoded stable state violates basic Raft invariants.
var ErrInvalidState = errors.New("raft: invalid persistent state")

// PersistentState is the Raft state that must survive a restart. CommitIndex is
// deliberately not included: it can be reconstructed from snapshots and the log.
type PersistentState struct {
	CurrentTerm uint64     `json:"current_term"`
	VotedFor    uint64     `json:"voted_for,omitempty"`
	HasVoted    bool       `json:"has_voted"`
	Log         []LogEntry `json:"log"`
}

// StableStore persists a node's term, vote, and log before protocol responses
// that depend on those values are sent.
type StableStore interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

// MemoryStore is a concurrency-safe store useful for simulations and tests.
type MemoryStore struct {
	mu    sync.Mutex
	state PersistentState
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (s *MemoryStore) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clonePersistentState(s.state), nil
}

func (s *MemoryStore) Save(state PersistentState) error {
	if err := validatePersistentState(state); err != nil {
		return err
	}
	s.mu.Lock()
	s.state = clonePersistentState(state)
	s.mu.Unlock()
	return nil
}

// FileStore stores one JSON state file. Save uses a temporary file, fsync, and
// atomic rename so a crash cannot leave a partially written state file.
type FileStore struct {
	mu   sync.Mutex
	path string
}

func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, errors.New("raft: persistent state path is empty")
	}
	return &FileStore{path: path}, nil
}

func (s *FileStore) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return PersistentState{}, nil
	}
	if err != nil {
		return PersistentState{}, fmt.Errorf("raft: open state: %w", err)
	}
	defer file.Close()

	var state PersistentState
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&state); err != nil {
		return PersistentState{}, fmt.Errorf("raft: decode state: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return PersistentState{}, fmt.Errorf("raft: trailing data in state file")
		}
		return PersistentState{}, fmt.Errorf("raft: read state trailer: %w", err)
	}
	if err := validatePersistentState(state); err != nil {
		return PersistentState{}, err
	}
	return clonePersistentState(state), nil
}

func (s *FileStore) Save(state PersistentState) error {
	if err := validatePersistentState(state); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("raft: create state directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".raft-state-*.tmp")
	if err != nil {
		return fmt.Errorf("raft: create temporary state: %w", err)
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()

	encoder := json.NewEncoder(tmp)
	if err := encoder.Encode(state); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("raft: encode state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("raft: sync state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("raft: close state: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("raft: replace state: %w", err)
	}
	committed = true

	// Sync the directory entry as well as file contents where supported.
	if directory, err := os.Open(dir); err == nil {
		syncErr := directory.Sync()
		closeErr := directory.Close()
		if syncErr != nil {
			return fmt.Errorf("raft: sync state directory: %w", syncErr)
		}
		if closeErr != nil {
			return fmt.Errorf("raft: close state directory: %w", closeErr)
		}
	}
	return nil
}

func validatePersistentState(state PersistentState) error {
	if state.HasVoted && state.VotedFor == 0 {
		return fmt.Errorf("%w: vote has no candidate id", ErrInvalidState)
	}
	for i, entry := range state.Log {
		if entry.Term > state.CurrentTerm {
			return fmt.Errorf("%w: log entry %d is from a future term", ErrInvalidState, i+1)
		}
	}
	return nil
}

func clonePersistentState(state PersistentState) PersistentState {
	copyState := state
	copyState.Log = make([]LogEntry, len(state.Log))
	for i, entry := range state.Log {
		copyState.Log[i] = entry
		copyState.Log[i].Nodes = append([]uint64(nil), entry.Nodes...)
	}
	return copyState
}
