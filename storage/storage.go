// Package storage holds the persistence layer for Raft's durable state.
//
// Phase 1 implements FileStore for the hard state (currentTerm, votedFor).
// The log's append-only write-ahead log arrives in Phase 4, and snapshot
// files in Phase 5 — the log stays in memory until then.
//
// (Phase 0 sketched one wide Store interface here; it was split so that the
// interface raft depends on — raft.StateStore — lives in package raft. That
// keeps the dependency arrow pointing one way: storage imports raft's
// types, raft never imports storage.)
package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Abdullah-A-Qazi/RaftDB/raft"
)

// FileStore is the durability layer for one node: it implements both
// raft.StateStore and raft.LogStore, backed by two files in the node's
// directory with deliberately different shapes:
//
//   - hardstate.json — two overwrite-in-place fields (currentTerm,
//     votedFor), replaced atomically on every save (temp + fsync + rename).
//   - wal.log — the replicated log as an append-only record stream
//     (see wal.go).
//
// Overwrite-shaped data gets atomic replacement; append-shaped data gets a
// WAL. Using one mechanism for both would make one of them either unsafe
// or needlessly slow.
type FileStore struct {
	dir  string
	path string
	wal  *os.File
}

const hardStateFile = "hardstate.json"

// NewFileStore creates (if needed) dir and returns a store rooted there.
// Each node must use its own directory, and at most one live process may
// use a directory at a time (no lock file yet — flagged in docs/phase-4).
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: creating %s: %w", dir, err)
	}
	wal, err := openWAL(dir)
	if err != nil {
		return nil, err
	}
	return &FileStore{dir: dir, path: filepath.Join(dir, hardStateFile), wal: wal}, nil
}

// SaveHardState durably replaces the stored hard state. The sequence is:
//
//	write to a temp file → fsync it → rename over the real file → fsync dir
//
// Why each step matters:
//   - temp file + rename: a crash mid-write must never leave a torn,
//     half-JSON state file. rename(2) is atomic on POSIX filesystems, so a
//     reader sees either the complete old state or the complete new one.
//   - fsync before rename: otherwise the rename can hit disk before the
//     file's *contents* do, and a power cut leaves the new name pointing at
//     garbage.
//   - fsync the directory: the rename itself is a directory mutation; until
//     the directory is synced, a crash can silently undo it.
//
// Only after all of that may the caller answer an RPC — a vote that isn't
// on disk is a vote we could repeat after a restart.
func (s *FileStore) SaveHardState(hs raft.HardState) error {
	data, err := json.Marshal(hs)
	if err != nil {
		return fmt.Errorf("storage: encoding hard state: %w", err)
	}
	if err := s.atomicWrite(hardStateFile, data); err != nil {
		return fmt.Errorf("storage: writing hard state: %w", err)
	}
	return nil
}

// atomicWrite durably replaces dir/name with data via the temp → fsync →
// rename → fsync-dir sequence (see SaveHardState's doc comment for why each
// step matters). Shared by hard state and snapshots — everything
// overwrite-shaped.
func (s *FileStore) atomicWrite(name string, data []byte) error {
	tmp, err := os.CreateTemp(s.dir, name+".tmp-*")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpName := tmp.Name()
	// Best-effort cleanup on any failure path below.
	defer os.Remove(tmpName)

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("writing temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("syncing temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}
	if err := os.Rename(tmpName, filepath.Join(s.dir, name)); err != nil {
		return fmt.Errorf("replacing %s: %w", name, err)
	}
	return syncDir(s.dir)
}

// LoadHardState reads the persisted hard state; found is false on a node
// that has never saved (a fresh cluster member).
func (s *FileStore) LoadHardState() (raft.HardState, bool, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return raft.HardState{}, false, nil
	}
	if err != nil {
		return raft.HardState{}, false, fmt.Errorf("storage: reading hard state: %w", err)
	}
	var hs raft.HardState
	if err := json.Unmarshal(data, &hs); err != nil {
		// A torn file here would mean the atomic-rename invariant was
		// violated (or the file was hand-edited); refuse to guess.
		return raft.HardState{}, false, fmt.Errorf("storage: corrupt hard state %s: %w", s.path, err)
	}
	return hs, true, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
