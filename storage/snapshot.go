package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Abdullah-A-Qazi/RaftDB/raft"
)

// Snapshot persistence: a single JSON file replaced atomically, exactly the
// hardstate.json discipline (temp → fsync → rename → fsync dir) — a
// snapshot is overwrite-shaped data (only the latest matters), not
// append-shaped, so it gets the atomic-replace treatment rather than a WAL.
// The write path's crash contract: a reader either sees the complete old
// snapshot or the complete new one, never a torn hybrid — which is what
// lets HandleInstallSnapshot and the applier compact the WAL immediately
// after SaveSnapshot returns.

const snapshotFile = "snapshot.json"

type snapshotEnvelope struct {
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"` // state machine bytes (base64 in JSON)
}

// SaveSnapshot implements raft.SnapshotStore.
func (s *FileStore) SaveSnapshot(snap raft.Snapshot) error {
	payload, err := json.Marshal(snapshotEnvelope{
		LastIncludedIndex: snap.LastIncludedIndex,
		LastIncludedTerm:  snap.LastIncludedTerm,
		Data:              snap.Data,
	})
	if err != nil {
		return fmt.Errorf("storage: encoding snapshot: %w", err)
	}
	if err := s.atomicWrite(snapshotFile, payload); err != nil {
		return fmt.Errorf("storage: writing snapshot: %w", err)
	}
	return nil
}

// LoadSnapshot implements raft.SnapshotStore.
func (s *FileStore) LoadSnapshot() (raft.Snapshot, bool, error) {
	data, err := os.ReadFile(filepath.Join(s.dir, snapshotFile))
	if os.IsNotExist(err) {
		return raft.Snapshot{}, false, nil
	}
	if err != nil {
		return raft.Snapshot{}, false, fmt.Errorf("storage: reading snapshot: %w", err)
	}
	var env snapshotEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		// Atomic replacement makes a torn snapshot impossible under the
		// crash-only model; corruption here is refused, same policy as
		// hard state.
		return raft.Snapshot{}, false, fmt.Errorf("storage: corrupt snapshot: %w", err)
	}
	return raft.Snapshot{
		LastIncludedIndex: env.LastIncludedIndex,
		LastIncludedTerm:  env.LastIncludedTerm,
		Data:              env.Data,
	}, true, nil
}
