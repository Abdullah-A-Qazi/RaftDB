// Package storage will hold the persistence layer for Raft's durable state.
//
// Phase 0 defines only the contract; implementations arrive later:
//   - Phase 1: a simple file-based store for the hard state (currentTerm,
//     votedFor), synced before any RPC response.
//   - Phase 4: a real append-only write-ahead log for entries, with fsync
//     before acking and a crash-recovery path.
//   - Phase 5: snapshot files and log truncation.
package storage

import "github.com/Abdullah-A-Qazi/RaftDB/raft"

// HardState is the state Figure 2 marks as persistent (minus the log, which
// gets its own methods because it is append-heavy and will be stored
// differently). It must be on disk before the node answers any RPC that
// depends on it — see the persistence comments in package raft.
type HardState struct {
	CurrentTerm uint64
	VotedFor    string
}

// Store is the durability contract the Raft node will program against.
// Every method that mutates state must not return until the change is
// durable (fsync'd), because Raft's correctness proofs assume a node that
// answered an RPC remembers what it answered — even across kill -9.
type Store interface {
	// SaveHardState atomically persists term and vote.
	SaveHardState(hs HardState) error

	// LoadHardState returns the persisted hard state, or a zero
	// HardState and found=false on a fresh node.
	LoadHardState() (hs HardState, found bool, err error)

	// AppendEntries durably appends entries to the log.
	AppendEntries(entries []raft.LogEntry) error

	// TruncateSuffix durably removes all entries with index >= from.
	// Needed when a follower's log conflicts with the leader's and the
	// divergent tail must be discarded (§5.3).
	TruncateSuffix(from uint64) error

	// LoadEntries returns all persisted log entries in index order.
	LoadEntries() ([]raft.LogEntry, error)

	Close() error
}
