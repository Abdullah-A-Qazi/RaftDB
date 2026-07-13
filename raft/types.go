package raft

import "context"

// HardState is the state Figure 2 marks as persistent, minus the log (which
// is append-heavy and gets its own storage treatment in Phase 4).
//
// Lives in package raft rather than package storage because raft is the
// layer that must call Save at the right moments; storage implements the
// interface and may import raft's types, never the reverse.
type HardState struct {
	CurrentTerm uint64
	VotedFor    string
}

// StateStore is the durability contract for HardState. SaveHardState must
// not return until the state is actually on disk (fsync'd): Raft's proofs
// assume that a node which answered an RPC still remembers what it answered
// after a crash. If votedFor could evaporate in a kill -9, a node could vote
// for candidate A, restart, and vote for candidate B in the same term —
// electing two leaders for one term, the exact thing Raft exists to prevent.
type StateStore interface {
	SaveHardState(HardState) error
	LoadHardState() (hs HardState, found bool, err error)
}

// LogStore is the durability contract for the replicated log itself: an
// append-only write-ahead log. Like StateStore, every mutating call must
// not return until the change is physically on disk (fsync'd) — a follower
// that acknowledged entries it forgets in a crash breaks the majority
// arithmetic commitment rests on (the leader counted that follower; if the
// entry silently vanishes, a "committed" entry may survive on fewer nodes
// than a quorum).
//
// The log is mutated in exactly two ways, so the interface has exactly two
// mutations: append at the tail, truncate a suffix (never edits in the
// middle, never truncates a prefix — that's Phase 5 compaction).
type LogStore interface {
	// AppendEntries durably appends entries at the tail of the log.
	AppendEntries(entries []LogEntry) error

	// TruncateSuffix durably removes every entry with index >= from.
	TruncateSuffix(from uint64) error

	// LoadEntries returns the entire persisted log in index order,
	// reflecting all prior appends and truncations. Called once at
	// startup; recovery from a torn final write (crash mid-append) is the
	// implementation's job — a torn record was never acknowledged to
	// anyone and must simply be discarded.
	LoadEntries() ([]LogEntry, error)
}

// RequestVoteArgs / RequestVoteReply mirror the proto messages in
// rpc/raft.proto. The core algorithm uses these domain types so it is not
// coupled to the wire encoding; the rpc package converts at the boundary.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool

	// Fast-backtracking hints (§5.3), set only when Success is false due to
	// a failed consistency check. ConflictTerm is the term of the
	// follower's entry at PrevLogIndex (0 if its log is shorter than
	// PrevLogIndex), ConflictIndex is the follower's first index carrying
	// ConflictTerm (or its lastIndex+1 when the log was too short).
	ConflictIndex uint64
	ConflictTerm  uint64
}

// Transport sends RPCs to one peer. Implementations: real gRPC (package
// rpc) for production, an in-memory router (package test) for cluster tests
// — the latter is what lets Phase 6 simulate partitions without processes.
//
// Locking rule: RaftNode never calls Transport while holding its own mutex.
// With the in-memory transport an RPC is a direct method call into the
// peer's handler, which takes the peer's mutex; two nodes calling each other
// while holding their own locks would deadlock. (It would also be wrong for
// gRPC: an fsync-slow peer would stall this node's every state transition.)
type Transport interface {
	RequestVote(ctx context.Context, peerID string, args RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(ctx context.Context, peerID string, args AppendEntriesArgs) (AppendEntriesReply, error)
	// InstallSnapshot is added in Phase 5.
}
