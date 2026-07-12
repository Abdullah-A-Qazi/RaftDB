// Package raft contains the from-scratch implementation of the Raft
// consensus algorithm (Ongaro & Ousterhout, "In Search of an Understandable
// Consensus Algorithm").
//
// Phase 0 defines only the state from the paper's Figure 2; behavior
// (elections, replication) arrives in later phases.
package raft

import (
	"fmt"
	"sync"
)

// State is the role a node is currently playing (§5.1). Every node starts as
// a follower; followers become candidates when they stop hearing from a
// leader, and candidates become leaders when they win an election.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("State(%d)", int(s))
	}
}

// None is the zero value for votedFor, standing in for the paper's "null":
// this node has not voted for anyone in the current term. It works as a
// sentinel because config validation rejects empty node IDs.
const None = ""

// LogEntry is one entry in the replicated log. This is a domain type,
// deliberately separate from the protobuf LogEntry: the core algorithm
// shouldn't be coupled to the wire encoding, and conversion happens at the
// RPC boundary (Phase 1+).
type LogEntry struct {
	// Term when the entry was created on the leader. Terms are how Raft
	// detects that two logs have diverged (Log Matching Property, §5.3).
	Term uint64

	// Index is the entry's 1-based position in the log. Explicit rather
	// than positional because after snapshotting (Phase 5) the in-memory
	// slice no longer starts at index 1.
	Index uint64

	// Command is the opaque state-machine command. Raft replicates it but
	// never interprets it; the KV encoding is defined in Phase 2.
	Command []byte
}

// RaftNode holds all Raft state for one member of the cluster, mirroring the
// paper's Figure 2 "State" box field-for-field.
type RaftNode struct {
	// mu guards every field below. A single coarse mutex is a deliberate
	// choice: Raft state transitions touch several fields at once
	// (e.g. granting a vote reads currentTerm and the log, then writes
	// votedFor), and finer-grained locking is where subtle races breed.
	// Performance is explicitly not a goal yet.
	mu sync.Mutex

	// id is this node's own ID from the cluster config.
	id string

	// peers is the IDs of every other cluster member. Static for now
	// (no membership changes, §6 is future work).
	peers []string

	// ---------------------------------------------------------------
	// Persistent state on all servers (Figure 2). These MUST be flushed
	// to stable storage before responding to any RPC — otherwise a node
	// could vote for candidate A, crash, restart with votedFor erased,
	// and vote for candidate B in the same term, electing two leaders.
	// Actual persistence is wired in Phase 1; the fields live here so
	// the shape is right from the start.
	// ---------------------------------------------------------------

	// currentTerm is the latest term this node has seen. Starts at 0,
	// increases monotonically. Terms act as Raft's logical clock: any RPC
	// carrying a higher term forces us to adopt it and step down.
	currentTerm uint64

	// votedFor is the candidate this node voted for in currentTerm, or
	// None. Persisting it is what enforces "one vote per term".
	votedFor string

	// log is the replicated log. Slot 0 holds a sentinel entry
	// {Term: 0, Index: 0} so real entries start at index 1 exactly as in
	// the paper — this keeps every index formula (prevLogIndex,
	// commitIndex, "last log index") identical to Figure 2 with no
	// off-by-one translation. The sentinel is also what prevLogIndex=0
	// matches against when appending the very first entry.
	log []LogEntry

	// ---------------------------------------------------------------
	// Volatile state on all servers (Figure 2). Safe to lose on crash:
	// commitIndex is rediscovered from the leader, lastApplied is
	// rebuilt by replaying the log into the state machine.
	// ---------------------------------------------------------------

	// commitIndex is the highest log index known to be committed, i.e.
	// safely replicated on a majority and never to be lost.
	commitIndex uint64

	// lastApplied is the highest log index actually fed to the state
	// machine. Always <= commitIndex; the gap is entries committed but
	// not yet applied.
	lastApplied uint64

	// ---------------------------------------------------------------
	// Volatile state on leaders (Figure 2), reinitialized after every
	// election win, keyed by peer ID.
	// ---------------------------------------------------------------

	// nextIndex is, per follower, the index of the next log entry to
	// send. Initialized optimistically to leader's last index + 1 and
	// decremented on AppendEntries rejections until logs match.
	nextIndex map[string]uint64

	// matchIndex is, per follower, the highest index known to be
	// replicated there. Initialized pessimistically to 0 (we know
	// nothing until a follower confirms). The leader advances
	// commitIndex by finding the highest N replicated on a majority of
	// matchIndex values — subject to the current-term rule (Phase 3).
	matchIndex map[string]uint64

	// state is this node's current role. Volatile: a restarted node
	// always comes back as a follower.
	state State
}

// NewNode creates a RaftNode with the initial state Figure 2 prescribes for
// a server that has never run before: term 0, no vote cast, empty log,
// nothing committed or applied, follower role.
func NewNode(id string, peers []string) *RaftNode {
	return &RaftNode{
		id:    id,
		peers: peers,
		state: Follower,

		currentTerm: 0,
		votedFor:    None,
		// Sentinel entry; see the log field comment.
		log: []LogEntry{{Term: 0, Index: 0}},

		commitIndex: 0,
		lastApplied: 0,

		// Leader-only maps are allocated lazily on election win
		// (Phase 1); leaving them nil here makes accidental use
		// while not leader loud in tests.
		nextIndex:  nil,
		matchIndex: nil,
	}
}

// ID returns this node's ID.
func (rn *RaftNode) ID() string {
	return rn.id
}

// lastLogIndex returns the index of the last entry in the log. The sentinel
// at slot 0 makes this well-defined even for an "empty" log (it returns 0,
// which is what RequestVote's last_log_index should carry then).
//
// Callers must hold rn.mu.
func (rn *RaftNode) lastLogIndex() uint64 {
	return rn.log[len(rn.log)-1].Index
}

// lastLogTerm returns the term of the last entry in the log (0 for an empty
// log, via the sentinel).
//
// Callers must hold rn.mu.
func (rn *RaftNode) lastLogTerm() uint64 {
	return rn.log[len(rn.log)-1].Term
}

// Status is a read-only snapshot of a node's externally observable state,
// for tests, logging, and (in Phase 7) the dashboard.
type Status struct {
	ID          string
	State       State
	CurrentTerm uint64
	VotedFor    string
	CommitIndex uint64
	LastApplied uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

// Status returns a consistent snapshot of the node's state.
func (rn *RaftNode) Status() Status {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return Status{
		ID:           rn.id,
		State:        rn.state,
		CurrentTerm:  rn.currentTerm,
		VotedFor:     rn.votedFor,
		CommitIndex:  rn.commitIndex,
		LastApplied:  rn.lastApplied,
		LastLogIndex: rn.lastLogIndex(),
		LastLogTerm:  rn.lastLogTerm(),
	}
}
