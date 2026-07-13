// Package raft contains the from-scratch implementation of the Raft
// consensus algorithm (Ongaro & Ousterhout, "In Search of an Understandable
// Consensus Algorithm").
//
// Phase 1 implemented leader election (§5.2). Phase 2 adds log replication
// (§5.3): Propose appends client commands to the leader's log, followers
// replicate them via AppendEntries, entries commit on majority
// acknowledgment, and committed entries are fed to a StateMachine in log
// order. The remaining safety rules (§5.4) arrive in Phase 3.
package raft

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// ErrNotLeader is returned by Propose on a non-leader. The caller should
// redirect the client to Status().LeaderID.
var ErrNotLeader = errors.New("raft: not the leader")

// StateMachine consumes committed log entries. Apply is called exactly once
// per entry per node lifetime, in strictly ascending index order, from a
// single goroutine — the state machine needs no locking against raft itself.
// Raft never interprets commands; whatever Apply does must be deterministic,
// because every node replays the same sequence and must end in the same
// state.
type StateMachine interface {
	Apply(entry LogEntry)
}

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

// None is the zero value for votedFor and leaderID, standing in for the
// paper's "null": no vote cast / no known leader. It works as a sentinel
// because config validation rejects empty node IDs.
const None = ""

// LogEntry is one entry in the replicated log. This is a domain type,
// deliberately separate from the protobuf LogEntry: the core algorithm
// shouldn't be coupled to the wire encoding, and conversion happens at the
// RPC boundary.
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

// Config carries everything a RaftNode needs at construction time.
type Config struct {
	// ID is this node's ID from the cluster config; Peers is every other
	// member. Quorum size is derived from len(Peers)+1, so all nodes must
	// agree on membership (enforced by sharing one config file).
	ID    string
	Peers []string

	// Store persists HardState. Required.
	Store StateStore

	// LogStore persists the replicated log (Phase 4). May be nil ONLY in
	// tests: a nil LogStore means the log is volatile, which silently
	// voids the durability half of Raft's guarantees — raftd always wires
	// one.
	LogStore LogStore

	// Transport sends RPCs to peers. Required by Start; may be nil for
	// tests that only poke the RPC handlers.
	Transport Transport

	// ElectionTimeoutMin/Max bound the randomized election timeout
	// (defaults 150–300ms, the paper's suggested range §5.6).
	// Randomization is load-bearing, not cosmetic: if all nodes timed out
	// identically, every split vote would repeat forever — spreading the
	// timeouts makes one node usually time out first, win, and heartbeat
	// the others down (§5.2).
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// HeartbeatInterval is how often a leader sends empty AppendEntries
	// (default 50ms). Must be well under ElectionTimeoutMin or followers
	// will start elections against a healthy leader.
	HeartbeatInterval time.Duration

	// RPCTimeout bounds each outgoing RPC (default 100ms). A dead peer
	// must not be able to wedge an election round.
	RPCTimeout time.Duration

	Logger *slog.Logger
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.ElectionTimeoutMin == 0 {
		out.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if out.ElectionTimeoutMax == 0 {
		out.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if out.HeartbeatInterval == 0 {
		out.HeartbeatInterval = 50 * time.Millisecond
	}
	if out.RPCTimeout == 0 {
		out.RPCTimeout = 100 * time.Millisecond
	}
	if out.Logger == nil {
		out.Logger = slog.Default()
	}
	return out
}

// RaftNode holds all Raft state for one member of the cluster, mirroring the
// paper's Figure 2 "State" box field-for-field.
type RaftNode struct {
	// mu guards every field below. A single coarse mutex is a deliberate
	// choice: Raft state transitions touch several fields at once
	// (e.g. granting a vote reads currentTerm and the log, then writes
	// votedFor), and finer-grained locking is where subtle races breed.
	// Performance is explicitly not a goal yet.
	//
	// Invariant: no code calls cfg.Transport while holding mu (see the
	// Transport doc comment for why).
	mu sync.Mutex

	cfg    Config
	logger *slog.Logger

	// id is this node's own ID; peers is everyone else.
	id    string
	peers []string

	// ---------------------------------------------------------------
	// Persistent state on all servers (Figure 2). Flushed to disk via
	// persistLocked() before any RPC response or request that depends
	// on them leaves this node.
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
	// (In-memory only until Phase 4's WAL; empty in Phase 1 anyway.)
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
	// machine. Always <= commitIndex.
	lastApplied uint64

	// ---------------------------------------------------------------
	// Volatile state on leaders (Figure 2), reinitialized after every
	// election win, keyed by peer ID.
	// ---------------------------------------------------------------

	// nextIndex is, per follower, the index of the next log entry to
	// send. Initialized optimistically to leader's last index + 1.
	nextIndex map[string]uint64

	// matchIndex is, per follower, the highest index known to be
	// replicated there. Initialized pessimistically to 0.
	matchIndex map[string]uint64

	// ---------------------------------------------------------------
	// Election machinery (volatile).
	// ---------------------------------------------------------------

	// state is this node's current role. A restarted node always comes
	// back as a follower (§5.1).
	state State

	// leaderID is the last known leader of currentTerm (learned from
	// AppendEntries), used for client redirection in Phase 2.
	leaderID string

	// electionResetAt is when the election timer last restarted, and
	// electionTimeout is the current randomized duration; the ticker
	// goroutine starts an election when now - electionResetAt exceeds
	// electionTimeout while not leader.
	electionResetAt time.Time
	electionTimeout time.Duration

	// stateMachine receives committed entries (may be nil: raft still
	// tracks lastApplied, it just has nowhere to deliver).
	stateMachine StateMachine

	// applyCond (on mu) wakes the applier goroutine whenever commitIndex
	// advances.
	applyCond *sync.Cond

	// triggerCh kicks the leader's replication loop ahead of the next
	// heartbeat tick — buffered(1) so a kick never blocks and repeated
	// kicks coalesce into one round.
	triggerCh chan struct{}

	stopCh  chan struct{}
	wg      sync.WaitGroup
	started bool
	stopped bool
}

// NewNode creates a RaftNode. A fresh node gets the initial state Figure 2
// prescribes (term 0, no vote, empty log, follower); a node with prior
// durable state recovers currentTerm and votedFor from its store — that
// recovery is what makes "restart and vote again in the same term"
// impossible.
func NewNode(cfg Config) (*RaftNode, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("raft: config.ID is required")
	}
	if cfg.Store == nil {
		return nil, fmt.Errorf("raft: config.Store is required")
	}
	cfg = cfg.withDefaults()
	if cfg.ElectionTimeoutMin > cfg.ElectionTimeoutMax {
		return nil, fmt.Errorf("raft: ElectionTimeoutMin > ElectionTimeoutMax")
	}
	if cfg.HeartbeatInterval >= cfg.ElectionTimeoutMin {
		return nil, fmt.Errorf("raft: HeartbeatInterval (%v) must be well below ElectionTimeoutMin (%v)",
			cfg.HeartbeatInterval, cfg.ElectionTimeoutMin)
	}

	rn := &RaftNode{
		cfg:    cfg,
		logger: cfg.Logger.With("node", cfg.ID),
		id:     cfg.ID,
		peers:  cfg.Peers,
		state:  Follower,

		currentTerm: 0,
		votedFor:    None,
		// Sentinel entry; see the log field comment.
		log: []LogEntry{{Term: 0, Index: 0}},

		triggerCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
	}
	rn.applyCond = sync.NewCond(&rn.mu)

	hs, found, err := cfg.Store.LoadHardState()
	if err != nil {
		return nil, fmt.Errorf("raft: loading hard state: %w", err)
	}
	if found {
		rn.currentTerm = hs.CurrentTerm
		rn.votedFor = hs.VotedFor
		rn.logger.Info("recovered hard state", "term", rn.currentTerm, "votedFor", rn.votedFor)
	}

	// Recover the log (Phase 4). commitIndex and lastApplied deliberately
	// stay 0 — Figure 2 marks them volatile. We re-learn commitIndex from
	// the next leader's AppendEntries (or by becoming leader), and the
	// applier then replays the committed prefix into the state machine,
	// rebuilding it from scratch. Persisting commitIndex would only be an
	// optimization; persisting the log is what's load-bearing.
	if cfg.LogStore != nil {
		entries, err := cfg.LogStore.LoadEntries()
		if err != nil {
			return nil, fmt.Errorf("raft: loading log: %w", err)
		}
		for i, e := range entries {
			// The sentinel occupies slot 0, so entry i must be index i+1;
			// anything else means the store handed us a corrupt log and
			// every index formula in this package would be silently wrong.
			if e.Index != uint64(i)+1 {
				return nil, fmt.Errorf("raft: recovered log is not contiguous: entry %d has index %d", i, e.Index)
			}
			rn.log = append(rn.log, e)
		}
		if len(entries) > 0 {
			rn.logger.Info("recovered log", "entries", len(entries),
				"lastIndex", rn.lastLogIndex(), "lastTerm", rn.lastLogTerm())
		}
	}
	return rn, nil
}

// Start launches the election timer. The node begins as a follower and will
// start an election if it hears from no leader within its (randomized)
// election timeout.
func (rn *RaftNode) Start() error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.cfg.Transport == nil {
		return fmt.Errorf("raft: config.Transport is required to Start")
	}
	if rn.started {
		return fmt.Errorf("raft: already started")
	}
	rn.started = true
	rn.resetElectionTimerLocked()
	rn.wg.Add(2)
	go rn.runElectionTicker()
	go rn.runApplier()
	return nil
}

// SetStateMachine wires the consumer of committed entries. Must be called
// before Start (it exists as a setter, rather than a Config field, only to
// break the chicken-and-egg between constructing the KV server — which
// needs the node — and the node — which needs the state machine).
func (rn *RaftNode) SetStateMachine(sm StateMachine) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.started {
		panic("raft: SetStateMachine after Start")
	}
	rn.stateMachine = sm
}

// Stop shuts down the node's goroutines. In-flight RPCs are not waited for;
// their late replies land on this (now inert) instance and are discarded by
// the term/state guards.
func (rn *RaftNode) Stop() {
	rn.mu.Lock()
	if rn.stopped {
		rn.mu.Unlock()
		return
	}
	rn.stopped = true
	rn.applyCond.Broadcast() // wake the applier so it can observe stopped
	rn.mu.Unlock()
	close(rn.stopCh)
	rn.wg.Wait()
}

// Propose asks the leader to replicate a state-machine command. It returns
// the log index and term the command was appended at — the command is
// committed once (and only if) that exact (index, term) pair gets applied;
// if a different term shows up at that index, leadership changed and the
// command was lost (the caller may safely retry against the new leader).
//
// Propose returns before the entry is committed. Callers who need to know
// the outcome (the KV server does) watch Apply for the (index, term) pair.
func (rn *RaftNode) Propose(command []byte) (index, term uint64, err error) {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		return 0, 0, ErrNotLeader
	}
	index = rn.lastLogIndex() + 1
	term = rn.currentTerm
	// Durable before anything counts it: the leader is one of the quorum
	// members backing this entry, so its own copy must survive a crash
	// just like a follower's must before its ack.
	rn.appendLogLocked(LogEntry{Term: term, Index: index, Command: command})
	rn.advanceCommitIndexLocked() // single-node cluster commits immediately
	rn.mu.Unlock()

	// Kick replication now instead of waiting out the heartbeat tick.
	select {
	case rn.triggerCh <- struct{}{}:
	default: // a kick is already pending; one round covers both
	}
	return index, term, nil
}

// ID returns this node's ID.
func (rn *RaftNode) ID() string {
	return rn.id
}

// persistLocked writes HardState durably. It MUST be called after any
// mutation of currentTerm/votedFor and before the lock is released — i.e.
// before any RPC reply or request reflecting the new state can leave this
// node. Callers must hold rn.mu.
//
// A store failure panics: Raft's model is fail-stop. A node whose disk
// cannot record its vote must crash rather than keep answering RPCs it will
// not remember after a restart (that amnesia is how a term gets two
// leaders).
func (rn *RaftNode) persistLocked() {
	err := rn.cfg.Store.SaveHardState(HardState{
		CurrentTerm: rn.currentTerm,
		VotedFor:    rn.votedFor,
	})
	if err != nil {
		panic(fmt.Sprintf("raft %s: persisting hard state: %v", rn.id, err))
	}
}

// resetElectionTimerLocked restarts the election countdown with a fresh
// random duration. Per Figure 2 this happens when (a) we accept an
// AppendEntries from the current leader, (b) we grant a vote, or (c) we
// start an election ourselves. Notably it does NOT happen when we merely
// receive (and deny) some candidate's RequestVote — a candidate that cannot
// win must not be able to indefinitely postpone our own candidacy.
//
// Callers must hold rn.mu.
func (rn *RaftNode) resetElectionTimerLocked() {
	rn.electionResetAt = time.Now()
	spread := rn.cfg.ElectionTimeoutMax - rn.cfg.ElectionTimeoutMin
	rn.electionTimeout = rn.cfg.ElectionTimeoutMin
	if spread > 0 {
		rn.electionTimeout += rand.N(spread)
	}
}

// becomeFollowerLocked steps down into the follower role because we saw
// evidence of term `term` (>= currentTerm).
//
// votedFor is cleared ONLY when the term actually advances. A candidate
// stepping down within the same term (because a leader for that term
// appeared) must keep its self-vote: clearing it would let this node vote a
// second time in the same term, and two votes from one node is how two
// leaders get elected for one term.
//
// The election timer is reset only on a term change — entering a new term
// is fresh evidence of an active election/leader, and it prevents a
// stampede of ex-leaders/candidates whose stale timers would otherwise fire
// instantly. (Strictly, Figure 2 resets only on heartbeat/vote-grant; this
// is a mild, widely used deviation — flagged in docs/phase-1.)
//
// Callers must hold rn.mu and must persist before releasing it if the term
// changed (this function persists, so that holds automatically).
func (rn *RaftNode) becomeFollowerLocked(term uint64) {
	if term > rn.currentTerm {
		rn.logger.Info("entering new term as follower", "term", term, "prevTerm", rn.currentTerm)
		rn.currentTerm = term
		rn.votedFor = None
		rn.leaderID = None
		rn.persistLocked()
		rn.resetElectionTimerLocked()
	} else if rn.state != Follower {
		rn.logger.Info("stepping down to follower", "term", rn.currentTerm)
	}
	rn.state = Follower
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

// termAt returns the term of the log entry at the given index (0 at the
// sentinel index 0). While the log is never compacted, a log index IS the
// slice position (the sentinel keeps them aligned); Phase 5's snapshotting
// will change that mapping, and centralizing the arithmetic here (and in
// entriesFrom/truncateFrom) means it changes in one place.
//
// Callers must hold rn.mu and ensure index <= lastLogIndex().
func (rn *RaftNode) termAt(index uint64) uint64 {
	return rn.log[index].Term
}

// entriesFrom returns a copy of all entries with index >= from. It must
// copy: the caller sends the result outside the lock, and the underlying
// array would otherwise be shared with a log that later gets truncated and
// re-appended (a data race, and worse, silently mutating in-flight RPCs).
//
// Callers must hold rn.mu and ensure from <= lastLogIndex()+1.
func (rn *RaftNode) entriesFrom(from uint64) []LogEntry {
	tail := rn.log[from:]
	if len(tail) == 0 {
		return nil
	}
	out := make([]LogEntry, len(tail))
	copy(out, tail)
	return out
}

// appendLogLocked appends entries to the log, durably first: the WAL write
// (with fsync) completes before the in-memory log — and therefore before
// any reply or quorum count derived from it — can observe the entries.
// This is the log's half of the persist-before-reply invariant; HardState's
// half lives in persistLocked. A LogStore failure panics for the same
// fail-stop reason.
//
// Callers must hold rn.mu.
func (rn *RaftNode) appendLogLocked(entries ...LogEntry) {
	if rn.cfg.LogStore != nil {
		if err := rn.cfg.LogStore.AppendEntries(entries); err != nil {
			panic(fmt.Sprintf("raft %s: persisting log entries: %v", rn.id, err))
		}
	}
	rn.log = append(rn.log, entries...)
}

// truncateLogLocked drops every entry with index >= from (from must be
// >= 1; the sentinel is never dropped), durably. Only ever called on
// conflicting, uncommitted suffixes — see HandleAppendEntries for the
// guard rails.
//
// Callers must hold rn.mu.
func (rn *RaftNode) truncateLogLocked(from uint64) {
	if rn.cfg.LogStore != nil {
		if err := rn.cfg.LogStore.TruncateSuffix(from); err != nil {
			panic(fmt.Sprintf("raft %s: persisting log truncation: %v", rn.id, err))
		}
	}
	rn.log = rn.log[:from]
}

// quorum returns the majority size for the full cluster (peers + self).
func (rn *RaftNode) quorum() int {
	return (len(rn.peers)+1)/2 + 1
}

// Status is a read-only snapshot of a node's externally observable state,
// for tests, logging, and (in Phase 7) the dashboard.
type Status struct {
	ID           string
	State        State
	CurrentTerm  uint64
	VotedFor     string
	LeaderID     string
	CommitIndex  uint64
	LastApplied  uint64
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
		LeaderID:     rn.leaderID,
		CommitIndex:  rn.commitIndex,
		LastApplied:  rn.lastApplied,
		LastLogIndex: rn.lastLogIndex(),
		LastLogTerm:  rn.lastLogTerm(),
	}
}
