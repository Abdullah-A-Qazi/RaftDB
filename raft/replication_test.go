package raft

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recorderSM records applied entries so tests can assert order and content.
type recorderSM struct {
	mu      sync.Mutex
	entries []LogEntry
}

func (r *recorderSM) Apply(e LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, e)
}

func (r *recorderSM) indexes() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]uint64, len(r.entries))
	for i, e := range r.entries {
		out[i] = e.Index
	}
	return out
}

// slowFollowerConfig keeps a node a follower for the whole test: the
// election timeout is far beyond test duration, so nothing fires unless the
// test drives it.
func slowFollowerConfig() Config {
	return Config{
		ElectionTimeoutMin: 10 * time.Second,
		ElectionTimeoutMax: 20 * time.Second,
		HeartbeatInterval:  time.Second,
		RPCTimeout:         50 * time.Millisecond,
	}
}

// --- Propose ---

func TestProposeOnFollowerRejected(t *testing.T) {
	rn := newTestNode(t, Config{ID: "node1", Peers: []string{"node2", "node3"}})
	if _, _, err := rn.Propose([]byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Propose on follower returned %v, want ErrNotLeader", err)
	}
}

func TestLeaderCommitsAfterMajorityAck(t *testing.T) {
	sm := &recorderSM{}
	cfg := fastConfig()
	cfg.ID = "node1"
	cfg.Peers = []string{"node2", "node3"}
	cfg.Transport = &fakeTransport{
		requestVote: func(_ string, args RequestVoteArgs) (RequestVoteReply, error) {
			return RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
		},
		// Followers accept everything.
		appendEntries: func(_ string, args AppendEntriesArgs) (AppendEntriesReply, error) {
			return AppendEntriesReply{Term: args.Term, Success: true}, nil
		},
	}
	rn := newTestNode(t, cfg)
	rn.SetStateMachine(sm)
	if err := rn.Start(); err != nil {
		t.Fatal(err)
	}
	defer rn.Stop()

	waitFor(t, 2*time.Second, "election", func() bool { return rn.Status().State == Leader })

	index, term, err := rn.Propose([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if index != 1 {
		t.Errorf("first proposal at index %d, want 1", index)
	}
	if term != rn.Status().CurrentTerm {
		t.Errorf("proposal term %d != current term %d", term, rn.Status().CurrentTerm)
	}

	waitFor(t, 2*time.Second, "commit and apply", func() bool {
		s := rn.Status()
		return s.CommitIndex >= index && s.LastApplied >= index
	})
	if got := sm.indexes(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("applied indexes = %v, want [1]", got)
	}
}

// A write must not commit while a majority is unreachable — and must commit
// as soon as enough followers come back.
func TestLeaderCommitsOnlyWithMajority(t *testing.T) {
	// 5-node cluster: quorum 3 = leader + 2 peers.
	var reachable atomic.Int32 // how many peers accept appends
	cfg := fastConfig()
	cfg.ID = "node1"
	cfg.Peers = []string{"node2", "node3", "node4", "node5"}
	peerRank := map[string]int32{"node2": 1, "node3": 2, "node4": 3, "node5": 4}
	cfg.Transport = &fakeTransport{
		requestVote: func(_ string, args RequestVoteArgs) (RequestVoteReply, error) {
			return RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
		},
		appendEntries: func(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
			if peerRank[peer] > reachable.Load() {
				return AppendEntriesReply{}, errors.New("unreachable")
			}
			return AppendEntriesReply{Term: args.Term, Success: true}, nil
		},
	}
	rn := newTestNode(t, cfg)
	if err := rn.Start(); err != nil {
		t.Fatal(err)
	}
	defer rn.Stop()
	waitFor(t, 2*time.Second, "election", func() bool { return rn.Status().State == Leader })

	index, _, err := rn.Propose([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}

	// Only 1 peer reachable: leader + 1 = 2 < 3. Must NOT commit.
	reachable.Store(1)
	time.Sleep(6 * cfg.HeartbeatInterval)
	if got := rn.Status().CommitIndex; got != 0 {
		t.Fatalf("committed at index %d with only 2/5 nodes — split-brain fuel", got)
	}

	// Second peer back: leader + 2 = 3 = quorum. Must commit.
	reachable.Store(2)
	waitFor(t, 2*time.Second, "commit once majority reachable", func() bool {
		return rn.Status().CommitIndex >= index
	})
}

// --- Follower append path (driving HandleAppendEntries directly) ---

// leaderFeed sends a well-formed AppendEntries stream from an imaginary
// leader to a follower node under test.
func feed(t *testing.T, rn *RaftNode, term uint64, prevIdx, prevTerm uint64, entries []LogEntry, commit uint64) AppendEntriesReply {
	t.Helper()
	return rn.HandleAppendEntries(AppendEntriesArgs{
		Term: term, LeaderID: "leaderX",
		PrevLogIndex: prevIdx, PrevLogTerm: prevTerm,
		Entries: entries, LeaderCommit: commit,
	})
}

func TestFollowerAppendsAndApplies(t *testing.T) {
	sm := &recorderSM{}
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	cfg.Peers = []string{"node2", "node3"}
	cfg.Transport = &fakeTransport{}
	rn := newTestNode(t, cfg)
	rn.SetStateMachine(sm)
	if err := rn.Start(); err != nil {
		t.Fatal(err)
	}
	defer rn.Stop()

	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	}
	if r := feed(t, rn, 1, 0, 0, entries, 0); !r.Success {
		t.Fatal("append rejected")
	}
	if s := rn.Status(); s.LastLogIndex != 2 || s.CommitIndex != 0 {
		t.Fatalf("lastLogIndex/commit = %d/%d, want 2/0 (nothing committed yet)", s.LastLogIndex, s.CommitIndex)
	}

	// Leader announces commit up to 2 via heartbeat.
	if r := feed(t, rn, 1, 2, 1, nil, 2); !r.Success {
		t.Fatal("heartbeat rejected")
	}
	waitFor(t, 2*time.Second, "entries applied", func() bool {
		return rn.Status().LastApplied == 2
	})
	got := sm.indexes()
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("applied order = %v, want [1 2]", got)
	}
}

// leaderCommit beyond what this request verified must not be trusted.
func TestFollowerCommitClampedToLastNewEntry(t *testing.T) {
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	rn := newTestNode(t, cfg)

	entries := []LogEntry{{Term: 1, Index: 1, Command: []byte("a")}}
	// Leader claims commitIndex 100; we only verified up to index 1.
	if r := feed(t, rn, 1, 0, 0, entries, 100); !r.Success {
		t.Fatal("append rejected")
	}
	if got := rn.Status().CommitIndex; got != 1 {
		t.Fatalf("commitIndex = %d, want 1 (min(leaderCommit, lastNew))", got)
	}
}

func TestFollowerRejectsGapWithHints(t *testing.T) {
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	rn := newTestNode(t, cfg)

	// Log is empty; leader thinks we have 5 entries.
	r := feed(t, rn, 1, 5, 1, nil, 0)
	if r.Success {
		t.Fatal("accepted entries with a gap before them")
	}
	if r.ConflictTerm != 0 || r.ConflictIndex != 1 {
		t.Fatalf("hints = (idx %d, term %d), want (1, 0): jump to our end", r.ConflictIndex, r.ConflictTerm)
	}
}

func TestFollowerRejectsWrongTermWithHints(t *testing.T) {
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	rn := newTestNode(t, cfg)

	// Seed: 3 entries of term 1 (from the term-1 leader).
	feed(t, rn, 1, 0, 0, []LogEntry{
		{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
	}, 0)

	// A term-3 leader probes at prev=(3, term 2): we have term 1 there.
	r := feed(t, rn, 3, 3, 2, nil, 0)
	if r.Success {
		t.Fatal("accepted despite term mismatch at prevLogIndex")
	}
	if r.ConflictTerm != 1 || r.ConflictIndex != 1 {
		t.Fatalf("hints = (idx %d, term %d), want (1, 1): whole run of term-1 entries", r.ConflictIndex, r.ConflictTerm)
	}
}

func TestFollowerTruncatesConflictingSuffix(t *testing.T) {
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	rn := newTestNode(t, cfg)

	// Term-1 leader replicated 3 entries (uncommitted).
	feed(t, rn, 1, 0, 0, []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("old2")},
		{Term: 1, Index: 3, Command: []byte("old3")},
	}, 0)

	// New term-2 leader agrees at index 1 but replaces 2 onward.
	r := feed(t, rn, 2, 1, 1, []LogEntry{
		{Term: 2, Index: 2, Command: []byte("new2")},
	}, 0)
	if !r.Success {
		t.Fatal("valid overwrite rejected")
	}
	s := rn.Status()
	if s.LastLogIndex != 2 || s.LastLogTerm != 2 {
		t.Fatalf("last = (idx %d, term %d), want (2, 2): old suffix gone", s.LastLogIndex, s.LastLogTerm)
	}
}

// THE truncation regression test: a duplicated/reordered AppendEntries
// covering an old prefix must not chop off newer entries — especially not
// committed ones.
func TestStaleDuplicateAppendDoesNotTruncate(t *testing.T) {
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	rn := newTestNode(t, cfg)

	// Leader replicated 3 entries and committed them.
	feed(t, rn, 1, 0, 0, []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 1, Index: 3, Command: []byte("c")},
	}, 3)

	// The network re-delivers the leader's *first* request (entries 1–2
	// only). Same term, same leader — a naive "truncate at prev+1 and
	// append" would delete committed entry 3 here.
	r := feed(t, rn, 1, 0, 0, []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	}, 2)
	if !r.Success {
		t.Fatal("duplicate append rejected")
	}
	s := rn.Status()
	if s.LastLogIndex != 3 {
		t.Fatalf("lastLogIndex = %d, want 3: stale duplicate truncated the log", s.LastLogIndex)
	}
	if s.CommitIndex != 3 {
		t.Fatalf("commitIndex = %d, want 3 (must never regress)", s.CommitIndex)
	}
}

// --- Leader-side bookkeeping ---

// Out-of-order replies: a late success for a short prefix must not drag
// matchIndex/nextIndex backwards.
func TestMatchIndexNeverRegresses(t *testing.T) {
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	cfg.Peers = []string{"node2", "node3"}
	cfg.Transport = &fakeTransport{}
	rn := newTestNode(t, cfg)

	// Manufacture leadership without timers.
	rn.mu.Lock()
	rn.currentTerm = 2
	rn.log = append(rn.log,
		LogEntry{Term: 2, Index: 1}, LogEntry{Term: 2, Index: 2}, LogEntry{Term: 2, Index: 3})
	rn.becomeLeaderLocked()
	term := rn.currentTerm
	rn.mu.Unlock()
	defer rn.Stop()

	// Reply for a request that carried entries 1–3 arrives first…
	rn.handleAppendEntriesReply("node2", term,
		AppendEntriesArgs{PrevLogIndex: 0, Entries: []LogEntry{{Index: 1}, {Index: 2}, {Index: 3}}},
		AppendEntriesReply{Term: term, Success: true})
	// …then a late reply for an earlier request that carried only entry 1.
	rn.handleAppendEntriesReply("node2", term,
		AppendEntriesArgs{PrevLogIndex: 0, Entries: []LogEntry{{Index: 1}}},
		AppendEntriesReply{Term: term, Success: true})

	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.matchIndex["node2"] != 3 {
		t.Fatalf("matchIndex = %d, want 3: late short reply regressed it", rn.matchIndex["node2"])
	}
	if rn.nextIndex["node2"] != 4 {
		t.Fatalf("nextIndex = %d, want 4", rn.nextIndex["node2"])
	}
}

func TestNextIndexAfterRejection(t *testing.T) {
	// Leader log: sentinel, then terms [1 1 4 4 5].
	log := []LogEntry{
		{Term: 0, Index: 0},
		{Term: 1, Index: 1}, {Term: 1, Index: 2},
		{Term: 4, Index: 3}, {Term: 4, Index: 4},
		{Term: 5, Index: 5},
	}
	cases := []struct {
		name    string
		prev    uint64
		reply   AppendEntriesReply
		want    uint64
		because string
	}{
		{
			name:    "follower log too short",
			prev:    5,
			reply:   AppendEntriesReply{ConflictTerm: 0, ConflictIndex: 3},
			want:    3,
			because: "jump to follower's end",
		},
		{
			name:    "leader also has the conflict term",
			prev:    4,
			reply:   AppendEntriesReply{ConflictTerm: 1, ConflictIndex: 1},
			want:    3,
			because: "retry just past leader's last term-1 entry (index 2)",
		},
		{
			name:    "leader lacks the conflict term entirely",
			prev:    4,
			reply:   AppendEntriesReply{ConflictTerm: 3, ConflictIndex: 3},
			want:    3,
			because: "wipe the follower's whole term-3 run",
		},
		{
			name:    "no hints degrades to decrement",
			prev:    4,
			reply:   AppendEntriesReply{},
			want:    4,
			because: "paper's step-back-one fallback",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextIndexAfterRejection(log, tc.prev, tc.reply); got != tc.want {
				t.Fatalf("got %d, want %d (%s)", got, tc.want, tc.because)
			}
		})
	}
}
