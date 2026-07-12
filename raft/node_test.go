package raft

import "testing"

func TestNewNodeInitialState(t *testing.T) {
	rn := newTestNode(t, Config{ID: "node1", Peers: []string{"node2", "node3"}})
	s := rn.Status()

	if s.ID != "node1" {
		t.Errorf("ID = %q, want node1", s.ID)
	}
	if s.State != Follower {
		t.Errorf("initial state = %v, want Follower", s.State)
	}
	if s.CurrentTerm != 0 {
		t.Errorf("initial term = %d, want 0", s.CurrentTerm)
	}
	if s.VotedFor != None {
		t.Errorf("initial votedFor = %q, want None", s.VotedFor)
	}
	if s.LeaderID != None {
		t.Errorf("initial leaderID = %q, want None", s.LeaderID)
	}
	if s.CommitIndex != 0 || s.LastApplied != 0 {
		t.Errorf("commitIndex/lastApplied = %d/%d, want 0/0", s.CommitIndex, s.LastApplied)
	}
}

// A node restarted with prior durable state must resume from its persisted
// term and vote, not from zero — resuming from zero is what would let it
// vote twice in one term.
func TestNewNodeRecoversHardState(t *testing.T) {
	store := &memStore{}
	if err := store.SaveHardState(HardState{CurrentTerm: 7, VotedFor: "node3"}); err != nil {
		t.Fatal(err)
	}
	rn := newTestNode(t, Config{ID: "node1", Store: store})
	s := rn.Status()
	if s.CurrentTerm != 7 || s.VotedFor != "node3" {
		t.Fatalf("recovered term/votedFor = %d/%q, want 7/node3", s.CurrentTerm, s.VotedFor)
	}
	if s.State != Follower {
		t.Errorf("recovered state = %v, want Follower (state is volatile)", s.State)
	}
}

func TestNewNodeConfigValidation(t *testing.T) {
	if _, err := NewNode(Config{Store: &memStore{}}); err == nil {
		t.Error("NewNode accepted empty ID")
	}
	if _, err := NewNode(Config{ID: "node1"}); err == nil {
		t.Error("NewNode accepted nil Store")
	}
	if _, err := NewNode(Config{
		ID: "node1", Store: &memStore{},
		ElectionTimeoutMin: 100, ElectionTimeoutMax: 50,
	}); err == nil {
		t.Error("NewNode accepted Min > Max election timeout")
	}
	if _, err := NewNode(Config{
		ID: "node1", Store: &memStore{},
		ElectionTimeoutMin: 100, ElectionTimeoutMax: 200, HeartbeatInterval: 100,
	}); err == nil {
		t.Error("NewNode accepted heartbeat >= election timeout")
	}
}

// The sentinel entry at slot 0 must make an empty log report index 0,
// term 0 — those are the values a brand-new candidate puts in
// RequestVote's last_log_index/last_log_term.
func TestEmptyLogSentinel(t *testing.T) {
	rn := newTestNode(t, Config{})

	if len(rn.log) != 1 {
		t.Fatalf("new log has %d slots, want 1 (sentinel only)", len(rn.log))
	}
	if got := rn.lastLogIndex(); got != 0 {
		t.Errorf("lastLogIndex of empty log = %d, want 0", got)
	}
	if got := rn.lastLogTerm(); got != 0 {
		t.Errorf("lastLogTerm of empty log = %d, want 0", got)
	}
}

func TestLastLogHelpersTrackAppends(t *testing.T) {
	rn := newTestNode(t, Config{})
	rn.log = append(rn.log,
		LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		LogEntry{Term: 1, Index: 2, Command: []byte("b")},
		LogEntry{Term: 3, Index: 3, Command: []byte("c")},
	)

	if got := rn.lastLogIndex(); got != 3 {
		t.Errorf("lastLogIndex = %d, want 3", got)
	}
	if got := rn.lastLogTerm(); got != 3 {
		t.Errorf("lastLogTerm = %d, want 3", got)
	}
}

func TestQuorum(t *testing.T) {
	cases := []struct {
		peers int
		want  int
	}{
		{0, 1}, // single-node cluster
		{2, 2}, // 3 nodes
		{4, 3}, // 5 nodes
		{3, 3}, // 4 nodes: majority is still 3
	}
	for _, tc := range cases {
		rn := newTestNode(t, Config{Peers: make([]string, tc.peers)})
		if got := rn.quorum(); got != tc.want {
			t.Errorf("quorum with %d peers = %d, want %d", tc.peers, got, tc.want)
		}
	}
}

func TestStateString(t *testing.T) {
	cases := map[State]string{
		Follower:  "Follower",
		Candidate: "Candidate",
		Leader:    "Leader",
		State(42): "State(42)",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("State(%d).String() = %q, want %q", int(s), got, want)
		}
	}
}
