package raft

import "testing"

func TestNewNodeInitialState(t *testing.T) {
	rn := NewNode("node1", []string{"node2", "node3"})
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
	if s.CommitIndex != 0 || s.LastApplied != 0 {
		t.Errorf("commitIndex/lastApplied = %d/%d, want 0/0", s.CommitIndex, s.LastApplied)
	}
}

// The sentinel entry at slot 0 must make an empty log report index 0,
// term 0 — those are the values a brand-new candidate puts in
// RequestVote's last_log_index/last_log_term.
func TestEmptyLogSentinel(t *testing.T) {
	rn := NewNode("node1", nil)

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
	rn := NewNode("node1", nil)
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
