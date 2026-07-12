package raft

import "testing"

// --- RequestVote ---

func TestRequestVoteGrantsFirstCandidate(t *testing.T) {
	store := &memStore{}
	rn := newTestNode(t, Config{ID: "node1", Peers: []string{"node2", "node3"}, Store: store})

	reply := rn.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node2"})
	if !reply.VoteGranted {
		t.Fatal("vote not granted to first candidate of a new term")
	}
	if reply.Term != 1 {
		t.Errorf("reply.Term = %d, want 1 (adopted candidate's term)", reply.Term)
	}
	// The grant must be durable by the time the reply exists.
	if hs := store.saved(); hs.CurrentTerm != 1 || hs.VotedFor != "node2" {
		t.Errorf("persisted state = %+v, want {1 node2}", hs)
	}
}

func TestRequestVoteOneVotePerTerm(t *testing.T) {
	rn := newTestNode(t, Config{ID: "node1", Peers: []string{"node2", "node3"}})

	if r := rn.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node2"}); !r.VoteGranted {
		t.Fatal("first vote not granted")
	}
	// A competing candidate in the same term must be denied…
	if r := rn.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node3"}); r.VoteGranted {
		t.Fatal("granted a second vote in the same term — this is how two leaders happen")
	}
	// …but a retry from the candidate we already voted for is granted
	// (idempotent, in case our first reply was lost).
	if r := rn.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node2"}); !r.VoteGranted {
		t.Fatal("re-request from the same candidate was denied")
	}
}

func TestRequestVoteRejectsStaleTerm(t *testing.T) {
	rn := newTestNode(t, Config{ID: "node1"})
	rn.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node2"}) // brings us to term 5

	reply := rn.HandleRequestVote(RequestVoteArgs{Term: 3, CandidateID: "node3"})
	if reply.VoteGranted {
		t.Fatal("granted vote to a candidate from a stale term")
	}
	if reply.Term != 5 {
		t.Errorf("reply.Term = %d, want 5 so the stale candidate steps down", reply.Term)
	}
}

func TestRequestVoteHigherTermResetsVote(t *testing.T) {
	rn := newTestNode(t, Config{ID: "node1"})
	rn.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node2"})

	// A new term is a new election: the term-1 vote for node2 must not
	// block a term-2 vote for node3.
	reply := rn.HandleRequestVote(RequestVoteArgs{Term: 2, CandidateID: "node3"})
	if !reply.VoteGranted {
		t.Fatal("vote in a higher term denied because of a previous term's vote")
	}
	if s := rn.Status(); s.CurrentTerm != 2 || s.VotedFor != "node3" {
		t.Errorf("term/votedFor = %d/%q, want 2/node3", s.CurrentTerm, s.VotedFor)
	}
}

func TestRequestVoteDeniedDoesNotResetElectionTimer(t *testing.T) {
	rn := newTestNode(t, Config{ID: "node1"})
	rn.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node2"})

	rn.mu.Lock()
	before := rn.electionResetAt
	rn.mu.Unlock()

	// Denied vote (same term, different candidate): our own countdown to
	// candidacy must keep running — a doomed candidate must not be able to
	// suppress elections.
	rn.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node3"})

	rn.mu.Lock()
	after := rn.electionResetAt
	rn.mu.Unlock()
	if !after.Equal(before) {
		t.Fatal("denied RequestVote reset the election timer")
	}
}

// --- AppendEntries ---

func TestAppendEntriesRejectsStaleLeader(t *testing.T) {
	rn := newTestNode(t, Config{ID: "node1"})
	rn.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node2"})

	reply := rn.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: "node3"})
	if reply.Success {
		t.Fatal("accepted heartbeat from a deposed leader")
	}
	if reply.Term != 5 {
		t.Errorf("reply.Term = %d, want 5 so the stale leader steps down", reply.Term)
	}
	if s := rn.Status(); s.LeaderID == "node3" {
		t.Error("recorded a stale leader as current")
	}
}

func TestAppendEntriesAcceptsCurrentLeader(t *testing.T) {
	rn := newTestNode(t, Config{ID: "node1"})

	reply := rn.HandleAppendEntries(AppendEntriesArgs{Term: 1, LeaderID: "node2"})
	if !reply.Success {
		t.Fatal("rejected valid heartbeat")
	}
	s := rn.Status()
	if s.CurrentTerm != 1 {
		t.Errorf("term = %d, want 1", s.CurrentTerm)
	}
	if s.LeaderID != "node2" {
		t.Errorf("leaderID = %q, want node2", s.LeaderID)
	}
}

// The subtle one: a candidate that discovers a leader for its own term must
// step down WITHOUT clearing its vote — it voted for itself this term, and
// erasing that would let it vote a second time in the same term.
func TestCandidateStepsDownSameTermKeepsVote(t *testing.T) {
	rn := newTestNode(t, Config{ID: "node1", Peers: []string{"node2", "node3"}})

	// Make node1 a candidate at term 1 (votedFor = itself).
	rn.mu.Lock()
	rn.cfg.Transport = &fakeTransport{
		requestVote: func(string, RequestVoteArgs) (RequestVoteReply, error) {
			return RequestVoteReply{Term: 1, VoteGranted: false}, nil
		},
	}
	rn.startElectionLocked()
	rn.mu.Unlock()

	if s := rn.Status(); s.State != Candidate || s.VotedFor != "node1" {
		t.Fatalf("setup: state/votedFor = %v/%q, want Candidate/node1", s.State, s.VotedFor)
	}

	// node2 won term 1 and heartbeats us.
	reply := rn.HandleAppendEntries(AppendEntriesArgs{Term: 1, LeaderID: "node2"})
	if !reply.Success {
		t.Fatal("candidate rejected same-term leader")
	}
	s := rn.Status()
	if s.State != Follower {
		t.Errorf("state = %v, want Follower", s.State)
	}
	if s.VotedFor != "node1" {
		t.Errorf("votedFor = %q, want node1 (same-term step-down must preserve the vote)", s.VotedFor)
	}
	// And now a term-1 candidate asking for our vote must still be denied.
	if r := rn.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node3"}); r.VoteGranted {
		t.Fatal("voted twice in one term after same-term step-down")
	}
}

// Persistence ordering: by the time any reply that grants/adopts state
// exists, the store must already have been written. We can't observe "before
// the reply left" directly, but every mutation path must have saved by
// handler return.
func TestHardStatePersistedByHandlerReturn(t *testing.T) {
	store := &memStore{}
	rn := newTestNode(t, Config{ID: "node1", Store: store})

	rn.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node2"})
	if store.saveCount() == 0 {
		t.Fatal("granting a vote did not persist")
	}
	n := store.saveCount()

	rn.HandleAppendEntries(AppendEntriesArgs{Term: 2, LeaderID: "node3"})
	if store.saveCount() <= n {
		t.Fatal("adopting a higher term via AppendEntries did not persist")
	}
	if hs := store.saved(); hs.CurrentTerm != 2 || hs.VotedFor != None {
		t.Errorf("persisted state = %+v, want {2 %q}", hs, None)
	}
}
