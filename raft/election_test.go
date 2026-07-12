package raft

import (
	"sync/atomic"
	"testing"
	"time"
)

// fastConfig returns timeouts sized for tests: elections resolve in tens of
// milliseconds instead of hundreds.
func fastConfig() Config {
	return Config{
		ElectionTimeoutMin: 50 * time.Millisecond,
		ElectionTimeoutMax: 100 * time.Millisecond,
		HeartbeatInterval:  15 * time.Millisecond,
		RPCTimeout:         25 * time.Millisecond,
	}
}

func TestCandidateWinsElectionAndHeartbeats(t *testing.T) {
	var heartbeats atomic.Int64
	cfg := fastConfig()
	cfg.ID = "node1"
	cfg.Peers = []string{"node2", "node3"}
	cfg.Transport = &fakeTransport{
		requestVote: func(_ string, args RequestVoteArgs) (RequestVoteReply, error) {
			return RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
		},
		appendEntries: func(_ string, args AppendEntriesArgs) (AppendEntriesReply, error) {
			heartbeats.Add(1)
			return AppendEntriesReply{Term: args.Term, Success: true}, nil
		},
	}
	rn := newTestNode(t, cfg)
	if err := rn.Start(); err != nil {
		t.Fatal(err)
	}
	defer rn.Stop()

	waitFor(t, 2*time.Second, "node to win election", func() bool {
		return rn.Status().State == Leader
	})
	s := rn.Status()
	if s.CurrentTerm == 0 {
		t.Error("won an election without ever incrementing the term")
	}
	if s.VotedFor != "node1" {
		t.Errorf("votedFor = %q, want self-vote node1", s.VotedFor)
	}
	if s.LeaderID != "node1" {
		t.Errorf("leaderID = %q, want node1", s.LeaderID)
	}

	// Figure 2: nextIndex reinitialized to last log index + 1, matchIndex to 0.
	rn.mu.Lock()
	for _, peer := range rn.peers {
		if rn.nextIndex[peer] != rn.lastLogIndex()+1 {
			t.Errorf("nextIndex[%s] = %d, want %d", peer, rn.nextIndex[peer], rn.lastLogIndex()+1)
		}
		if rn.matchIndex[peer] != 0 {
			t.Errorf("matchIndex[%s] = %d, want 0", peer, rn.matchIndex[peer])
		}
	}
	rn.mu.Unlock()

	// Heartbeats must keep flowing to suppress new elections.
	before := heartbeats.Load()
	waitFor(t, 2*time.Second, "recurring heartbeats", func() bool {
		return heartbeats.Load() >= before+4 // ≥2 more rounds × 2 peers
	})
}

func TestSingleNodeClusterElectsItself(t *testing.T) {
	cfg := fastConfig()
	cfg.ID = "node1" // no peers
	cfg.Transport = &fakeTransport{}
	rn := newTestNode(t, cfg)
	if err := rn.Start(); err != nil {
		t.Fatal(err)
	}
	defer rn.Stop()

	waitFor(t, 2*time.Second, "single node to elect itself", func() bool {
		return rn.Status().State == Leader
	})
}

func TestCandidateStepsDownOnHigherTermVoteReply(t *testing.T) {
	cfg := fastConfig()
	cfg.ID = "node1"
	cfg.Peers = []string{"node2", "node3"}
	cfg.Transport = &fakeTransport{
		requestVote: func(_ string, args RequestVoteArgs) (RequestVoteReply, error) {
			// Peers are far ahead of us.
			return RequestVoteReply{Term: args.Term + 5, VoteGranted: false}, nil
		},
	}
	rn := newTestNode(t, cfg)
	if err := rn.Start(); err != nil {
		t.Fatal(err)
	}
	defer rn.Stop()

	waitFor(t, 2*time.Second, "candidate to adopt the higher term", func() bool {
		s := rn.Status()
		return s.CurrentTerm >= 6 && s.State != Leader
	})
}

func TestLeaderStepsDownOnHigherTermHeartbeatReply(t *testing.T) {
	var deposed atomic.Bool
	cfg := fastConfig()
	cfg.ID = "node1"
	cfg.Peers = []string{"node2", "node3"}
	cfg.Transport = &fakeTransport{
		requestVote: func(_ string, args RequestVoteArgs) (RequestVoteReply, error) {
			return RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
		},
		appendEntries: func(_ string, args AppendEntriesArgs) (AppendEntriesReply, error) {
			if deposed.Load() {
				// The cluster has moved on to a higher term without us.
				return AppendEntriesReply{Term: args.Term + 10, Success: false}, nil
			}
			return AppendEntriesReply{Term: args.Term, Success: true}, nil
		},
	}
	rn := newTestNode(t, cfg)
	if err := rn.Start(); err != nil {
		t.Fatal(err)
	}
	defer rn.Stop()

	waitFor(t, 2*time.Second, "node to become leader", func() bool {
		return rn.Status().State == Leader
	})
	termAsLeader := rn.Status().CurrentTerm
	deposed.Store(true)

	waitFor(t, 2*time.Second, "leader to step down", func() bool {
		s := rn.Status()
		return s.State == Follower && s.CurrentTerm > termAsLeader
	})
}

// Split-vote behavior from one node's perspective: if votes never arrive,
// the node must keep retrying with fresh terms (no deadlock, no wedge) and
// must never promote itself.
func TestElectionRetriesWithNewTermsWhenVotesDenied(t *testing.T) {
	cfg := fastConfig()
	cfg.ID = "node1"
	cfg.Peers = []string{"node2", "node3"}
	cfg.Transport = &fakeTransport{
		requestVote: func(_ string, args RequestVoteArgs) (RequestVoteReply, error) {
			return RequestVoteReply{Term: args.Term, VoteGranted: false}, nil
		},
	}
	rn := newTestNode(t, cfg)
	if err := rn.Start(); err != nil {
		t.Fatal(err)
	}
	defer rn.Stop()

	waitFor(t, 5*time.Second, "at least 3 election rounds", func() bool {
		return rn.Status().CurrentTerm >= 3
	})
	if s := rn.Status(); s.State == Leader {
		t.Fatal("became leader without a quorum of votes")
	}
}

// Term confusion guard: votes granted for an old election must not be able
// to elect the node after it has already moved to a newer term.
func TestStaleVoteRepliesAreIgnored(t *testing.T) {
	release := make(chan struct{})
	cfg := fastConfig()
	cfg.ID = "node1"
	cfg.Peers = []string{"node2", "node3"}
	cfg.Transport = &fakeTransport{
		requestVote: func(_ string, args RequestVoteArgs) (RequestVoteReply, error) {
			<-release // hold every vote reply until the test says so
			return RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
		},
	}
	rn := newTestNode(t, cfg)

	// Become a candidate for term 1; the two vote RPCs are now in flight,
	// blocked inside the fake transport.
	rn.mu.Lock()
	rn.startElectionLocked()
	term := rn.currentTerm
	rn.mu.Unlock()

	// Before any vote reply lands, the node learns of a much higher term.
	rn.HandleRequestVote(RequestVoteArgs{Term: term + 5, CandidateID: "node9"})

	// Now deliver the term-1 vote grants. They reference a dead election:
	// the state/term guard must discard them.
	close(release)

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if s := rn.Status(); s.State == Leader {
			t.Fatal("stale vote replies elected a node in a newer term")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if s := rn.Status(); s.CurrentTerm != term+5 {
		t.Errorf("term = %d, want %d", s.CurrentTerm, term+5)
	}
}
