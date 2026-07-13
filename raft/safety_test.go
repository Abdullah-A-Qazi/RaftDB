package raft

// Phase 3 safety tests: the election restriction (§5.4.1), the current-term
// commit rule (§5.4.2), and a step-by-step replay of the paper's Figure 8 —
// the canonical interleaving where getting the commit rule wrong loses an
// acknowledged write. The prose walkthrough lives in
// docs/phase-3-safety-rules.md; the subtest names here mirror its sections.

import (
	"errors"
	"testing"
)

// deadTransport fails every RPC: manufactured leaders in these tests must
// not make progress on their own — every replication step is driven by
// hand so each assertion pins down one exact cluster state.
func deadTransport() *fakeTransport {
	return &fakeTransport{
		requestVote: func(string, RequestVoteArgs) (RequestVoteReply, error) {
			return RequestVoteReply{}, errors.New("unreachable")
		},
		appendEntries: func(string, AppendEntriesArgs) (AppendEntriesReply, error) {
			return AppendEntriesReply{}, errors.New("unreachable")
		},
	}
}

// newSafetyNode builds a node that never acts on its own (timers far beyond
// test duration, dead transport).
func newSafetyNode(t *testing.T, id string, peers []string) *RaftNode {
	t.Helper()
	cfg := slowFollowerConfig()
	cfg.ID = id
	cfg.Peers = peers
	cfg.Transport = deadTransport()
	rn := newTestNode(t, cfg)
	t.Cleanup(rn.Stop)
	return rn
}

// makeLeader manufactures leadership at the given term without running an
// election (the election paths have their own tests).
func makeLeader(rn *RaftNode, term uint64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.currentTerm = term
	rn.votedFor = rn.id
	rn.becomeLeaderLocked()
}

// ackFrom simulates a follower acknowledging entries: the follower matched
// everything the leader's request carried.
func ackFrom(rn *RaftNode, peer string, term uint64, prevIndex uint64, entries []LogEntry) {
	rn.handleAppendEntriesReply(peer, term,
		AppendEntriesArgs{PrevLogIndex: prevIndex, Entries: entries},
		AppendEntriesReply{Term: term, Success: true})
}

// --- Election restriction (§5.4.1) ---

func TestElectionRestrictionMatrix(t *testing.T) {
	// Voter's log ends at (index 2, term 2).
	cases := []struct {
		name        string
		lastTerm    uint64
		lastIndex   uint64
		wantGranted bool
		because     string
	}{
		{"higher last term, shorter log", 3, 1, true,
			"terms dominate: a higher last term means a later, more complete history"},
		{"equal term, equal index", 2, 2, true, "identical logs are up-to-date"},
		{"equal term, longer log", 2, 3, true, "same term, more entries"},
		{"equal term, shorter log", 2, 1, false, "missing the tail of our log"},
		{"lower last term, much longer log", 1, 100, false,
			"length never beats term: those 100 entries are uncommittable junk"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			voter := newSafetyNode(t, "node1", []string{"node2", "node3"})
			feed(t, voter, 1, 0, 0, []LogEntry{{Term: 1, Index: 1}}, 0)
			feed(t, voter, 2, 1, 1, []LogEntry{{Term: 2, Index: 2}}, 0)

			reply := voter.HandleRequestVote(RequestVoteArgs{
				Term: 10, CandidateID: "node2",
				LastLogIndex: tc.lastIndex, LastLogTerm: tc.lastTerm,
			})
			if reply.VoteGranted != tc.wantGranted {
				t.Fatalf("granted=%v, want %v (%s)", reply.VoteGranted, tc.wantGranted, tc.because)
			}
		})
	}
}

// A denial for staleness must not burn the voter's one vote for the term —
// a viable candidate asking later in the same term must still get it.
func TestDeniedVoteIsNotBurned(t *testing.T) {
	voter := newSafetyNode(t, "node1", []string{"node2", "node3"})
	feed(t, voter, 2, 0, 0, []LogEntry{{Term: 2, Index: 1}}, 0)

	// Stale candidate: denied.
	if r := voter.HandleRequestVote(RequestVoteArgs{
		Term: 5, CandidateID: "node2", LastLogIndex: 0, LastLogTerm: 0,
	}); r.VoteGranted {
		t.Fatal("granted vote to a candidate with a stale log")
	}
	// But the vote itself is still available in term 5.
	if r := voter.HandleRequestVote(RequestVoteArgs{
		Term: 5, CandidateID: "node3", LastLogIndex: 1, LastLogTerm: 2,
	}); !r.VoteGranted {
		t.Fatal("denial of a stale candidate burned the vote for the whole term")
	}
}

// --- Current-term commit rule (§5.4.2), compact form ---

func TestNoDirectCommitOfOldTermEntry(t *testing.T) {
	// Leader of term 4 whose log tail is an inherited term-2 entry.
	rn := newSafetyNode(t, "node1", []string{"node2", "node3", "node4", "node5"})
	feed(t, rn, 1, 0, 0, []LogEntry{{Term: 1, Index: 1, Command: []byte("a")}}, 0)
	feed(t, rn, 2, 1, 1, []LogEntry{{Term: 2, Index: 2, Command: []byte("b")}}, 0)
	makeLeader(rn, 4)

	// A majority acks everything through index 2 — the exact situation
	// where the naive rule commits and Figure 8 then loses the entry.
	old := rn.entriesFrom(1)
	ackFrom(rn, "node2", 4, 0, old)
	ackFrom(rn, "node3", 4, 0, old)
	if got := rn.Status().CommitIndex; got != 0 {
		t.Fatalf("commitIndex = %d after majority-acked OLD-term entries, want 0", got)
	}

	// The leader's own term-4 entry reaching the same majority commits
	// everything below it in one step.
	index, _, err := rn.Propose([]byte("c"))
	if err != nil {
		t.Fatal(err)
	}
	fresh := rn.entriesFrom(index)
	ackFrom(rn, "node2", 4, index-1, fresh)
	ackFrom(rn, "node3", 4, index-1, fresh)
	if got := rn.Status().CommitIndex; got != index {
		t.Fatalf("commitIndex = %d, want %d (old entries commit indirectly under a current-term entry)", got, index)
	}
}

// --- Figure 8, the full replay ---
//
// Cast (matching the paper's S1..S5): five nodes; entry (idx 1, term 1) is
// on everyone; S1 as leader of term 2 replicated (idx 2, term 2) only to
// S2 before losing leadership; S5 as leader of term 3 wrote (idx 2, term 3)
// only to itself. Now S1 leads term 4 and re-replicates its (idx 2, term 2)
// to S3 — putting an OLD-term entry on a majority {S1,S2,S3}.
type figure8 struct {
	s1, s2, s3, s4, s5 *RaftNode
}

func buildFigure8(t *testing.T) figure8 {
	t.Helper()
	ids := []string{"node1", "node2", "node3", "node4", "node5"}
	peersOf := func(self string) []string {
		var out []string
		for _, id := range ids {
			if id != self {
				out = append(out, id)
			}
		}
		return out
	}
	f := figure8{
		s1: newSafetyNode(t, "node1", peersOf("node1")),
		s2: newSafetyNode(t, "node2", peersOf("node2")),
		s3: newSafetyNode(t, "node3", peersOf("node3")),
		s4: newSafetyNode(t, "node4", peersOf("node4")),
		s5: newSafetyNode(t, "node5", peersOf("node5")),
	}

	// Term 1: (idx 1, term 1) replicated everywhere.
	e1 := []LogEntry{{Term: 1, Index: 1, Command: []byte("e1")}}
	for _, n := range []*RaftNode{f.s1, f.s2, f.s3, f.s4, f.s5} {
		feed(t, n, 1, 0, 0, e1, 0)
	}

	// (a) Term 2: leader S1 appends (idx 2, term 2); it reaches only S2.
	e2old := []LogEntry{{Term: 2, Index: 2, Command: []byte("from-term-2")}}
	feed(t, f.s1, 2, 1, 1, e2old, 0)
	feed(t, f.s2, 2, 1, 1, e2old, 0)

	// (b) Term 3: S5 wins with votes from S3 and S4 — verify the election
	// restriction ALLOWS this (their logs end at (1,1), S5's too).
	for _, voter := range []*RaftNode{f.s3, f.s4} {
		r := voter.HandleRequestVote(RequestVoteArgs{
			Term: 3, CandidateID: "node5", LastLogIndex: 1, LastLogTerm: 1,
		})
		if !r.VoteGranted {
			t.Fatalf("setup: %s denied S5's legitimate term-3 candidacy", voter.ID())
		}
	}
	makeLeader(f.s5, 3)
	if _, _, err := f.s5.Propose([]byte("from-term-3")); err != nil {
		t.Fatalf("setup: S5 propose: %v", err)
	} // (idx 2, term 3), on S5 only

	// (c) Term 4: S1 wins (S2: identical logs; S3: S1's last term 2 > 1)
	// and re-replicates its old (idx 2, term 2) to S3.
	for _, voter := range []*RaftNode{f.s2, f.s3} {
		r := voter.HandleRequestVote(RequestVoteArgs{
			Term: 4, CandidateID: "node1", LastLogIndex: 2, LastLogTerm: 2,
		})
		if !r.VoteGranted {
			t.Fatalf("setup: %s denied S1's legitimate term-4 candidacy", voter.ID())
		}
	}
	makeLeader(f.s1, 4)
	feed(t, f.s3, 4, 1, 1, e2old, 0) // S3 now holds (idx 2, term 2)
	ackFrom(f.s1, "node2", 4, 1, e2old)
	ackFrom(f.s1, "node3", 4, 1, e2old)

	// The moment the whole phase is about: (idx 2, term 2) is now on a
	// majority {S1,S2,S3}, and the leader MUST NOT count that as committed.
	if got := f.s1.Status().CommitIndex; got != 0 {
		t.Fatalf("commitIndex = %d with only old-term entries replicated, want 0", got)
	}
	return f
}

// Branch (d): because nothing was committed, S5 overwriting everyone's
// (idx 2) is LEGAL — the rule's job is not to prevent this rewrite, it is
// to guarantee no client was told "committed" beforehand.
func TestFigure8OldEntryLegallyOverwritten(t *testing.T) {
	f := buildFigure8(t)

	// S5 (last entry (2, term 3)) campaigns for term 5. Voters S2 and S3
	// hold (2, term 2): 3 > 2, so they MUST grant — which is exactly why
	// committing (2, term 2) on majority-count alone would have been fatal.
	for _, voter := range []*RaftNode{f.s2, f.s3, f.s4} {
		r := voter.HandleRequestVote(RequestVoteArgs{
			Term: 5, CandidateID: "node5", LastLogIndex: 2, LastLogTerm: 3,
		})
		if !r.VoteGranted {
			t.Fatalf("%s denied S5's term-5 candidacy — it should be electable, nothing is committed", voter.ID())
		}
	}

	// S5, leader of term 5, replaces S2's (idx 2, term 2) with its own.
	// This truncation of an uncommitted entry must go through cleanly (the
	// committed-entry truncation panic must NOT fire).
	e2new := []LogEntry{{Term: 3, Index: 2, Command: []byte("from-term-3")}}
	if r := feed(t, f.s2, 5, 1, 1, e2new, 0); !r.Success {
		t.Fatal("S2 rejected the new leader's overwrite of an uncommitted entry")
	}
	if s := f.s2.Status(); s.LastLogIndex != 2 || s.LastLogTerm != 3 {
		t.Fatalf("S2 last = (%d, term %d), want (2, term 3)", s.LastLogIndex, s.LastLogTerm)
	}
}

// Branch (e): S1 replicates one entry of ITS OWN term on a majority; that
// commits everything below it, and from then on S5 can never win again —
// the committed prefix is sealed.
func TestFigure8CurrentTermEntrySealsThePrefix(t *testing.T) {
	f := buildFigure8(t)

	index, _, err := f.s1.Propose([]byte("from-term-4")) // (idx 3, term 4)
	if err != nil {
		t.Fatal(err)
	}
	e3 := f.s1.entriesFrom(index)
	feed(t, f.s2, 4, 2, 2, e3, 0)
	feed(t, f.s3, 4, 2, 2, e3, 0)
	ackFrom(f.s1, "node2", 4, 2, e3)
	ackFrom(f.s1, "node3", 4, 2, e3)

	// Committing (3, term 4) drags (1, term 1) and (2, term 2) in with it.
	if got := f.s1.Status().CommitIndex; got != 3 {
		t.Fatalf("commitIndex = %d, want 3 (current-term entry commits the whole prefix)", got)
	}

	// S5 tries term 5 again — S2 and S3 now end at (3, term 4), and
	// 3 < 4 means DENIED. With only S4 (and itself), S5 is 2 votes short
	// of the 3 it needs: no elected leader can lack the committed entries.
	for _, voter := range []*RaftNode{f.s2, f.s3} {
		r := voter.HandleRequestVote(RequestVoteArgs{
			Term: 5, CandidateID: "node5", LastLogIndex: 2, LastLogTerm: 3,
		})
		if r.VoteGranted {
			t.Fatalf("%s granted S5 a vote AFTER the prefix was committed — Figure 8's lost write", voter.ID())
		}
	}
	if r := f.s4.HandleRequestVote(RequestVoteArgs{
		Term: 5, CandidateID: "node5", LastLogIndex: 2, LastLogTerm: 3,
	}); !r.VoteGranted {
		t.Fatal("S4 (log ends at term 1) should still grant — it can't tell, but it doesn't matter: quorum is unreachable")
	}
}
