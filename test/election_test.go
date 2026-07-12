package test

import (
	"testing"
	"time"

	"github.com/Abdullah-A-Qazi/RaftDB/raft"
)

// Scenario 1: a healthy 5-node cluster elects exactly one leader, everyone
// agrees on it, and it stays stable (no spurious re-elections while
// heartbeats flow).
func TestHealthyClusterElectsSingleLeader(t *testing.T) {
	c := NewCluster(t, 5)

	leader := c.WaitForAgreement(5 * time.Second)
	term := c.Status(leader).CurrentTerm

	// Stability check: several election timeouts pass with no change. A
	// missing heartbeat or timer-reset bug shows up here as a term bump.
	time.Sleep(4 * electionMax)
	if got, ok := c.agreement(); !ok || got != leader {
		t.Fatalf("leadership changed in a healthy cluster: had %s, now %v", leader, c.leaders())
	}
	if newTerm := c.Status(leader).CurrentTerm; newTerm != term {
		t.Fatalf("term advanced from %d to %d in a healthy cluster", term, newTerm)
	}
}

// Scenario 2: killing the leader triggers a new election that completes
// within bounded time, at a higher term.
func TestLeaderCrashTriggersNewElection(t *testing.T) {
	c := NewCluster(t, 5)

	leader1 := c.WaitForAgreement(5 * time.Second)
	term1 := c.Status(leader1).CurrentTerm
	c.Stop(leader1)

	// Bound: detection takes at most electionMax, then one or two election
	// rounds; 3s is many multiples of that.
	leader2 := c.WaitForLeader(3 * time.Second)
	if leader2 == leader1 {
		t.Fatalf("dead node %s still counted as leader", leader1)
	}
	term2 := c.Status(leader2).CurrentTerm
	if term2 <= term1 {
		t.Fatalf("new leader's term %d not above old term %d", term2, term1)
	}
}

// Scenario 3: simultaneous candidates must not deadlock the cluster. We
// force the worst case — every node disconnected until all four followers
// are candidates fighting over votes — then reconnect everyone at once and
// require convergence to a single leader within a few election rounds.
func TestSplitVoteResolves(t *testing.T) {
	c := NewCluster(t, 5)
	c.WaitForAgreement(5 * time.Second)

	for _, id := range c.ids {
		c.Disconnect(id)
	}
	// All four non-leaders keep timing out and re-candidating in isolation.
	// (The old leader stays Leader: with no reachable peers nothing can
	// depose it — Raft leaders have no self-demotion timeout in the paper.)
	waitFor(t, 5*time.Second, "at least 3 concurrent candidates", func() bool {
		candidates := 0
		for _, id := range c.ids {
			if c.Status(id).State == raft.Candidate {
				candidates++
			}
		}
		return candidates >= 3
	})

	for _, id := range c.ids {
		c.Reconnect(id)
	}
	// Randomized timeouts must now break the tie: someone wins, everyone
	// (including the stale ex-leader) falls in line behind one term.
	c.WaitForAgreement(10 * time.Second)
}

// Scenario 4: a node that was down through an election must come back as a
// follower of the new leader, at the new term.
func TestRejoinedNodeFollowsNewLeader(t *testing.T) {
	c := NewCluster(t, 5)

	leader1 := c.WaitForAgreement(5 * time.Second)
	c.Stop(leader1)
	leader2 := c.WaitForLeader(3 * time.Second)

	c.Restart(leader1)
	waitFor(t, 5*time.Second, "rejoined node to follow the new leader", func() bool {
		s := c.Status(leader1)
		l := c.Status(leader2)
		return s.State == raft.Follower &&
			l.State == raft.Leader &&
			s.CurrentTerm == l.CurrentTerm &&
			s.LeaderID == leader2
	})
}
