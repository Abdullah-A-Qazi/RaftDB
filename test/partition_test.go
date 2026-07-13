package test

// Phase 6: network partition scenarios. Every test here severs links while
// all processes keep running — the failure mode that actually produces
// split brain in the wild (crashes are the easy case; a machine that is
// alive, convinced it is leader, and unreachable is the hard one).
// Results are written up in docs/partition-testing.md.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Abdullah-A-Qazi/RaftDB/raft"
	"github.com/Abdullah-A-Qazi/RaftDB/rpc/kvpb"
)

// splitAround returns (minority of `size` non-leader nodes, the rest).
func splitAround(c *Cluster, leader string, size int) (minority, majority []string) {
	for _, id := range c.ids {
		if id != leader && len(minority) < size {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}
	return minority, majority
}

// Scenario 1 — symmetric 3/2 partition, leader on the majority side.
// The two isolated followers must NEVER become leader (2 < quorum 3), and
// must never acknowledge a write; the majority must keep serving
// uninterrupted. On heal, the minority reconciles.
func TestSymmetricPartitionMinorityStarves(t *testing.T) {
	c := NewCluster(t, 5)
	leader := c.WaitForAgreement(5 * time.Second)
	minority, _ := splitAround(c, leader, 2)

	c.MustPut("pre", "partition")
	c.Partition(minority...)

	// Majority side: business as usual, and quickly — no waiting out
	// long timeouts, the leader never lost its quorum.
	for i := range 3 {
		c.MustPut(fmt.Sprintf("during%d", i), "majority")
	}

	// Minority side: watch it for ~6 election timeouts. The two followers
	// will time out, become candidates, inflate their terms — but with
	// only 2 of 5 votes reachable they must never win.
	deadline := time.Now().Add(6 * electionMax)
	sawCandidate := false
	for time.Now().Before(deadline) {
		for _, id := range minority {
			switch c.Status(id).State {
			case raft.Leader:
				t.Fatalf("%s became leader inside a 2-node minority — quorum arithmetic is broken", id)
			case raft.Candidate:
				sawCandidate = true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sawCandidate {
		t.Fatal("minority nodes never even tried an election — is the partition real?")
	}

	// A client asking a minority node must not get an ack: no leader on
	// that side, so the response is a redirect (to a leader it can't
	// name or can't reach), never success.
	resp, err := c.KV(minority[0]).Put(shortCtx(t, time.Second), &kvpb.PutRequest{Key: "ghost", Value: "w"})
	if err == nil && resp.Redirect == nil {
		t.Fatal("a minority node acknowledged a write during the partition")
	}

	c.Heal()
	// The rejoining candidates carry inflated terms and will depose the
	// leader once (no PreVote — flagged since Phase 1); the election
	// restriction then routes leadership to a full log and everyone
	// converges.
	c.WaitForAgreement(10 * time.Second)
	waitFor(t, 10*time.Second, "minority caught up with all majority writes", func() bool {
		for _, id := range minority {
			m := c.Store(id).Snapshot()
			if m["pre"] != "partition" || m["during0"] != "majority" || m["during2"] != "majority" {
				return false
			}
		}
		return true
	})
	if _, ok := c.Store(minority[0]).Get("ghost"); ok {
		t.Fatal("the unacknowledged minority write materialized after heal")
	}
}

// Scenario 2 — the partition isolates the current leader (with one
// follower, so it isn't trivially alone). The old leader keeps believing it
// leads at its stale term; writes sent to it must time out unacknowledged
// and must NOT survive the heal — this is the split-brain test.
func TestPartitionIsolatingLeaderLosesNoAckedWrites(t *testing.T) {
	c := NewCluster(t, 5)
	leader1 := c.WaitForAgreement(5 * time.Second)
	term1 := c.Status(leader1).CurrentTerm

	c.MustPut("stable", "before")

	// Cut off the leader plus one follower from the other three.
	minority := []string{leader1}
	var majority []string
	for _, id := range c.ids {
		if id == leader1 {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}
	c.Partition(minority...)

	// The stale leader accepts the proposal (it still thinks it leads)
	// but can never commit it: 2 nodes < quorum 3. The client must get a
	// timeout, not an ack.
	resp, err := c.KV(leader1).Put(shortCtx(t, 1500*time.Millisecond),
		&kvpb.PutRequest{Key: "lost", Value: "split-brain"})
	if err == nil && resp.Redirect == nil {
		t.Fatal("stale leader ACKNOWLEDGED a write with 2/5 nodes — split brain")
	}

	// Meanwhile the majority elects a real leader at a higher term and
	// serves clients.
	leader2 := c.WaitForLeaderAmong(majority, 5*time.Second)
	term2 := c.Status(leader2).CurrentTerm
	if term2 <= term1 {
		t.Fatalf("new leader's term %d not above deposed leader's %d", term2, term1)
	}
	c.MustPut("safe", "committed-by-majority")

	// The old leader still believes, seconds later, that it leads — this
	// is what makes leader-side Gets non-linearizable (documented since
	// Phase 2) and why the write above had to fail.
	if s := c.Status(leader1); s.State != raft.Leader {
		t.Logf("note: old leader already stepped down (%v) — unusual timing but legal", s.State)
	}

	c.Heal()
	c.WaitForAgreement(10 * time.Second)

	// The deposed leader must have stepped down and adopted the new term.
	waitFor(t, 5*time.Second, "old leader steps down and follows", func() bool {
		s := c.Status(leader1)
		return s.State == raft.Follower && s.CurrentTerm >= term2
	})

	// No split-brain write survives: "lost" was never committed and its
	// log entry is truncated in favor of the majority's history; "safe"
	// and "stable" are everywhere.
	waitFor(t, 10*time.Second, "all stores converged to the majority history", func() bool {
		for _, id := range c.ids {
			m := c.Store(id).Snapshot()
			if m["stable"] != "before" || m["safe"] != "committed-by-majority" {
				return false
			}
			if _, ok := m["lost"]; ok {
				return false
			}
		}
		return true
	})
	// Belt and braces: assert the poisoned key is really absent (waitFor
	// above would time out, but a direct failure message is clearer).
	for _, id := range c.ids {
		if v, ok := c.Store(id).Get("lost"); ok {
			t.Fatalf("%s still holds the never-acked split-brain write (%q)", id, v)
		}
	}
}

// Scenario 3 — asymmetric partition: the leader's RPCs to one follower are
// dropped while the follower's RPCs to everyone (including the leader)
// still deliver. The starved follower times out and disrupts with
// higher-term candidacies (the disruptive-server pattern PreVote exists
// for); the cluster must keep every acknowledged write and settle on a
// working leader anyway.
func TestAsymmetricPartitionConverges(t *testing.T) {
	c := NewCluster(t, 5)
	leader1 := c.WaitForAgreement(5 * time.Second)
	var starved string
	for _, id := range c.ids {
		if id != leader1 {
			starved = id
			break
		}
	}

	acked := map[string]string{}
	put := func(k, v string) {
		c.MustPut(k, v)
		acked[k] = v
	}
	put("a1", "before")

	c.BlockLink(leader1, starved) // leader→follower dead; reverse alive

	// Writes keep flowing (MustPut rides out any disruption elections the
	// starved follower causes).
	for i := range 4 {
		put(fmt.Sprintf("a2-%d", i), "during")
	}

	// The cluster must reach a stable arrangement serving ALL nodes:
	// either leadership moved off leader1 (whose link is broken), or the
	// starved node ended up served indirectly. Convergence = everyone
	// applied everything some leader committed.
	waitFor(t, 15*time.Second, "all five nodes converge despite the one-way break", func() bool {
		statuses := c.Statuses()
		var lead string
		for id, s := range statuses {
			if s.State == raft.Leader {
				if lead != "" {
					return false
				}
				lead = id
			}
		}
		if lead == "" {
			return false
		}
		commit := statuses[lead].CommitIndex
		for _, s := range statuses {
			if s.LastApplied != commit {
				return false
			}
		}
		return true
	})

	c.Heal()
	put("a3", "after")
	c.WaitForAgreement(10 * time.Second)

	waitFor(t, 10*time.Second, "every acked write on every node", func() bool {
		for _, id := range c.ids {
			m := c.Store(id).Snapshot()
			for k, v := range acked {
				if m[k] != v {
					return false
				}
			}
		}
		return true
	})
}

// Scenario 4 — rapid partition/heal cycling with snapshotting enabled, plus
// a background invariant monitor. Ten cycles of rotating 2-node partitions
// (regularly catching the current leader) with writes throughout. The
// monitor samples every node continuously and fails the test if two nodes
// ever claim leadership OF THE SAME TERM — the definition of split brain.
// (Two leaders of different terms is legal and transient; same term never.)
func TestRapidPartitionHealCycling(t *testing.T) {
	c := NewSnapshottingCluster(t, 5, 5) // compaction chaos included
	c.WaitForAgreement(5 * time.Second)

	// Background split-brain monitor.
	var (
		monMu      sync.Mutex
		termLeader = map[uint64]string{}
		violations []string
	)
	stopMon := make(chan struct{})
	var monWG sync.WaitGroup
	monWG.Add(1)
	go func() {
		defer monWG.Done()
		for {
			select {
			case <-stopMon:
				return
			case <-time.After(10 * time.Millisecond):
			}
			for _, id := range c.ids {
				c.mu.Lock()
				node := c.nodes[id]
				c.mu.Unlock()
				if node == nil {
					continue
				}
				s := node.Status()
				if s.State != raft.Leader {
					continue
				}
				monMu.Lock()
				if prev, ok := termLeader[s.CurrentTerm]; ok && prev != id {
					violations = append(violations,
						fmt.Sprintf("term %d claimed by both %s and %s", s.CurrentTerm, prev, id))
				} else {
					termLeader[s.CurrentTerm] = id
				}
				monMu.Unlock()
			}
		}
	}()

	acked := map[string]string{}
	const cycles = 10
	for i := range cycles {
		// Rotate the cut so every node (leaders included) gets isolated.
		minority := []string{c.ids[i%5], c.ids[(i+1)%5]}
		c.Partition(minority...)

		k := fmt.Sprintf("c%d-during", i)
		c.MustPut(k, "v") // must land on the majority side
		acked[k] = "v"
		time.Sleep(120 * time.Millisecond)

		c.Heal()
		k = fmt.Sprintf("c%d-after", i)
		c.MustPut(k, "v")
		acked[k] = "v"
		time.Sleep(60 * time.Millisecond)
	}

	c.WaitForAgreement(15 * time.Second)
	waitFor(t, 15*time.Second, "cluster fully converged after cycling", func() bool {
		for _, id := range c.ids {
			m := c.Store(id).Snapshot()
			for k, v := range acked {
				if m[k] != v {
					return false
				}
			}
		}
		return true
	})

	close(stopMon)
	monWG.Wait()
	monMu.Lock()
	defer monMu.Unlock()
	for _, v := range violations {
		t.Errorf("SPLIT BRAIN: %s", v)
	}
	if len(termLeader) < 2 {
		t.Error("only one term ever had a leader — the cycling never forced an election, test exercised nothing")
	}
	t.Logf("cycled %d partitions; %d acked writes verified on all nodes; %d terms had (unique) leaders",
		cycles, len(acked), len(termLeader))
}

func shortCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
