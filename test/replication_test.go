package test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/Abdullah-A-Qazi/RaftDB/rpc/kvpb"
)

// Scenario: a write acknowledged to the client is immediately visible on
// the leader (ack happens only after commit + apply).
func TestWriteVisibleOnLeaderAfterCommit(t *testing.T) {
	c := NewCluster(t, 5)
	c.WaitForAgreement(5 * time.Second)

	c.MustPut("color", "green")
	if v, found := c.GetFromLeader("color"); !found || v != "green" {
		t.Fatalf("Get(color) = %q/%v right after acked Put, want green/true", v, found)
	}

	c.MustDelete("color")
	if _, found := c.GetFromLeader("color"); found {
		t.Fatal("key visible after acked Delete")
	}
}

// Scenario: a client that talks to a follower gets pointed at the leader.
func TestFollowerRedirectsToLeader(t *testing.T) {
	c := NewCluster(t, 3)
	leader := c.WaitForAgreement(5 * time.Second)

	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}

	resp, err := c.KV(follower).Put(context.Background(), &kvpb.PutRequest{Key: "k", Value: "v"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Redirect == nil {
		t.Fatal("follower accepted a write")
	}
	if resp.Redirect.LeaderId != leader {
		t.Fatalf("redirect points at %q, want %q", resp.Redirect.LeaderId, leader)
	}
	if want := clientAddr(leader); resp.Redirect.LeaderAddr != want {
		t.Fatalf("redirect addr %q, want %q", resp.Redirect.LeaderAddr, want)
	}
}

// Scenario: a follower that was down while writes happened catches up fully
// after restart (log replay rebuilds its state machine).
func TestFollowerRestartCatchesUp(t *testing.T) {
	c := NewCluster(t, 5)
	leader := c.WaitForAgreement(5 * time.Second)

	for i := range 5 {
		c.MustPut(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}

	// Take down a follower, keep writing (including overwrites and a
	// delete so catch-up isn't just "append blindly").
	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	c.Stop(follower)
	for i := 5; i < 10; i++ {
		c.MustPut(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	c.MustPut("k0", "v0-rewritten")
	c.MustDelete("k1")

	c.Restart(follower)
	waitFor(t, 5*time.Second, "restarted follower to catch up", func() bool {
		var leaderStatus, followerStatus = c.Status(leader), c.Status(follower)
		if followerStatus.CommitIndex != leaderStatus.CommitIndex ||
			followerStatus.LastApplied != leaderStatus.CommitIndex {
			return false
		}
		lm, fm := c.Store(leader).Snapshot(), c.Store(follower).Snapshot()
		if len(lm) != len(fm) {
			return false
		}
		for k, v := range lm {
			if fm[k] != v {
				return false
			}
		}
		return true
	})

	// Spot-check semantics survived the replay.
	if v, _ := c.Store(follower).Get("k0"); v != "v0-rewritten" {
		t.Fatalf("follower k0 = %q, want v0-rewritten", v)
	}
	if _, ok := c.Store(follower).Get("k1"); ok {
		t.Fatal("follower still has deleted key k1")
	}
}

// Scenario: every node applies the same entries in the same order — the
// property that makes replicated state machines work at all.
func TestSameApplyOrderOnAllNodes(t *testing.T) {
	c := NewCluster(t, 5)
	c.WaitForAgreement(5 * time.Second)

	const writes = 20
	for i := range writes {
		key := fmt.Sprintf("k%d", i%7) // collisions on purpose: order matters
		c.MustPut(key, fmt.Sprintf("v%d", i))
	}

	// Wait for every node to finish applying everything the leader
	// committed.
	waitFor(t, 5*time.Second, "all nodes fully applied", func() bool {
		leaderCommit := uint64(0)
		for _, s := range c.Statuses() {
			if s.CommitIndex > leaderCommit {
				leaderCommit = s.CommitIndex
			}
		}
		for _, s := range c.Statuses() {
			if s.LastApplied != leaderCommit {
				return false
			}
		}
		return true
	})

	reference := c.Applied(c.ids[0])
	if len(reference) < writes {
		t.Fatalf("node1 applied %d entries, want >= %d", len(reference), writes)
	}
	for _, id := range c.ids[1:] {
		if got := c.Applied(id); !slices.Equal(got, reference) {
			t.Fatalf("apply order differs:\n%s: %v\n%s: %v", c.ids[0], reference, id, got)
		}
	}
	// And identical final state.
	referenceMap := c.Store(c.ids[0]).Snapshot()
	for _, id := range c.ids[1:] {
		m := c.Store(id).Snapshot()
		if len(m) != len(referenceMap) {
			t.Fatalf("%s has %d keys, %s has %d", c.ids[0], len(referenceMap), id, len(m))
		}
		for k, v := range referenceMap {
			if m[k] != v {
				t.Fatalf("%s[%q]=%q, %s[%q]=%q", c.ids[0], k, referenceMap[k], id, k, m[k])
			}
		}
	}
}

// Scenario: writes survive losing a minority mid-stream — kill 2 of 5,
// write, verify the ack was honest by bringing the dead nodes back and
// checking the value is everywhere.
func TestWriteSucceedsWithMinorityDown(t *testing.T) {
	c := NewCluster(t, 5)
	leader := c.WaitForAgreement(5 * time.Second)

	// Kill two followers (a minority; leader + 2 = quorum remains).
	var downed []string
	for _, id := range c.ids {
		if id != leader && len(downed) < 2 {
			c.Stop(id)
			downed = append(downed, id)
		}
	}

	c.MustPut("durable", "yes") // must succeed with 3/5 alive
	if v, found := c.GetFromLeader("durable"); !found || v != "yes" {
		t.Fatalf("Get(durable) = %q/%v, want yes/true", v, found)
	}

	// The dead rejoin; the write must reach them too.
	for _, id := range downed {
		c.Restart(id)
	}
	waitFor(t, 5*time.Second, "restarted nodes to receive the write", func() bool {
		for _, id := range downed {
			if v, found := c.Store(id).Get("durable"); !found || v != "yes" {
				return false
			}
		}
		return true
	})
}

// Scenario: with a majority down, a write must NOT be acknowledged. And
// once quorum is restored, the un-acked write is allowed to (and here,
// does) commit — "timeout" means unknown outcome, not "didn't happen".
func TestWriteNotAckedWithoutQuorum(t *testing.T) {
	c := NewCluster(t, 5)
	leader := c.WaitForAgreement(5 * time.Second)

	// Kill three followers: leader + 1 = 2 < 3 = quorum.
	var downed []string
	for _, id := range c.ids {
		if id != leader && len(downed) < 3 {
			c.Stop(id)
			downed = append(downed, id)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := c.KV(leader).Put(ctx, &kvpb.PutRequest{Key: "limbo", Value: "?"})
	if err == nil {
		t.Fatal("write acknowledged with 2/5 nodes alive — that ack could be lost forever")
	}
	// Not applied on the leader either (commit never advanced).
	if _, found := c.Store(leader).Get("limbo"); found {
		t.Fatal("unacked write visible in leader state machine")
	}

	// Restore quorum: the leader still leads (nobody could depose it), so
	// the entry sitting in its log now replicates and commits.
	c.Restart(downed[0])
	waitFor(t, 5*time.Second, "limbo write to commit after quorum restored", func() bool {
		v, found := c.Store(leader).Get("limbo")
		return found && v == "?"
	})
}
