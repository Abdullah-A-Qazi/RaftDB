package test

import (
	"fmt"
	"testing"
	"time"
)

// The Phase 5 scenario verbatim: a follower goes offline, the leader
// compacts its log past the entries the follower would need, and on rejoin
// the follower must catch up via InstallSnapshot — not get stuck retrying
// AppendEntries for entries that no longer exist anywhere.
func TestOfflineFollowerCatchesUpViaInstallSnapshot(t *testing.T) {
	const threshold = 5
	c := NewSnapshottingCluster(t, 5, threshold)
	leader := c.WaitForAgreement(5 * time.Second)

	// Pick a follower and take it down.
	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	c.MustPut("before", "outage")
	c.Stop(follower)

	// Write enough for every live node to snapshot and compact well past
	// the follower's last entry (it stopped around index 2).
	for i := range 20 {
		c.MustPut(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	waitFor(t, 5*time.Second, "leader compacted past the follower's log", func() bool {
		return c.Status(leader).FirstLogIndex > 3
	})

	c.Restart(follower)
	waitFor(t, 10*time.Second, "follower caught up to the leader", func() bool {
		f, l := c.Status(follower), c.Status(leader)
		return f.LastApplied == l.CommitIndex && f.LastLogIndex == l.LastLogIndex
	})

	// The catch-up must have gone through InstallSnapshot: the entries it
	// needed are compacted away, so there was no other path.
	if got := c.SnapshotsDeliveredTo(follower); got == 0 {
		t.Fatal("follower caught up without receiving any InstallSnapshot — where did the compacted entries come from?")
	}
	// And the follower's state machine must be byte-for-byte right,
	// including data from before its outage.
	fm, lm := c.Store(follower).Snapshot(), c.Store(leader).Snapshot()
	if len(fm) != len(lm) {
		t.Fatalf("follower has %d keys, leader %d", len(fm), len(lm))
	}
	for k, v := range lm {
		if fm[k] != v {
			t.Fatalf("follower[%q] = %q, leader has %q", k, fm[k], v)
		}
	}
	if v, ok := fm["before"]; !ok || v != "outage" {
		t.Fatalf("pre-outage key = %q/%v, want outage/true", v, ok)
	}

	// The rejoined follower is a full participant again: it can carry a
	// quorum. Stop two OTHER followers (leader + rejoined follower + one
	// more = 3 of 5) and require writes to still commit.
	stopped := 0
	for _, id := range c.ids {
		if id != leader && id != follower && stopped < 2 {
			c.Stop(id)
			stopped++
		}
	}
	c.MustPut("quorum-with-rejoined", "yes")
	waitFor(t, 5*time.Second, "rejoined follower holds the new write", func() bool {
		v, ok := c.Store(follower).Get("quorum-with-rejoined")
		return ok && v == "yes"
	})
}

// Restarting a node that has snapshots on disk must recover through
// snapshot + WAL suffix (its lastApplied resumes at the snapshot boundary,
// not zero — the log below it no longer exists to replay).
func TestRestartRecoversFromSnapshotAndWAL(t *testing.T) {
	const threshold = 4
	c := NewSnapshottingCluster(t, 5, threshold)
	c.WaitForAgreement(5 * time.Second)

	for i := range 10 {
		c.MustPut(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	c.MustDelete("k3")

	// Wait for every node to have compacted at least once, so the restart
	// below cannot possibly replay from index 1.
	waitFor(t, 5*time.Second, "all nodes compacted", func() bool {
		for _, s := range c.Statuses() {
			if s.FirstLogIndex == 1 {
				return false
			}
		}
		return true
	})

	// Full-cluster restart: recovery has nothing to copy from — snapshot
	// plus WAL suffix on each node's own disk is the only source.
	for _, id := range c.ids {
		c.Stop(id)
	}
	for _, id := range c.ids {
		c.Restart(id)
	}
	c.WaitForAgreement(10 * time.Second)
	c.MustPut("post", "restart") // unblocks commit re-establishment

	waitFor(t, 10*time.Second, "all nodes rebuilt identical state", func() bool {
		want := map[string]string{"post": "restart"}
		for i := range 10 {
			if i != 3 {
				want[fmt.Sprintf("k%d", i)] = fmt.Sprintf("v%d", i)
			}
		}
		for _, id := range c.ids {
			m := c.Store(id).Snapshot()
			if len(m) != len(want) {
				return false
			}
			for k, v := range want {
				if m[k] != v {
					return false
				}
			}
		}
		return true
	})

	// Every recovered node resumed from its snapshot, not from scratch.
	for _, id := range c.ids {
		if s := c.Status(id); s.FirstLogIndex == 1 {
			t.Errorf("%s recovered with FirstLogIndex 1 — it ignored its snapshot", id)
		}
		if r := c.recorderOf(id); r.restoreCount() == 0 {
			t.Errorf("%s never restored a snapshot into its state machine", id)
		}
	}
}

// recorderOf and restoreCount expose the harness recorder for assertions.
func (c *Cluster) recorderOf(id string) *applyRecorder {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recorders[id]
}

func (r *applyRecorder) restoreCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.restores
}

// Snapshotting must never cost an acked write: run the Phase 3 churn
// (kill the leader repeatedly during writes) with aggressive compaction
// enabled and require every acknowledged write on every node.
func TestAckedWritesSurviveChurnWithSnapshots(t *testing.T) {
	c := NewSnapshottingCluster(t, 5, 3) // compact every ~3 entries
	acked := make(map[string]string)

	for round := range 4 {
		leader := c.WaitForLeader(10 * time.Second)
		for i := range 4 {
			k, v := fmt.Sprintf("r%d-k%d", round, i), fmt.Sprintf("v%d-%d", round, i)
			c.MustPut(k, v)
			acked[k] = v
		}
		c.Stop(leader)
		k := fmt.Sprintf("r%d-mid", round)
		c.MustPut(k, "mid")
		acked[k] = "mid"
		c.Restart(leader)
	}

	c.WaitForAgreement(10 * time.Second)
	waitFor(t, 10*time.Second, "cluster fully converged", func() bool {
		for _, id := range c.ids {
			m := c.Store(id).Snapshot()
			if len(m) != len(acked) {
				return false
			}
		}
		return true
	})
	for _, id := range c.ids {
		store := c.Store(id)
		for k, want := range acked {
			if got, ok := store.Get(k); !ok || got != want {
				t.Errorf("%s: acked %s=%q is %q/%v — lost across snapshotting churn",
					id, k, want, got, ok)
			}
		}
	}

	if got := c.Status(c.WaitForLeader(5 * time.Second)); got.FirstLogIndex == 1 {
		t.Error("no compaction ever happened — the test exercised nothing")
	}
}
