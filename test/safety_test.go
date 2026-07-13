package test

import (
	"fmt"
	"testing"
	"time"
)

// The behavioral form of Raft's safety guarantee: every write that was
// ACKNOWLEDGED to a client survives any sequence of leader crashes and
// rejoins. This is the property Figure 8 breaks when the commit rule is
// implemented naively — the deterministic replay of that exact interleaving
// lives in raft/safety_test.go; this test hammers the same guarantee
// through real elections, real replication, and real crash timing for
// several rounds of leadership churn.
//
// It doubles as a canary: HandleAppendEntries panics if any leader ever
// asks a follower to truncate a committed entry, so a safety-rule
// regression fails this test loudly even if the final reads happen to look
// right.
func TestAckedWritesSurviveLeaderChurn(t *testing.T) {
	c := NewCluster(t, 5)
	acked := make(map[string]string)

	put := func(key, value string) {
		c.MustPut(key, value) // MustPut returns only after commit+apply
		acked[key] = value
	}

	const rounds = 5
	for round := range rounds {
		leader := c.WaitForLeader(10 * time.Second)

		// Writes acked by this round's leader...
		for i := range 3 {
			put(fmt.Sprintf("r%d-k%d", round, i), fmt.Sprintf("v%d-%d", round, i))
		}

		// ...then that leader dies immediately. The acked entries live on
		// a majority; the election restriction must funnel leadership to a
		// node that has them all.
		c.Stop(leader)

		// Writes continue against whoever wins next (this is also where a
		// new leader must first commit an own-term entry before the
		// inherited ones count — exercised implicitly on every round).
		put(fmt.Sprintf("r%d-mid", round), fmt.Sprintf("mid%d", round))

		// The crashed ex-leader rejoins with an empty in-memory log and a
		// stale term; it must be caught back up, never listened to.
		c.Restart(leader)
	}

	// Let the whole cluster converge, then verify nothing acked was lost —
	// on ANY node, not just the leader.
	c.WaitForAgreement(10 * time.Second)
	waitFor(t, 10*time.Second, "all nodes fully applied and identical", func() bool {
		var commit uint64
		for _, s := range c.Statuses() {
			if s.CommitIndex > commit {
				commit = s.CommitIndex
			}
		}
		for _, s := range c.Statuses() {
			if s.CommitIndex != commit || s.LastApplied != commit {
				return false
			}
		}
		return true
	})

	for _, id := range c.ids {
		store := c.Store(id)
		for k, want := range acked {
			if got, ok := store.Get(k); !ok || got != want {
				t.Errorf("%s: acked write %s=%q is %q/%v — a committed entry was lost",
					id, k, want, got, ok)
			}
		}
	}
	// And no ghost writes: every node holds exactly the acked set.
	for _, id := range c.ids {
		if n := c.Store(id).Len(); n != len(acked) {
			t.Errorf("%s holds %d keys, want %d", id, n, len(acked))
		}
	}
}
