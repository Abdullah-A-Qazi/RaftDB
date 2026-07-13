package test

import (
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Abdullah-A-Qazi/RaftDB/raft"
	"github.com/Abdullah-A-Qazi/RaftDB/storage"
)

func quietNode(t *testing.T, id string, dir string) *raft.RaftNode {
	t.Helper()
	store, err := storage.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	node, err := raft.NewNode(raft.Config{
		ID:    id,
		Peers: []string{"node2", "node3"},
		Store: store,
		// No transport, never Started: we only poke the RPC handlers,
		// exactly as a real node would be poked between crash and timer.
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return node
}

// THE persistence property (§5.2 via Figure 2's "Updated on stable storage
// before responding to RPCs"): a vote must survive a crash. If it didn't, a
// node could vote for A, crash, restart empty, and vote for B — two votes
// in one term, potentially two leaders in one term.
func TestVoteSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	// Incarnation 1 votes for node2 in term 1, then "crashes" (dropped
	// without any graceful shutdown; only the store's files remain).
	n1 := quietNode(t, "node1", dir)
	if r := n1.HandleRequestVote(raft.RequestVoteArgs{Term: 1, CandidateID: "node2"}); !r.VoteGranted {
		t.Fatal("setup: first vote not granted")
	}

	// Incarnation 2 recovers from the same directory.
	n2 := quietNode(t, "node1", dir)
	if r := n2.HandleRequestVote(raft.RequestVoteArgs{Term: 1, CandidateID: "node3"}); r.VoteGranted {
		t.Fatal("restarted node granted a second vote in the same term")
	}
	// The original candidate retrying is still fine (idempotent).
	if r := n2.HandleRequestVote(raft.RequestVoteArgs{Term: 1, CandidateID: "node2"}); !r.VoteGranted {
		t.Fatal("restarted node denied the candidate it had already voted for")
	}
}

// The definitive durability test: stop EVERY node, restart them all, and
// require every acknowledged write back. With all five processes down there
// is no live replica to catch anyone up — the data exists nowhere but the
// WALs, so this passes only if the log genuinely round-trips through disk.
func TestFullClusterRestartRecoversData(t *testing.T) {
	c := NewCluster(t, 5)
	c.WaitForAgreement(5 * time.Second)

	acked := make(map[string]string)
	for i := range 8 {
		k, v := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		c.MustPut(k, v)
		acked[k] = v
	}
	c.MustDelete("k0")
	delete(acked, "k0")

	for _, id := range c.ids {
		c.Stop(id)
	}
	for _, id := range c.ids {
		c.Restart(id)
	}

	// A restarted node knows its log but not commitIndex (volatile, per
	// Figure 2). The new leader's first committed own-term entry — via a
	// write — re-establishes it; until then nothing applies. (This is the
	// "no no-op entry" liveness quirk from Phase 3, now load-bearing:
	// without this write the data would sit committed-but-unapplied
	// indefinitely on an idle cluster.)
	c.WaitForAgreement(10 * time.Second)
	c.MustPut("post-restart", "ok")
	acked["post-restart"] = "ok"

	waitFor(t, 10*time.Second, "all recovered data applied on every node", func() bool {
		for _, id := range c.ids {
			store := c.Store(id)
			if store.Len() != len(acked) {
				return false
			}
			for k, v := range acked {
				if got, ok := store.Get(k); !ok || got != v {
					return false
				}
			}
		}
		return true
	})
	// And the delete must have stayed deleted.
	for _, id := range c.ids {
		if _, ok := c.Store(id).Get("k0"); ok {
			t.Fatalf("%s resurrected deleted key k0 from the WAL replay", id)
		}
	}
}

// currentTerm must survive too: a node restarting with an old term could
// accept a deposed leader's heartbeats as current.
func TestTermSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	n1 := quietNode(t, "node1", dir)
	n1.HandleAppendEntries(raft.AppendEntriesArgs{Term: 9, LeaderID: "node2"})

	n2 := quietNode(t, "node1", dir)
	if got := n2.Status().CurrentTerm; got != 9 {
		t.Fatalf("recovered term = %d, want 9", got)
	}
	// A heartbeat from a leader deposed before the crash must be rejected.
	if r := n2.HandleAppendEntries(raft.AppendEntriesArgs{Term: 5, LeaderID: "node3"}); r.Success {
		t.Fatal("restarted node accepted a stale leader it had already outgrown")
	}
}
