package test

import (
	"io"
	"log/slog"
	"testing"

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
