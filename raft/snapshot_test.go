package raft

import (
	"sync"
	"testing"
	"time"
)

// spySnapshotStore records saves and serves a scripted snapshot.
type spySnapshotStore struct {
	mu    sync.Mutex
	saved []Snapshot
	load  *Snapshot // returned by LoadSnapshot when set
}

func (s *spySnapshotStore) SaveSnapshot(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saved = append(s.saved, snap)
	cp := snap
	s.load = &cp // saving makes it the loadable latest, like the real store
	return nil
}

func (s *spySnapshotStore) LoadSnapshot() (Snapshot, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.load == nil {
		return Snapshot{}, false, nil
	}
	return *s.load, true, nil
}

func (s *spySnapshotStore) lastSaved() (Snapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.saved) == 0 {
		return Snapshot{}, false
	}
	return s.saved[len(s.saved)-1], true
}

// --- Threshold-triggered snapshotting (the applier side) ---

func TestApplierSnapshotsAndCompactsAtThreshold(t *testing.T) {
	sm := &recorderSM{snapshot: []byte("state-bytes")}
	snaps := &spySnapshotStore{}
	logs := &spyLogStore{}
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	cfg.Transport = &fakeTransport{}
	cfg.SnapshotStore = snaps
	cfg.LogStore = logs
	cfg.SnapshotThreshold = 3
	rn := newTestNode(t, cfg)
	rn.SetStateMachine(sm)
	if err := rn.Start(); err != nil {
		t.Fatal(err)
	}
	defer rn.Stop()

	// Feed and commit 4 entries (threshold is 3).
	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 1, Index: 3, Command: []byte("c")},
		{Term: 2, Index: 4, Command: []byte("d")},
	}
	if r := feed(t, rn, 2, 0, 0, entries, 4); !r.Success {
		t.Fatal("append rejected")
	}

	waitFor(t, 2*time.Second, "snapshot taken and log compacted", func() bool {
		s := rn.Status()
		return s.FirstLogIndex == 5 && s.LastApplied == 4
	})

	snap, ok := snaps.lastSaved()
	if !ok {
		t.Fatal("no snapshot saved")
	}
	if snap.LastIncludedIndex != 4 || snap.LastIncludedTerm != 2 {
		t.Fatalf("snapshot covers (%d, term %d), want (4, term 2)",
			snap.LastIncludedIndex, snap.LastIncludedTerm)
	}
	if string(snap.Data) != "state-bytes" {
		t.Fatalf("snapshot data = %q, want the state machine's serialization", snap.Data)
	}
	// The WAL must have been compacted to the (empty) suffix.
	logs.mu.Lock()
	compacts := len(logs.compacts)
	logs.mu.Unlock()
	if compacts == 0 {
		t.Fatal("log store was never compacted — WAL space is never reclaimed")
	}

	// The log still answers correctly at and beyond the boundary: a
	// heartbeat with prevLogIndex == the snapshot boundary must match via
	// the sentinel.
	if r := feed(t, rn, 2, 4, 2, nil, 4); !r.Success {
		t.Fatal("consistency check at the compaction boundary failed")
	}
	// And elections still advertise the right last position.
	if s := rn.Status(); s.LastLogIndex != 4 || s.LastLogTerm != 2 {
		t.Fatalf("last log = (%d, term %d) after full compaction, want (4, term 2)",
			s.LastLogIndex, s.LastLogTerm)
	}
}

// --- HandleInstallSnapshot (the follower side) ---

func snapshotArgs(term uint64) InstallSnapshotArgs {
	return InstallSnapshotArgs{
		Term:              term,
		LeaderID:          "leaderX",
		LastIncludedIndex: 10,
		LastIncludedTerm:  3,
		Data:              []byte("snap-state"),
		Done:              true,
	}
}

func newSnapshotFollower(t *testing.T) (*RaftNode, *recorderSM, *spySnapshotStore) {
	sm := &recorderSM{}
	snaps := &spySnapshotStore{}
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	cfg.Transport = &fakeTransport{}
	cfg.SnapshotStore = snaps
	cfg.LogStore = &spyLogStore{}
	rn := newTestNode(t, cfg)
	rn.SetStateMachine(sm)
	if err := rn.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rn.Stop)
	return rn, sm, snaps
}

func TestInstallSnapshotOnFreshFollower(t *testing.T) {
	rn, sm, snaps := newSnapshotFollower(t)

	reply := rn.HandleInstallSnapshot(snapshotArgs(3))
	if reply.Term != 3 {
		t.Fatalf("reply.Term = %d, want 3", reply.Term)
	}

	waitFor(t, 2*time.Second, "state machine restored", func() bool {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		return sm.restores == 1
	})
	sm.mu.Lock()
	restored := string(sm.snapshot)
	sm.mu.Unlock()
	if restored != "snap-state" {
		t.Fatalf("restored %q, want snap-state", restored)
	}

	s := rn.Status()
	if s.FirstLogIndex != 11 || s.LastLogIndex != 10 || s.LastLogTerm != 3 {
		t.Fatalf("log after install: first=%d last=(%d, term %d), want 11/(10, term 3)",
			s.FirstLogIndex, s.LastLogIndex, s.LastLogTerm)
	}
	if s.CommitIndex != 10 {
		t.Fatalf("commitIndex = %d, want 10 (snapshot contents are committed)", s.CommitIndex)
	}
	waitFor(t, 2*time.Second, "lastApplied catches the snapshot", func() bool {
		return rn.Status().LastApplied == 10
	})
	// And it was persisted before the reply.
	if snap, ok := snaps.lastSaved(); !ok || snap.LastIncludedIndex != 10 {
		t.Fatalf("persisted snapshot = %+v, %v — must be durable before the reply", snap, ok)
	}
}

func TestInstallSnapshotRejectsStaleLeader(t *testing.T) {
	rn, sm, _ := newSnapshotFollower(t)
	// Move to term 5 first.
	rn.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node2"})

	reply := rn.HandleInstallSnapshot(snapshotArgs(3)) // term 3 < 5
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5 so the stale leader steps down", reply.Term)
	}
	if s := rn.Status(); s.FirstLogIndex != 1 {
		t.Fatal("stale leader's snapshot was installed")
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.restores != 0 {
		t.Fatal("stale leader's snapshot reached the state machine")
	}
}

func TestInstallSnapshotIgnoresAlreadyCoveredState(t *testing.T) {
	rn, sm, _ := newSnapshotFollower(t)

	// The follower already has 12 committed entries.
	var entries []LogEntry
	for i := uint64(1); i <= 12; i++ {
		entries = append(entries, LogEntry{Term: 3, Index: i, Command: []byte("x")})
	}
	feed(t, rn, 3, 0, 0, entries, 12)
	waitFor(t, 2*time.Second, "applied", func() bool { return rn.Status().LastApplied == 12 })

	// A duplicate/older snapshot through index 10 arrives late. Installing
	// it would rewind the state machine below what clients may have read.
	rn.HandleInstallSnapshot(snapshotArgs(3))
	if s := rn.Status(); s.LastLogIndex != 12 || s.CommitIndex != 12 {
		t.Fatalf("stale snapshot damaged the log: last=%d commit=%d, want 12/12",
			s.LastLogIndex, s.CommitIndex)
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.restores != 0 {
		t.Fatal("stale snapshot reached the state machine")
	}
}

// Figure 13 step 6: a follower whose log extends beyond the snapshot and
// matches it at the boundary keeps its suffix — those entries are real.
func TestInstallSnapshotRetainsMatchingSuffix(t *testing.T) {
	rn, _, _ := newSnapshotFollower(t)

	// 12 entries, terms such that index 10 has term 3 (matching the
	// incoming snapshot), but NOTHING is committed yet.
	var entries []LogEntry
	for i := uint64(1); i <= 12; i++ {
		entries = append(entries, LogEntry{Term: 3, Index: i, Command: []byte("x")})
	}
	feed(t, rn, 3, 0, 0, entries, 0) // leaderCommit 0: uncommitted

	rn.HandleInstallSnapshot(snapshotArgs(3)) // covers through (10, term 3)

	s := rn.Status()
	if s.FirstLogIndex != 11 || s.LastLogIndex != 12 {
		t.Fatalf("log after install: first=%d last=%d, want 11/12 (suffix retained)",
			s.FirstLogIndex, s.LastLogIndex)
	}
	if s.CommitIndex != 10 {
		t.Fatalf("commitIndex = %d, want 10", s.CommitIndex)
	}
}

// --- Leader side: sending snapshots to lagging peers ---

func TestLeaderSendsSnapshotWhenPeerNeedsCompactedEntries(t *testing.T) {
	var (
		mu        sync.Mutex
		installed []InstallSnapshotArgs
	)
	snaps := &spySnapshotStore{}
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	cfg.Peers = []string{"node2", "node3"}
	cfg.SnapshotStore = snaps
	cfg.LogStore = &spyLogStore{}
	cfg.Transport = &fakeTransport{
		appendEntries: func(_ string, args AppendEntriesArgs) (AppendEntriesReply, error) {
			return AppendEntriesReply{Term: args.Term, Success: true}, nil
		},
		installSnapshot: func(peer string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
			mu.Lock()
			installed = append(installed, args)
			mu.Unlock()
			return InstallSnapshotReply{Term: args.Term}, nil
		},
	}
	rn := newTestNode(t, cfg)
	rn.SetStateMachine(&recorderSM{})

	// Manufacture: a leader at term 4 whose log was compacted through
	// (10, term 3) and has entries 11–12.
	if err := snaps.SaveSnapshot(Snapshot{LastIncludedIndex: 10, LastIncludedTerm: 3, Data: []byte("s")}); err != nil {
		t.Fatal(err)
	}
	rn.mu.Lock()
	rn.log = []LogEntry{
		{Term: 3, Index: 10}, // sentinel at the compaction boundary
		{Term: 4, Index: 11}, {Term: 4, Index: 12},
	}
	rn.commitIndex = 10
	rn.lastApplied = 10
	rn.currentTerm = 4
	rn.becomeLeaderLocked()
	// node2 is far behind: it needs entry 5, which only the snapshot has.
	rn.nextIndex["node2"] = 5
	term := rn.currentTerm
	rn.mu.Unlock()
	defer rn.Stop()

	if !rn.replicationRound(term) {
		t.Fatal("replication round refused to run")
	}
	waitFor(t, 2*time.Second, "snapshot sent to the lagging peer", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(installed) >= 1
	})
	mu.Lock()
	got := installed[0]
	mu.Unlock()
	if got.LastIncludedIndex != 10 || got.LastIncludedTerm != 3 || !got.Done {
		t.Fatalf("sent snapshot (%d, term %d, done=%v), want (10, term 3, true)",
			got.LastIncludedIndex, got.LastIncludedTerm, got.Done)
	}

	// The successful send must advance the peer's replication cursor past
	// the snapshot so entries 11+ flow as normal AppendEntries next round.
	waitFor(t, 2*time.Second, "nextIndex advanced past the snapshot", func() bool {
		rn.mu.Lock()
		defer rn.mu.Unlock()
		return rn.nextIndex["node2"] == 11 && rn.matchIndex["node2"] == 10
	})
}

// --- Recovery with a snapshot on disk ---

func TestNewNodeRecoversFromSnapshotPlusWAL(t *testing.T) {
	snaps := &spySnapshotStore{
		load: &Snapshot{LastIncludedIndex: 10, LastIncludedTerm: 3, Data: []byte("snap-state")},
	}
	logs := &spyLogStore{preloaded: []LogEntry{
		// Crash-between-save-and-compact leftovers: entries the snapshot
		// already covers must be skipped on load...
		{Term: 3, Index: 9}, {Term: 3, Index: 10},
		// ...and the real suffix beyond it kept.
		{Term: 4, Index: 11}, {Term: 4, Index: 12},
	}}
	sm := &recorderSM{}
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	cfg.Transport = &fakeTransport{}
	cfg.SnapshotStore = snaps
	cfg.LogStore = logs
	rn := newTestNode(t, cfg)
	rn.SetStateMachine(sm)
	if err := rn.Start(); err != nil {
		t.Fatal(err)
	}
	defer rn.Stop()

	s := rn.Status()
	if s.FirstLogIndex != 11 || s.LastLogIndex != 12 {
		t.Fatalf("recovered log: first=%d last=%d, want 11/12", s.FirstLogIndex, s.LastLogIndex)
	}
	if s.CommitIndex != 10 || s.LastApplied != 10 {
		t.Fatalf("commit/applied = %d/%d, want 10/10 (snapshot contents are committed+applied)",
			s.CommitIndex, s.LastApplied)
	}
	// Start must have pushed the snapshot into the state machine.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.restores != 1 || string(sm.snapshot) != "snap-state" {
		t.Fatalf("restores=%d snapshot=%q, want 1/snap-state", sm.restores, sm.snapshot)
	}
}

func TestStartRefusesSnapshotWithoutStateMachine(t *testing.T) {
	snaps := &spySnapshotStore{load: &Snapshot{LastIncludedIndex: 5, LastIncludedTerm: 1, Data: []byte("s")}}
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	cfg.Transport = &fakeTransport{}
	cfg.SnapshotStore = snaps
	rn := newTestNode(t, cfg)

	if err := rn.Start(); err == nil {
		rn.Stop()
		t.Fatal("Start accepted a recovered snapshot with nowhere to restore it")
	}
}
