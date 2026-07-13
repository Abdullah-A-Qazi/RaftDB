package raft

import (
	"sync"
	"testing"
)

// spyLogStore records mutations so tests can assert the durability half of
// persist-before-reply: whatever a handler acknowledged must have hit the
// store by the time the handler returned.
type spyLogStore struct {
	mu        sync.Mutex
	appended  []LogEntry
	truncates []uint64
	preloaded []LogEntry
	compacts  [][]LogEntry
}

func (s *spyLogStore) Compact(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.compacts = append(s.compacts, entries)
	return nil
}

func (s *spyLogStore) AppendEntries(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appended = append(s.appended, entries...)
	return nil
}

func (s *spyLogStore) TruncateSuffix(from uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.truncates = append(s.truncates, from)
	return nil
}

func (s *spyLogStore) LoadEntries() ([]LogEntry, error) {
	return s.preloaded, nil
}

func (s *spyLogStore) appendedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.appended)
}

func TestFollowerPersistsEntriesBeforeReply(t *testing.T) {
	spy := &spyLogStore{}
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	cfg.LogStore = spy
	rn := newTestNode(t, cfg)

	r := feed(t, rn, 1, 0, 0, []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	}, 0)
	if !r.Success {
		t.Fatal("append rejected")
	}
	// The reply exists; the entries must already be in the store.
	if got := spy.appendedCount(); got != 2 {
		t.Fatalf("store has %d entries at reply time, want 2 — the leader is about to count an un-durable ack", got)
	}
}

func TestFollowerPersistsTruncationBeforeReply(t *testing.T) {
	spy := &spyLogStore{}
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	cfg.LogStore = spy
	rn := newTestNode(t, cfg)

	feed(t, rn, 1, 0, 0, []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("old")},
	}, 0)
	// New leader replaces index 2.
	r := feed(t, rn, 2, 1, 1, []LogEntry{{Term: 2, Index: 2, Command: []byte("new")}}, 0)
	if !r.Success {
		t.Fatal("overwrite rejected")
	}
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if len(spy.truncates) != 1 || spy.truncates[0] != 2 {
		t.Fatalf("truncates = %v, want [2]: a conflict wipe that isn't durable resurrects on restart", spy.truncates)
	}
}

func TestLeaderPersistsOwnEntryOnPropose(t *testing.T) {
	spy := &spyLogStore{}
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	cfg.Peers = []string{"node2", "node3"}
	cfg.Transport = deadTransport()
	cfg.LogStore = spy
	rn := newTestNode(t, cfg)
	makeLeader(rn, 1)
	t.Cleanup(rn.Stop)

	if _, _, err := rn.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	// The leader counts itself toward the quorum; its copy must be durable
	// from the moment the entry exists.
	if got := spy.appendedCount(); got != 1 {
		t.Fatalf("store has %d entries after Propose, want 1", got)
	}
}

func TestNewNodeRecoversLog(t *testing.T) {
	spy := &spyLogStore{preloaded: []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 2, Index: 2, Command: []byte("b")},
	}}
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	cfg.LogStore = spy
	rn := newTestNode(t, cfg)

	s := rn.Status()
	if s.LastLogIndex != 2 || s.LastLogTerm != 2 {
		t.Fatalf("recovered last = (%d, term %d), want (2, term 2)", s.LastLogIndex, s.LastLogTerm)
	}
	// Volatile per Figure 2 — must NOT be resurrected.
	if s.CommitIndex != 0 || s.LastApplied != 0 {
		t.Fatalf("commit/applied = %d/%d after recovery, want 0/0 (volatile state)", s.CommitIndex, s.LastApplied)
	}
}

func TestNewNodeRejectsNonContiguousRecoveredLog(t *testing.T) {
	spy := &spyLogStore{preloaded: []LogEntry{
		{Term: 1, Index: 1}, {Term: 1, Index: 3}, // hole at 2
	}}
	cfg := slowFollowerConfig()
	cfg.ID = "node1"
	cfg.Store = &memStore{}
	cfg.LogStore = spy
	if _, err := NewNode(cfg); err == nil {
		t.Fatal("NewNode accepted a recovered log with a hole — every index formula would be wrong")
	}
}
