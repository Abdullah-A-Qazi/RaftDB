package raft

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"
)

// memStore is an in-memory StateStore for white-box tests. It records every
// save so tests can assert that persistence happened (and with what) by the
// time a handler returned.
type memStore struct {
	mu    sync.Mutex
	hs    HardState
	found bool
	saves []HardState
}

func (m *memStore) SaveHardState(hs HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs = hs
	m.found = true
	m.saves = append(m.saves, hs)
	return nil
}

func (m *memStore) LoadHardState() (HardState, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hs, m.found, nil
}

func (m *memStore) saved() HardState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hs
}

func (m *memStore) saveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.saves)
}

// fakeTransport scripts peer behavior per RPC type.
type fakeTransport struct {
	requestVote     func(peerID string, args RequestVoteArgs) (RequestVoteReply, error)
	appendEntries   func(peerID string, args AppendEntriesArgs) (AppendEntriesReply, error)
	installSnapshot func(peerID string, args InstallSnapshotArgs) (InstallSnapshotReply, error)
}

func (f *fakeTransport) RequestVote(ctx context.Context, peerID string, args RequestVoteArgs) (RequestVoteReply, error) {
	return f.requestVote(peerID, args)
}

func (f *fakeTransport) AppendEntries(ctx context.Context, peerID string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	if f.appendEntries == nil {
		return AppendEntriesReply{Term: args.Term, Success: true}, nil
	}
	return f.appendEntries(peerID, args)
}

func (f *fakeTransport) InstallSnapshot(ctx context.Context, peerID string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	if f.installSnapshot == nil {
		// Tests that don't script snapshots shouldn't trigger them.
		return InstallSnapshotReply{}, errors.New("fakeTransport: InstallSnapshot not scripted")
	}
	return f.installSnapshot(peerID, args)
}

func testLogger(t *testing.T) *slog.Logger {
	w := io.Writer(io.Discard)
	if testing.Verbose() {
		w = os.Stderr
	}
	return slog.New(slog.NewTextHandler(w, nil))
}

// newTestNode builds an unstarted node (handlers work without Start).
func newTestNode(t *testing.T, cfg Config) *RaftNode {
	t.Helper()
	if cfg.ID == "" {
		cfg.ID = "node1"
	}
	if cfg.Store == nil {
		cfg.Store = &memStore{}
	}
	if cfg.Logger == nil {
		cfg.Logger = testLogger(t)
	}
	rn, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return rn
}

// waitFor polls cond until it holds or timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", timeout, msg)
}
