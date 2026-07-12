package kv

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Abdullah-A-Qazi/RaftDB/raft"
	"github.com/Abdullah-A-Qazi/RaftDB/rpc/kvpb"
)

// --- Command encoding ---

func TestCommandRoundTrip(t *testing.T) {
	for _, in := range []Command{
		{Op: OpPut, Key: "k", Value: "v"},
		{Op: OpDelete, Key: "k"},
	} {
		data, err := in.Encode()
		if err != nil {
			t.Fatal(err)
		}
		out, err := DecodeCommand(data)
		if err != nil {
			t.Fatal(err)
		}
		if out != in {
			t.Fatalf("round trip: got %+v, want %+v", out, in)
		}
	}
}

func TestCommandRejectsGarbage(t *testing.T) {
	if _, err := (Command{Op: "increment", Key: "k"}).Encode(); err == nil {
		t.Error("encoded unknown op")
	}
	if _, err := DecodeCommand([]byte("{not json")); err == nil {
		t.Error("decoded corrupt bytes")
	}
	if _, err := DecodeCommand([]byte(`{"op":"increment","key":"k"}`)); err == nil {
		t.Error("decoded unknown op")
	}
}

// --- Store ---

func TestStoreApplyAndGet(t *testing.T) {
	s := NewStore()
	if err := s.apply(Command{Op: OpPut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := s.apply(Command{Op: OpPut, Key: "a", Value: "2"}); err != nil {
		t.Fatal(err)
	}
	if v, ok := s.Get("a"); !ok || v != "2" {
		t.Fatalf("Get(a) = %q/%v, want 2/true", v, ok)
	}
	if err := s.apply(Command{Op: OpDelete, Key: "a"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("key survived delete")
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d, want 0", s.Len())
	}
}

// --- Server (follower behavior; leader paths are covered by the cluster
// tests in /test, which run real elections) ---

type nopStore struct{}

func (nopStore) SaveHardState(raft.HardState) error          { return nil }
func (nopStore) LoadHardState() (raft.HardState, bool, error) { return raft.HardState{}, false, nil }

func newFollowerServer(t *testing.T) *Server {
	t.Helper()
	node, err := raft.NewNode(raft.Config{
		ID:     "node1",
		Peers:  []string{"node2", "node3"},
		Store:  nopStore{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Never started: the node sits as a follower with no leader known.
	return NewServer(node, NewStore(), map[string]string{
		"node1": "addr1", "node2": "addr2", "node3": "addr3",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestFollowerRedirectsWrites(t *testing.T) {
	s := newFollowerServer(t)
	resp, err := s.Put(context.Background(), &kvpb.PutRequest{Key: "k", Value: "v"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Redirect == nil {
		t.Fatal("follower accepted a write without redirecting")
	}
	if resp.Redirect.LeaderAddr != "" {
		t.Errorf("leader addr = %q, want empty (no leader known)", resp.Redirect.LeaderAddr)
	}
}

func TestFollowerRedirectsReads(t *testing.T) {
	s := newFollowerServer(t)
	resp, err := s.Get(context.Background(), &kvpb.GetRequest{Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Redirect == nil {
		t.Fatal("follower served a read locally — that read could be arbitrarily stale")
	}
}

// The (index, term) check in fireWaiter: an entry applied at the waited
// index but with a different term means the original command was replaced
// by another leader — the client must get an error, never a false OK.
func TestWaiterRejectsSupersededEntry(t *testing.T) {
	s := newFollowerServer(t)
	cmd, _ := Command{Op: OpPut, Key: "k", Value: "v"}.Encode()

	ch := make(chan error, 1)
	s.mu.Lock()
	s.waiters[3] = waiter{term: 2, ch: ch}
	s.mu.Unlock()

	// Index 3 gets applied, but carrying term 5, not 2.
	s.Apply(raft.LogEntry{Index: 3, Term: 5, Command: cmd})

	select {
	case err := <-ch:
		if err != raft.ErrNotLeader {
			t.Fatalf("waiter got %v, want ErrNotLeader", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter never fired")
	}
}

func TestWaiterAcceptsMatchingEntry(t *testing.T) {
	s := newFollowerServer(t)
	cmd, _ := Command{Op: OpPut, Key: "k", Value: "v"}.Encode()

	ch := make(chan error, 1)
	s.mu.Lock()
	s.waiters[3] = waiter{term: 2, ch: ch}
	s.mu.Unlock()

	s.Apply(raft.LogEntry{Index: 3, Term: 2, Command: cmd})

	select {
	case err := <-ch:
		if err != nil {
			t.Fatalf("waiter got %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter never fired")
	}
	// And the command really was applied to the store.
	if v, ok := s.store.Get("k"); !ok || v != "v" {
		t.Fatalf("store.Get(k) = %q/%v, want v/true", v, ok)
	}
}
