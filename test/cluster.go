// Package test holds the multi-node integration harness: an in-process
// cluster whose nodes talk over an in-memory transport that the test can
// sever per node ("crash"/partition) and reconnect. Phase 6 grows this into
// per-link partitions; Phase 1 only needs whole-node connect/disconnect.
package test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Abdullah-A-Qazi/RaftDB/raft"
	"github.com/Abdullah-A-Qazi/RaftDB/storage"
)

// Test timing: fast enough that scenarios finish in a couple of seconds,
// with heartbeat << election-min so healthy leaders are never falsely
// suspected even on a loaded CI machine.
const (
	electionMin = 150 * time.Millisecond
	electionMax = 300 * time.Millisecond
	heartbeat   = 30 * time.Millisecond
	rpcTimeout  = 50 * time.Millisecond
)

// Cluster is an in-process Raft cluster for tests.
type Cluster struct {
	t   *testing.T
	ids []string

	mu        sync.Mutex
	nodes     map[string]*raft.RaftNode // nil entry = not running
	dirs      map[string]string         // per-node durable state, survives restarts
	connected map[string]bool
}

// NewCluster starts n nodes (node1..nodeN) with real FileStore persistence
// in per-node temp dirs, so Restart genuinely recovers from disk.
func NewCluster(t *testing.T, n int) *Cluster {
	t.Helper()
	c := &Cluster{
		t:         t,
		nodes:     make(map[string]*raft.RaftNode, n),
		dirs:      make(map[string]string, n),
		connected: make(map[string]bool, n),
	}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("node%d", i)
		c.ids = append(c.ids, id)
		c.dirs[id] = t.TempDir()
		c.connected[id] = true
	}
	for _, id := range c.ids {
		c.startNode(id)
	}
	t.Cleanup(c.Shutdown)
	return c
}

func (c *Cluster) startNode(id string) {
	c.t.Helper()
	store, err := storage.NewFileStore(c.dirs[id])
	if err != nil {
		c.t.Fatalf("startNode(%s): %v", id, err)
	}
	var peers []string
	for _, other := range c.ids {
		if other != id {
			peers = append(peers, other)
		}
	}
	node, err := raft.NewNode(raft.Config{
		ID:                 id,
		Peers:              peers,
		Store:              store,
		Transport:          &memTransport{c: c, from: id},
		ElectionTimeoutMin: electionMin,
		ElectionTimeoutMax: electionMax,
		HeartbeatInterval:  heartbeat,
		RPCTimeout:         rpcTimeout,
		Logger:             clusterLogger(),
	})
	if err != nil {
		c.t.Fatalf("startNode(%s): %v", id, err)
	}
	c.mu.Lock()
	c.nodes[id] = node
	c.mu.Unlock()
	if err := node.Start(); err != nil {
		c.t.Fatalf("startNode(%s): %v", id, err)
	}
}

// Stop simulates a node crash: it drops off the network and its process
// state (everything but the FileStore's files) is gone.
func (c *Cluster) Stop(id string) {
	c.t.Helper()
	c.mu.Lock()
	node := c.nodes[id]
	c.nodes[id] = nil
	c.connected[id] = false
	c.mu.Unlock()
	// Outside c.mu: Stop waits on goroutines that may be inside the
	// transport, which locks c.mu.
	if node != nil {
		node.Stop()
	}
}

// Restart brings a stopped node back with a fresh process state recovered
// from its on-disk store.
func (c *Cluster) Restart(id string) {
	c.t.Helper()
	c.mu.Lock()
	if c.nodes[id] != nil {
		c.mu.Unlock()
		c.t.Fatalf("Restart(%s): still running", id)
	}
	c.connected[id] = true
	c.mu.Unlock()
	c.startNode(id)
}

// Disconnect cuts a running node off from everyone (both directions)
// without stopping it; Reconnect undoes it.
func (c *Cluster) Disconnect(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected[id] = false
}

func (c *Cluster) Reconnect(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected[id] = true
}

// Shutdown stops every running node.
func (c *Cluster) Shutdown() {
	for _, id := range c.ids {
		c.Stop(id)
	}
}

// Statuses returns the status of every *running and connected* node.
func (c *Cluster) Statuses() map[string]raft.Status {
	c.mu.Lock()
	running := make(map[string]*raft.RaftNode)
	for id, n := range c.nodes {
		if n != nil && c.connected[id] {
			running[id] = n
		}
	}
	c.mu.Unlock()
	out := make(map[string]raft.Status, len(running))
	for id, n := range running {
		out[id] = n.Status()
	}
	return out
}

// Status returns one node's status; the node must be running.
func (c *Cluster) Status(id string) raft.Status {
	c.t.Helper()
	c.mu.Lock()
	node := c.nodes[id]
	c.mu.Unlock()
	if node == nil {
		c.t.Fatalf("Status(%s): not running", id)
	}
	return node.Status()
}

// leaders returns the IDs of connected running nodes in the Leader state.
func (c *Cluster) leaders() []string {
	var out []string
	for id, s := range c.Statuses() {
		if s.State == raft.Leader {
			out = append(out, id)
		}
	}
	return out
}

// WaitForLeader waits until the connected part of the cluster has exactly
// one leader and returns its ID.
func (c *Cluster) WaitForLeader(timeout time.Duration) string {
	c.t.Helper()
	var last []string
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		last = c.leaders()
		if len(last) == 1 {
			return last[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no single leader within %v (leaders: %v)", timeout, last)
	return ""
}

// WaitForAgreement waits until every connected node agrees: one leader,
// everyone in its term, every follower pointing at it. Returns the leader.
func (c *Cluster) WaitForAgreement(timeout time.Duration) string {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if id, ok := c.agreement(); ok {
			return id
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("cluster did not converge within %v (statuses: %+v)", timeout, c.Statuses())
	return ""
}

func (c *Cluster) agreement() (string, bool) {
	statuses := c.Statuses()
	leader := ""
	for id, s := range statuses {
		if s.State == raft.Leader {
			if leader != "" {
				return "", false // two leaders visible right now
			}
			leader = id
		}
	}
	if leader == "" {
		return "", false
	}
	term := statuses[leader].CurrentTerm
	for id, s := range statuses {
		if id == leader {
			continue
		}
		if s.State != raft.Follower || s.CurrentTerm != term || s.LeaderID != leader {
			return "", false
		}
	}
	return leader, true
}

// waitFor polls cond until it holds or the test fails.
func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", timeout, msg)
}

func clusterLogger() *slog.Logger {
	w := io.Writer(io.Discard)
	if testing.Verbose() {
		w = os.Stderr
	}
	return slog.New(slog.NewTextHandler(w, nil))
}

// memTransport routes RPCs by directly invoking the target node's handler.
// An RPC goes through only if both endpoints are currently connected —
// matching how a real network fails (neither direction works).
type memTransport struct {
	c    *Cluster
	from string
}

func (t *memTransport) target(to string) (*raft.RaftNode, error) {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	if !t.c.connected[t.from] {
		return nil, fmt.Errorf("memtransport: %s is disconnected", t.from)
	}
	if !t.c.connected[to] {
		return nil, fmt.Errorf("memtransport: %s is disconnected", to)
	}
	node := t.c.nodes[to]
	if node == nil {
		return nil, fmt.Errorf("memtransport: %s is down", to)
	}
	return node, nil
}

func (t *memTransport) RequestVote(ctx context.Context, to string, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	node, err := t.target(to)
	if err != nil {
		return raft.RequestVoteReply{}, err
	}
	return node.HandleRequestVote(args), nil
}

func (t *memTransport) AppendEntries(ctx context.Context, to string, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	node, err := t.target(to)
	if err != nil {
		return raft.AppendEntriesReply{}, err
	}
	return node.HandleAppendEntries(args), nil
}
