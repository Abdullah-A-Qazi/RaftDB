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

	"github.com/Abdullah-A-Qazi/RaftDB/kv"
	"github.com/Abdullah-A-Qazi/RaftDB/raft"
	"github.com/Abdullah-A-Qazi/RaftDB/rpc/kvpb"
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

	snapshotThreshold uint64 // 0 = snapshotting disabled

	mu        sync.Mutex
	nodes     map[string]*raft.RaftNode // nil entry = not running
	dirs      map[string]string         // per-node durable state, survives restarts
	connected map[string]bool
	snapsSent map[string]int // InstallSnapshot RPCs delivered, by target

	// blocked is the partition matrix (Phase 6): blocked[from][to] drops
	// every RPC from → to while leaving both processes running — a network
	// failure, not a crash. Directional on purpose: real partitions are
	// routinely asymmetric (a link that delivers one way), and Raft's
	// nastiest liveness scenarios come from exactly that.
	blocked map[string]map[string]bool

	// KV layer, one per running node. Rebuilt from scratch on restart —
	// the log replay through normal replication is what repopulates it.
	stores    map[string]*kv.Store
	kvs       map[string]*kv.Server
	recorders map[string]*applyRecorder
}

// applyRecorder wraps a node's state machine to record the order entries
// were applied in, so tests can assert every node saw the same sequence.
type applyRecorder struct {
	inner raft.StateMachine

	mu       sync.Mutex
	indexes  []uint64
	restores int
}

func (r *applyRecorder) Apply(e raft.LogEntry) {
	r.mu.Lock()
	r.indexes = append(r.indexes, e.Index)
	r.mu.Unlock()
	r.inner.Apply(e)
}

func (r *applyRecorder) Snapshot() ([]byte, error) {
	return r.inner.Snapshot()
}

func (r *applyRecorder) Restore(snapshot []byte) error {
	r.mu.Lock()
	r.restores++
	r.mu.Unlock()
	return r.inner.Restore(snapshot)
}

func (r *applyRecorder) applied() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]uint64, len(r.indexes))
	copy(out, r.indexes)
	return out
}

// clientAddr fabricates the client address a node would advertise; the
// in-process harness has no real listeners, so tests only assert the
// redirect *routing* (ID + address string), not a network hop.
func clientAddr(id string) string { return "client-addr-" + id }

// NewCluster starts n nodes (node1..nodeN) with real FileStore persistence
// in per-node temp dirs, so Restart genuinely recovers from disk.
// Snapshotting is disabled — the Phases 1–4 behavior.
func NewCluster(t *testing.T, n int) *Cluster {
	return NewSnapshottingCluster(t, n, 0)
}

// NewSnapshottingCluster is NewCluster with log compaction enabled: every
// node snapshots and compacts after `threshold` applied entries accumulate.
func NewSnapshottingCluster(t *testing.T, n int, threshold uint64) *Cluster {
	t.Helper()
	c := &Cluster{
		t:                 t,
		snapshotThreshold: threshold,
		nodes:             make(map[string]*raft.RaftNode, n),
		dirs:              make(map[string]string, n),
		connected:         make(map[string]bool, n),
		snapsSent:         make(map[string]int, n),
		blocked:           make(map[string]map[string]bool, n),
		stores:            make(map[string]*kv.Store, n),
		kvs:               make(map[string]*kv.Server, n),
		recorders:         make(map[string]*applyRecorder, n),
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
		LogStore:           store, // real WAL: harness restarts recover from disk
		SnapshotStore:      store,
		SnapshotThreshold:  c.snapshotThreshold,
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

	// KV layer for this incarnation. Fresh and empty: everything it will
	// contain arrives by replaying the replicated log.
	clientAddrs := make(map[string]string, len(c.ids))
	for _, other := range c.ids {
		clientAddrs[other] = clientAddr(other)
	}
	kvStore := kv.NewStore()
	server := kv.NewServer(node, kvStore, clientAddrs, clusterLogger())
	recorder := &applyRecorder{inner: server}
	node.SetStateMachine(recorder)

	c.mu.Lock()
	c.nodes[id] = node
	c.stores[id] = kvStore
	c.kvs[id] = server
	c.recorders[id] = recorder
	c.mu.Unlock()
	if err := node.Start(); err != nil {
		c.t.Fatalf("startNode(%s): %v", id, err)
	}
}

// KV returns a node's client-facing server (callable directly — gRPC
// methods are plain methods).
func (c *Cluster) KV(id string) *kv.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.kvs[id]
}

// Store returns a node's state machine for direct inspection.
func (c *Cluster) Store(id string) *kv.Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stores[id]
}

// Applied returns the order of indexes a node has applied (this
// incarnation).
func (c *Cluster) Applied(id string) []uint64 {
	c.mu.Lock()
	r := c.recorders[id]
	c.mu.Unlock()
	return r.applied()
}

// MustPut writes through the current leader, retrying across leadership
// changes until it succeeds or the deadline passes.
func (c *Cluster) MustPut(key, value string) {
	c.t.Helper()
	c.mustWrite("Put "+key, func(ctx context.Context, srv *kv.Server) (*kvpb.Redirect, error) {
		resp, err := srv.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
		if err != nil {
			return nil, err
		}
		return resp.Redirect, nil
	})
}

// MustDelete deletes through the current leader, with the same retry rules.
func (c *Cluster) MustDelete(key string) {
	c.t.Helper()
	c.mustWrite("Delete "+key, func(ctx context.Context, srv *kv.Server) (*kvpb.Redirect, error) {
		resp, err := srv.Delete(ctx, &kvpb.DeleteRequest{Key: key})
		if err != nil {
			return nil, err
		}
		return resp.Redirect, nil
	})
}

func (c *Cluster) mustWrite(desc string, op func(context.Context, *kv.Server) (*kvpb.Redirect, error)) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, srv := c.pickLeaderKV()
		if srv == nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		redirect, err := op(ctx, srv)
		cancel()
		if err == nil && redirect == nil {
			return // committed and applied
		}
		time.Sleep(20 * time.Millisecond) // leadership churn; retry
	}
	c.t.Fatalf("%s: no successful write within deadline", desc)
}

// GetFromLeader reads a key from the current leader's KV server.
func (c *Cluster) GetFromLeader(key string) (string, bool) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, srv := c.pickLeaderKV()
		if srv != nil {
			resp, err := srv.Get(context.Background(), &kvpb.GetRequest{Key: key})
			if err == nil && resp.Redirect == nil {
				return resp.Value, resp.Found
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("Get %s: no leader served the read within deadline", key)
	return "", false
}

// pickLeaderKV returns the KV server of the node currently believing it is
// leader (among running, connected nodes), or nil. When several nodes claim
// leadership — a stale leader on the wrong side of a partition plus the
// real one — the highest term wins: stale claimants are always behind in
// term, and asking them would just burn a proposal timeout.
func (c *Cluster) pickLeaderKV() (string, *kv.Server) {
	best := ""
	var bestTerm uint64
	for id, s := range c.Statuses() {
		if s.State == raft.Leader && s.CurrentTerm > bestTerm {
			best, bestTerm = id, s.CurrentTerm
		}
	}
	if best == "" {
		return "", nil
	}
	return best, c.KV(best)
}

// LeadersAmong returns which of the given nodes currently claim leadership.
func (c *Cluster) LeadersAmong(ids []string) []string {
	var out []string
	for _, id := range ids {
		c.mu.Lock()
		node := c.nodes[id]
		c.mu.Unlock()
		if node != nil && node.Status().State == raft.Leader {
			out = append(out, id)
		}
	}
	return out
}

// WaitForLeaderAmong waits until exactly one of the given nodes claims
// leadership and returns it — the per-side form of WaitForLeader for use
// during partitions.
func (c *Cluster) WaitForLeaderAmong(ids []string, timeout time.Duration) string {
	c.t.Helper()
	var last []string
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		last = c.LeadersAmong(ids)
		if len(last) == 1 {
			return last[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no single leader among %v within %v (leaders: %v)", ids, timeout, last)
	return ""
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

// BlockLink drops all RPCs sent from → to (one direction only). Both nodes
// stay up and all their other links keep working.
func (c *Cluster) BlockLink(from, to string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blockLinkLocked(from, to)
}

func (c *Cluster) blockLinkLocked(from, to string) {
	if c.blocked[from] == nil {
		c.blocked[from] = make(map[string]bool)
	}
	c.blocked[from][to] = true
}

// UnblockLink restores one direction of one link.
func (c *Cluster) UnblockLink(from, to string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.blocked[from], to)
}

// Partition splits the cluster in two: the given nodes on one side,
// everyone else on the other. All links across the cut are blocked in both
// directions; links within each side are untouched. Replaces any previous
// partition (call Heal first for a clean slate if composing manually).
func (c *Cluster) Partition(side ...string) {
	inSide := make(map[string]bool, len(side))
	for _, id := range side {
		inSide[id] = true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocked = make(map[string]map[string]bool, len(c.ids))
	for _, a := range c.ids {
		for _, b := range c.ids {
			if a != b && inSide[a] != inSide[b] {
				c.blockLinkLocked(a, b)
			}
		}
	}
}

// Heal removes every link block (partitions only — crashed/disconnected
// nodes stay as they are).
func (c *Cluster) Heal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocked = make(map[string]map[string]bool, len(c.ids))
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
	if t.c.blocked[t.from][to] {
		return nil, fmt.Errorf("memtransport: link %s -> %s is partitioned", t.from, to)
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

func (t *memTransport) InstallSnapshot(ctx context.Context, to string, args raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	node, err := t.target(to)
	if err != nil {
		return raft.InstallSnapshotReply{}, err
	}
	t.c.mu.Lock()
	t.c.snapsSent[to]++
	t.c.mu.Unlock()
	return node.HandleInstallSnapshot(args), nil
}

// SnapshotsDeliveredTo reports how many InstallSnapshot RPCs a node has
// received — the assertion that catch-up went via snapshot, not entries.
func (c *Cluster) SnapshotsDeliveredTo(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapsSent[id]
}
