package kv

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Abdullah-A-Qazi/RaftDB/raft"
	"github.com/Abdullah-A-Qazi/RaftDB/rpc/kvpb"
)

// defaultProposeTimeout bounds how long a Put/Delete waits for commitment
// when the client supplied no deadline of its own. Generous: commitment
// normally takes one RPC round trip, so hitting this means quorum is lost.
const defaultProposeTimeout = 5 * time.Second

// Server is the client-facing KV service on one node. It is also the
// node's raft.StateMachine: committed commands flow in through Apply, both
// to mutate the Store and to unblock the client requests waiting on them.
type Server struct {
	kvpb.UnimplementedKVServer

	node        *raft.RaftNode
	store       *Store
	clientAddrs map[string]string // nodeID -> client_addr, for redirects
	logger      *slog.Logger

	// waiters maps a proposed log index to the request goroutine waiting
	// for it to be applied. Only ever populated on (what believes it is)
	// the leader; keyed by index alone because a leader proposes each
	// index at most once — the term check happens at fire time.
	mu      sync.Mutex
	waiters map[uint64]waiter
}

type waiter struct {
	term uint64 // term the entry was proposed in
	ch   chan error
}

// NewServer builds the KV service. Wire it to the node with
// node.SetStateMachine(server) before node.Start().
func NewServer(node *raft.RaftNode, store *Store, clientAddrs map[string]string, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		node:        node,
		store:       store,
		clientAddrs: clientAddrs,
		logger:      logger,
		waiters:     make(map[uint64]waiter),
	}
}

// Apply implements raft.StateMachine: called once per committed entry, in
// log order, from Raft's single applier goroutine.
func (s *Server) Apply(entry raft.LogEntry) {
	cmd, err := DecodeCommand(entry.Command)
	if err != nil {
		// A committed-but-undecodable command means a bug on the proposing
		// node, and every replica sees it identically. Skipping it keeps
		// determinism (everyone skips the same entry); crashing the whole
		// cluster over it would be the alternative.
		s.logger.Error("skipping undecodable committed command", "index", entry.Index, "err", err)
	} else if err := s.store.apply(cmd); err != nil {
		s.logger.Error("apply failed", "index", entry.Index, "err", err)
	}
	s.fireWaiter(entry)
}

// fireWaiter completes the client request (if any) parked on this index.
//
// The term comparison is the whole trick: Propose returned (index, term),
// and commitment of that exact pair is the only proof the command survived.
// If leadership changed, a different leader's entry can occupy the same
// index — the command this client sent is gone, and it must hear an error,
// not a false OK.
func (s *Server) fireWaiter(entry raft.LogEntry) {
	s.mu.Lock()
	w, ok := s.waiters[entry.Index]
	if ok {
		delete(s.waiters, entry.Index)
	}
	s.mu.Unlock()
	if !ok {
		return
	}
	if w.term == entry.Term {
		w.ch <- nil
	} else {
		w.ch <- raft.ErrNotLeader // superseded by another leader's entry
	}
}

// propose replicates one command and waits until it is committed and
// applied (or fails/times out).
func (s *Server) propose(ctx context.Context, cmd Command) error {
	data, err := cmd.Encode()
	if err != nil {
		return err
	}
	index, term, err := s.node.Propose(data)
	if err != nil {
		return err
	}

	ch := make(chan error, 1) // buffered: fireWaiter must never block Apply
	s.mu.Lock()
	if old, ok := s.waiters[index]; ok {
		// Same index proposed twice can happen across two leaderships of
		// this node (uncommitted tail truncated in between). The old
		// entry was superseded — fail its client immediately.
		old.ch <- raft.ErrNotLeader
	}
	s.waiters[index] = waiter{term: term, ch: ch}
	s.mu.Unlock()

	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultProposeTimeout)
		defer cancel()
	}
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		// Give up waiting, but the entry may still commit later — the
		// client cannot distinguish "lost" from "slow" (see the no-dedup
		// caveat in docs/phase-2). Drop the waiter so it doesn't leak.
		s.mu.Lock()
		delete(s.waiters, index)
		s.mu.Unlock()
		return ctx.Err()
	}
}

// redirect builds the not-the-leader response hint. Empty addr means "no
// leader known right now, back off and retry".
func (s *Server) redirect() *kvpb.Redirect {
	leaderID := s.node.Status().LeaderID
	return &kvpb.Redirect{
		LeaderId:   leaderID,
		LeaderAddr: s.clientAddrs[leaderID], // "" when leaderID unknown
	}
}

// --- gRPC methods ---

func (s *Server) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	err := s.propose(ctx, Command{Op: OpPut, Key: req.Key, Value: req.Value})
	if err == raft.ErrNotLeader {
		return &kvpb.PutResponse{Redirect: s.redirect()}, nil
	}
	if err != nil {
		return nil, err
	}
	return &kvpb.PutResponse{}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	err := s.propose(ctx, Command{Op: OpDelete, Key: req.Key})
	if err == raft.ErrNotLeader {
		return &kvpb.DeleteResponse{Redirect: s.redirect()}, nil
	}
	if err != nil {
		return nil, err
	}
	return &kvpb.DeleteResponse{}, nil
}

// Get serves reads from the leader's local store, without going through
// the log.
//
// Consistency caveat (deviation, flagged): this is NOT strictly
// linearizable. A leader that has just been deposed by a partition it
// can't see may serve a value that a newer leader has already overwritten.
// The fixes — ReadIndex (§6.4 of Ongaro's thesis) or leader leases — are
// future work; routing reads through the log would also fix it at the cost
// of log growth. Reads on a *stable* leader do reflect all committed
// writes, because a command is only acknowledged after the leader applies
// it (see the Phase 2 test "write visible on leader after commit").
func (s *Server) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if s.node.Status().State != raft.Leader {
		return &kvpb.GetResponse{Redirect: s.redirect()}, nil
	}
	v, found := s.store.Get(req.Key)
	return &kvpb.GetResponse{Value: v, Found: found}, nil
}
