package rpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Abdullah-A-Qazi/RaftDB/raft"
	"github.com/Abdullah-A-Qazi/RaftDB/rpc/raftpb"
)

// Transport implements raft.Transport over gRPC. One client connection per
// peer, created up front: grpc.NewClient does not actually dial until the
// first RPC, and the connection self-heals with backoff after failures, so
// peer crashes/restarts need no handling here — a down peer just surfaces
// as per-RPC errors, which the raft layer already treats as "no reply".
type Transport struct {
	clients map[string]raftpb.RaftClient
	conns   []*grpc.ClientConn
}

// NewTransport builds a transport for the given peerID -> raft address map.
// TLS is out of scope for this project (local clusters only); connections
// are plaintext.
func NewTransport(peerAddrs map[string]string) (*Transport, error) {
	t := &Transport{clients: make(map[string]raftpb.RaftClient, len(peerAddrs))}
	for id, addr := range peerAddrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Close()
			return nil, fmt.Errorf("rpc: creating client for %s (%s): %w", id, addr, err)
		}
		t.conns = append(t.conns, conn)
		t.clients[id] = raftpb.NewRaftClient(conn)
	}
	return t, nil
}

func (t *Transport) RequestVote(ctx context.Context, peerID string, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	c, ok := t.clients[peerID]
	if !ok {
		return raft.RequestVoteReply{}, fmt.Errorf("rpc: unknown peer %q", peerID)
	}
	resp, err := c.RequestVote(ctx, &raftpb.RequestVoteRequest{
		Term:         args.Term,
		CandidateId:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return raft.RequestVoteReply{}, err
	}
	return raft.RequestVoteReply{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (t *Transport) AppendEntries(ctx context.Context, peerID string, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	c, ok := t.clients[peerID]
	if !ok {
		return raft.AppendEntriesReply{}, fmt.Errorf("rpc: unknown peer %q", peerID)
	}
	resp, err := c.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term:         args.Term,
		LeaderId:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      entriesToProto(args.Entries),
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return raft.AppendEntriesReply{}, err
	}
	return raft.AppendEntriesReply{Term: resp.Term, Success: resp.Success}, nil
}

// Close tears down all peer connections.
func (t *Transport) Close() {
	for _, conn := range t.conns {
		_ = conn.Close()
	}
}
