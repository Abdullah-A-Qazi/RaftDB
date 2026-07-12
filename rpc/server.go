// Package rpc is the gRPC boundary: it converts between the wire types in
// rpc/raftpb and the domain types in package raft, in both directions —
// Server adapts incoming RPCs onto a node's handlers, Transport carries a
// node's outgoing RPCs to its peers.
package rpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Abdullah-A-Qazi/RaftDB/raft"
	"github.com/Abdullah-A-Qazi/RaftDB/rpc/raftpb"
)

// Server exposes a RaftNode's RPC handlers as the raftpb.Raft gRPC service.
type Server struct {
	raftpb.UnimplementedRaftServer
	node *raft.RaftNode
}

func NewServer(node *raft.RaftNode) *Server {
	return &Server{node: node}
}

func (s *Server) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	reply := s.node.HandleRequestVote(raft.RequestVoteArgs{
		Term:         req.Term,
		CandidateID:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	return &raftpb.RequestVoteResponse{
		Term:        reply.Term,
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (s *Server) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	reply := s.node.HandleAppendEntries(raft.AppendEntriesArgs{
		Term:         req.Term,
		LeaderID:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entriesFromProto(req.Entries),
		LeaderCommit: req.LeaderCommit,
	})
	return &raftpb.AppendEntriesResponse{
		Term:    reply.Term,
		Success: reply.Success,
	}, nil
}

func (s *Server) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "InstallSnapshot arrives in Phase 5")
}

func entriesFromProto(in []*raftpb.LogEntry) []raft.LogEntry {
	if len(in) == 0 {
		return nil
	}
	out := make([]raft.LogEntry, len(in))
	for i, e := range in {
		out[i] = raft.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command}
	}
	return out
}

func entriesToProto(in []raft.LogEntry) []*raftpb.LogEntry {
	if len(in) == 0 {
		return nil
	}
	out := make([]*raftpb.LogEntry, len(in))
	for i, e := range in {
		out[i] = &raftpb.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command}
	}
	return out
}
