package raftpb

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

// Sanity check that the generated code round-trips a representative message
// — catches a broken or stale codegen setup before it can confuse a later
// phase.
func TestAppendEntriesRoundTrip(t *testing.T) {
	in := &AppendEntriesRequest{
		Term:         7,
		LeaderId:     "node3",
		PrevLogIndex: 41,
		PrevLogTerm:  6,
		Entries: []*LogEntry{
			{Term: 7, Index: 42, Command: []byte("put:k=v")},
			{Term: 7, Index: 43, Command: []byte("del:k")},
		},
		LeaderCommit: 40,
	}

	data, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := &AppendEntriesRequest{}
	if err := proto.Unmarshal(data, out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("round trip mismatch:\n in: %v\nout: %v", in, out)
	}
	if out.Entries[1].Index != 43 {
		t.Errorf("entry index = %d, want 43", out.Entries[1].Index)
	}
}
