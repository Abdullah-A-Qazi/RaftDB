// Package kv is the key-value layer on top of Raft: the command encoding,
// the replicated state machine (Store), and the client-facing gRPC service
// (Server) with leader redirection.
package kv

import (
	"encoding/json"
	"fmt"
)

// Op is a state-machine operation. Get is deliberately NOT an Op: reads
// don't go through the log (see Server.Get), so only mutations are encoded.
type Op string

const (
	OpPut    Op = "put"
	OpDelete Op = "delete"
)

// Command is what actually gets replicated in a Raft log entry. Encoded as
// JSON: a few bytes of overhead per entry buys human-readable logs and
// zero encoding code — the right trade for a learning project (protobuf or
// a hand-rolled binary format would be the production choice).
type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// Encode serializes a command for Propose.
func (c Command) Encode() ([]byte, error) {
	if c.Op != OpPut && c.Op != OpDelete {
		return nil, fmt.Errorf("kv: unknown op %q", c.Op)
	}
	return json.Marshal(c)
}

// DecodeCommand parses a replicated command.
func DecodeCommand(data []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return Command{}, fmt.Errorf("kv: corrupt command: %w", err)
	}
	if c.Op != OpPut && c.Op != OpDelete {
		return Command{}, fmt.Errorf("kv: unknown op %q", c.Op)
	}
	return c, nil
}
