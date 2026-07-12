// Package config loads and validates the static cluster configuration.
//
// For now cluster membership is fixed at startup: every node reads the same
// JSON file listing all members. Dynamic membership changes (the joint
// consensus protocol from §6 of the Raft paper) are out of scope and noted
// as future work.
package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Node describes a single cluster member.
type Node struct {
	// ID uniquely identifies the node within the cluster. It is what goes
	// into votedFor and leader_id fields, so it must never be reused for a
	// different machine within the life of a cluster.
	ID string `json:"id"`

	// RaftAddr is the host:port where the node serves Raft RPCs
	// (RequestVote/AppendEntries/InstallSnapshot) to its peers.
	RaftAddr string `json:"raft_addr"`

	// ClientAddr is the host:port where the node serves client requests
	// (Put/Get/Delete). Kept separate from RaftAddr so that client load
	// never competes with heartbeat traffic on the same listener, and so
	// tests can partition the two independently.
	ClientAddr string `json:"client_addr"`
}

// Cluster is the full static membership list. Every node must be started
// with an identical copy: quorum size is derived from len(Nodes), so two
// nodes disagreeing about membership could each believe in a different
// majority — the exact split-brain Raft exists to prevent.
type Cluster struct {
	Nodes []Node `json:"nodes"`
}

// Load reads and validates a cluster config from a JSON file.
func Load(path string) (*Cluster, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	var c Cluster
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parsing config %s: %w", path, err)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return &c, nil
}

// Validate checks the structural invariants the rest of the system assumes.
func (c *Cluster) Validate() error {
	if len(c.Nodes) == 0 {
		return fmt.Errorf("cluster has no nodes")
	}
	seenIDs := make(map[string]bool, len(c.Nodes))
	seenAddrs := make(map[string]string, len(c.Nodes)*2)
	for i, n := range c.Nodes {
		if n.ID == "" {
			return fmt.Errorf("node at index %d has empty id", i)
		}
		if n.RaftAddr == "" {
			return fmt.Errorf("node %q has empty raft_addr", n.ID)
		}
		if n.ClientAddr == "" {
			return fmt.Errorf("node %q has empty client_addr", n.ID)
		}
		if seenIDs[n.ID] {
			return fmt.Errorf("duplicate node id %q", n.ID)
		}
		seenIDs[n.ID] = true
		for _, addr := range []string{n.RaftAddr, n.ClientAddr} {
			if owner, taken := seenAddrs[addr]; taken {
				return fmt.Errorf("address %q used by both %q and %q", addr, owner, n.ID)
			}
			seenAddrs[addr] = n.ID
		}
	}
	return nil
}

// Node returns the config entry for the given ID.
func (c *Cluster) Node(id string) (Node, bool) {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return Node{}, false
}

// Peers returns every node except selfID, i.e. the set a node sends
// RequestVote and AppendEntries to.
func (c *Cluster) Peers(selfID string) []Node {
	peers := make([]Node, 0, len(c.Nodes)-1)
	for _, n := range c.Nodes {
		if n.ID != selfID {
			peers = append(peers, n)
		}
	}
	return peers
}

// QuorumSize returns the number of nodes that constitutes a majority.
// For 5 nodes this is 3; for 4 nodes it is also 3 (a majority must be a
// strict majority — this is why even-sized clusters buy no extra fault
// tolerance over the next-smaller odd size).
func (c *Cluster) QuorumSize() int {
	return len(c.Nodes)/2 + 1
}
