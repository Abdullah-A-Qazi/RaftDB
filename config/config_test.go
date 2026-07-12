package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validCluster() Cluster {
	return Cluster{Nodes: []Node{
		{ID: "node1", RaftAddr: "127.0.0.1:9001", ClientAddr: "127.0.0.1:8001"},
		{ID: "node2", RaftAddr: "127.0.0.1:9002", ClientAddr: "127.0.0.1:8002"},
		{ID: "node3", RaftAddr: "127.0.0.1:9003", ClientAddr: "127.0.0.1:8003"},
	}}
}

func TestLoadValidConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.json")
	data := `{
		"nodes": [
			{"id": "node1", "raft_addr": "127.0.0.1:9001", "client_addr": "127.0.0.1:8001"},
			{"id": "node2", "raft_addr": "127.0.0.1:9002", "client_addr": "127.0.0.1:8002"},
			{"id": "node3", "raft_addr": "127.0.0.1:9003", "client_addr": "127.0.0.1:8003"}
		]
	}`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(c.Nodes) != 3 {
		t.Fatalf("got %d nodes, want 3", len(c.Nodes))
	}
	if c.Nodes[1].RaftAddr != "127.0.0.1:9002" {
		t.Errorf("node2 raft_addr = %q", c.Nodes[1].RaftAddr)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadMalformedJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte(`{"nodes": [`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

func TestValidateRejections(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Cluster)
		wantErr string
	}{
		{
			name:    "empty cluster",
			mutate:  func(c *Cluster) { c.Nodes = nil },
			wantErr: "no nodes",
		},
		{
			name:    "empty id",
			mutate:  func(c *Cluster) { c.Nodes[0].ID = "" },
			wantErr: "empty id",
		},
		{
			name:    "empty raft addr",
			mutate:  func(c *Cluster) { c.Nodes[1].RaftAddr = "" },
			wantErr: "empty raft_addr",
		},
		{
			name:    "empty client addr",
			mutate:  func(c *Cluster) { c.Nodes[2].ClientAddr = "" },
			wantErr: "empty client_addr",
		},
		{
			name:    "duplicate id",
			mutate:  func(c *Cluster) { c.Nodes[2].ID = "node1" },
			wantErr: "duplicate node id",
		},
		{
			name:    "duplicate address across nodes",
			mutate:  func(c *Cluster) { c.Nodes[2].RaftAddr = c.Nodes[0].RaftAddr },
			wantErr: "used by both",
		},
		{
			name:    "raft and client addr collide on one node",
			mutate:  func(c *Cluster) { c.Nodes[0].ClientAddr = c.Nodes[0].RaftAddr },
			wantErr: "used by both",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validCluster()
			tc.mutate(&c)
			err := c.Validate()
			if err == nil {
				t.Fatalf("Validate accepted invalid config")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestNodeLookup(t *testing.T) {
	c := validCluster()
	n, ok := c.Node("node2")
	if !ok || n.RaftAddr != "127.0.0.1:9002" {
		t.Fatalf("Node(node2) = %+v, %v", n, ok)
	}
	if _, ok := c.Node("ghost"); ok {
		t.Fatal("lookup of unknown id succeeded")
	}
}

func TestPeers(t *testing.T) {
	c := validCluster()
	peers := c.Peers("node2")
	if len(peers) != 2 {
		t.Fatalf("got %d peers, want 2", len(peers))
	}
	for _, p := range peers {
		if p.ID == "node2" {
			t.Fatal("Peers included self")
		}
	}
}

func TestQuorumSize(t *testing.T) {
	cases := []struct{ nodes, want int }{
		{1, 1}, {2, 2}, {3, 2}, {4, 3}, {5, 3}, {7, 4},
	}
	for _, tc := range cases {
		c := Cluster{Nodes: make([]Node, tc.nodes)}
		if got := c.QuorumSize(); got != tc.want {
			t.Errorf("QuorumSize with %d nodes = %d, want %d", tc.nodes, got, tc.want)
		}
	}
}
