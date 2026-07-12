// raftd is the RaftDB server daemon: one process per cluster node.
//
// Phase 0 only wires up config loading and node construction so the scaffold
// is runnable end to end; Phase 1 adds the gRPC server and election loop.
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/Abdullah-A-Qazi/RaftDB/config"
	"github.com/Abdullah-A-Qazi/RaftDB/raft"
)

func main() {
	configPath := flag.String("config", "cluster.json", "path to the cluster config file")
	nodeID := flag.String("id", "", "this node's ID (must appear in the config)")
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("raftd: -id is required")
	}

	cluster, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("raftd: %v", err)
	}

	self, ok := cluster.Node(*nodeID)
	if !ok {
		log.Fatalf("raftd: node id %q not found in %s", *nodeID, *configPath)
	}

	peers := cluster.Peers(self.ID)
	peerIDs := make([]string, len(peers))
	for i, p := range peers {
		peerIDs[i] = p.ID
	}

	node := raft.NewNode(self.ID, peerIDs)
	status := node.Status()

	fmt.Printf("raftd %s\n", self.ID)
	fmt.Printf("  raft addr:   %s\n", self.RaftAddr)
	fmt.Printf("  client addr: %s\n", self.ClientAddr)
	fmt.Printf("  cluster:     %d nodes, quorum %d\n", len(cluster.Nodes), cluster.QuorumSize())
	fmt.Printf("  peers:       %v\n", peerIDs)
	fmt.Printf("  state:       %s (term %d)\n", status.State, status.CurrentTerm)
	fmt.Println("Phase 0 scaffold: no server started yet — elections arrive in Phase 1.")
}
