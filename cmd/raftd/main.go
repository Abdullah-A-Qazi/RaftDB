// raftd is the RaftDB server daemon: one process per cluster node.
//
// Phase 1: serves Raft RPCs on raft_addr and participates in leader
// election. The client-facing KV API (on client_addr) arrives in Phase 2.
package main

import (
	"flag"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"google.golang.org/grpc"

	"github.com/Abdullah-A-Qazi/RaftDB/config"
	"github.com/Abdullah-A-Qazi/RaftDB/raft"
	"github.com/Abdullah-A-Qazi/RaftDB/rpc"
	"github.com/Abdullah-A-Qazi/RaftDB/rpc/raftpb"
	"github.com/Abdullah-A-Qazi/RaftDB/storage"
)

func main() {
	configPath := flag.String("config", "cluster.json", "path to the cluster config file")
	nodeID := flag.String("id", "", "this node's ID (must appear in the config)")
	dataDir := flag.String("data", "data", "base directory for per-node durable state")
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
	peerAddrs := make(map[string]string, len(peers))
	for i, p := range peers {
		peerIDs[i] = p.ID
		peerAddrs[p.ID] = p.RaftAddr
	}

	// Each node's durable state lives in <data>/<id> so multiple nodes can
	// run out of one working directory for local demos.
	store, err := storage.NewFileStore(filepath.Join(*dataDir, self.ID))
	if err != nil {
		log.Fatalf("raftd: %v", err)
	}

	transport, err := rpc.NewTransport(peerAddrs)
	if err != nil {
		log.Fatalf("raftd: %v", err)
	}
	defer transport.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	node, err := raft.NewNode(raft.Config{
		ID:        self.ID,
		Peers:     peerIDs,
		Store:     store,
		Transport: transport,
		Logger:    logger,
	})
	if err != nil {
		log.Fatalf("raftd: %v", err)
	}

	lis, err := net.Listen("tcp", self.RaftAddr)
	if err != nil {
		log.Fatalf("raftd: listening on %s: %v", self.RaftAddr, err)
	}
	grpcServer := grpc.NewServer()
	raftpb.RegisterRaftServer(grpcServer, rpc.NewServer(node))

	// Serve RPCs before starting the election timer so a peer's very first
	// RequestVote can't hit a not-yet-listening socket after we ourselves
	// have already begun counting down.
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("raftd: grpc serve: %v", err)
		}
	}()
	if err := node.Start(); err != nil {
		log.Fatalf("raftd: %v", err)
	}
	logger.Info("raftd started",
		"node", self.ID,
		"raftAddr", self.RaftAddr,
		"clusterSize", len(cluster.Nodes),
		"quorum", cluster.QuorumSize(),
	)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Info("shutting down")
	node.Stop()
	grpcServer.GracefulStop()
}
