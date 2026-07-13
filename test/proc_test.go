package test

// Real-process crash testing: five actual raftd processes killed with
// SIGKILL (kill -9 — no signal handler, no graceful shutdown, no chance to
// flush) while a client keeps writing. This is the only test tier where the
// operating system, the filesystem, and torn-mid-write WAL tails are real;
// the in-process harness can only approximate a crash.
//
// Skipped under -short (builds a binary, runs ~30s of process churn).

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Abdullah-A-Qazi/RaftDB/config"
	"github.com/Abdullah-A-Qazi/RaftDB/rpc/kvpb"
)

type procCluster struct {
	t       *testing.T
	bin     string
	cfgPath string
	dataDir string
	logDir  string
	nodes   []config.Node
	procs   map[string]*exec.Cmd
	clients map[string]kvpb.KVClient
	conns   []*grpc.ClientConn
	leader  string // last known leader's client addr ("" = unknown)
	rr      int    // round-robin cursor for picking a node to ask
}

// nextAddr rotates deterministically through the nodes. (It replaced a
// `UnixNano() % n` "random" pick that was constant in practice: macOS's
// clock granularity and the retry sleep are both multiples of 5ns·k, so
// the modulo never changed and every retry hammered the same dead node.)
func (pc *procCluster) nextAddr() string {
	pc.rr++
	return pc.nodes[pc.rr%len(pc.nodes)].ClientAddr
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func newProcCluster(t *testing.T, n int) *procCluster {
	t.Helper()
	dir := t.TempDir()

	pc := &procCluster{
		t:       t,
		bin:     filepath.Join(dir, "raftd"),
		cfgPath: filepath.Join(dir, "cluster.json"),
		dataDir: filepath.Join(dir, "data"),
		logDir:  filepath.Join(dir, "logs"),
		procs:   make(map[string]*exec.Cmd),
		clients: make(map[string]kvpb.KVClient),
	}
	if err := os.MkdirAll(pc.logDir, 0o755); err != nil {
		t.Fatal(err)
	}

	build := exec.Command("go", "build", "-o", pc.bin, "github.com/Abdullah-A-Qazi/RaftDB/cmd/raftd")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building raftd: %v\n%s", err, out)
	}

	for i := 1; i <= n; i++ {
		pc.nodes = append(pc.nodes, config.Node{
			ID:         fmt.Sprintf("node%d", i),
			RaftAddr:   freePort(t),
			ClientAddr: freePort(t),
		})
	}
	cfg, err := json.Marshal(config.Cluster{Nodes: pc.nodes})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pc.cfgPath, cfg, 0o644); err != nil {
		t.Fatal(err)
	}

	for _, node := range pc.nodes {
		pc.start(node.ID)
		conn, err := grpc.NewClient(node.ClientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		pc.conns = append(pc.conns, conn)
		pc.clients[node.ClientAddr] = kvpb.NewKVClient(conn)
	}

	t.Cleanup(func() {
		for id := range pc.procs {
			pc.kill9(id)
		}
		for _, c := range pc.conns {
			c.Close()
		}
		if t.Failed() {
			pc.dumpLogs()
		}
	})
	return pc
}

// dumpLogs prints the tail of every node's log when the test failed —
// the only way to see what real child processes were doing.
func (pc *procCluster) dumpLogs() {
	for _, node := range pc.nodes {
		data, err := os.ReadFile(filepath.Join(pc.logDir, node.ID+".log"))
		if err != nil {
			continue
		}
		const tail = 3000
		if len(data) > tail {
			data = data[len(data)-tail:]
		}
		pc.t.Logf("--- %s log tail ---\n%s", node.ID, data)
	}
}

func (pc *procCluster) start(id string) {
	pc.t.Helper()
	logf, err := os.OpenFile(filepath.Join(pc.logDir, id+".log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		pc.t.Fatal(err)
	}
	cmd := exec.Command(pc.bin, "-config", pc.cfgPath, "-id", id, "-data", pc.dataDir)
	cmd.Stdout = logf
	cmd.Stderr = logf
	if err := cmd.Start(); err != nil {
		pc.t.Fatalf("starting %s: %v", id, err)
	}
	logf.Close() // the child holds its own descriptor now
	pc.procs[id] = cmd
}

// kill9 delivers SIGKILL — the process gets no opportunity to do anything.
func (pc *procCluster) kill9(id string) {
	pc.t.Helper()
	cmd, ok := pc.procs[id]
	if !ok {
		return
	}
	delete(pc.procs, id)
	_ = cmd.Process.Signal(syscall.SIGKILL)
	_ = cmd.Wait() // reap; error is expected (killed)
	pc.leader = "" // leadership may well have moved
}

// put writes through whatever node currently leads, following redirects,
// retrying across elections. Returns only after a real ack.
func (pc *procCluster) put(key, value string) error {
	deadline := time.Now().Add(20 * time.Second)
	addr := pc.leader
	var trace []string
	for time.Now().Before(deadline) {
		if addr == "" {
			addr = pc.nextAddr()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := pc.clients[addr].Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
		cancel()
		switch {
		case err != nil: // node down or wedged; try another
			trace = append(trace, fmt.Sprintf("%s: err %v", addr, err))
			addr = ""
			time.Sleep(50 * time.Millisecond)
		case resp.Redirect != nil:
			trace = append(trace, fmt.Sprintf("%s: redirect to %q", addr, resp.Redirect.LeaderAddr))
			addr = resp.Redirect.LeaderAddr // may be "" (election in progress)
			time.Sleep(20 * time.Millisecond)
		default:
			pc.leader = addr
			return nil
		}
	}
	if len(trace) > 8 {
		trace = trace[len(trace)-8:]
	}
	return fmt.Errorf("put %s: no ack within deadline; last attempts:\n%s",
		key, joinLines(trace))
}

func joinLines(lines []string) string {
	out := ""
	for _, l := range lines {
		out += "  " + l + "\n"
	}
	return out
}

func (pc *procCluster) get(key string) (string, bool, error) {
	deadline := time.Now().Add(20 * time.Second)
	addr := pc.leader
	for time.Now().Before(deadline) {
		if addr == "" {
			addr = pc.nextAddr()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := pc.clients[addr].Get(ctx, &kvpb.GetRequest{Key: key})
		cancel()
		switch {
		case err != nil:
			addr = ""
			time.Sleep(50 * time.Millisecond)
		case resp.Redirect != nil:
			addr = resp.Redirect.LeaderAddr
			time.Sleep(20 * time.Millisecond)
		default:
			pc.leader = addr
			return resp.Value, resp.Found, nil
		}
	}
	return "", false, fmt.Errorf("get %s: no answer within deadline", key)
}

// TestKill9EveryNodeAndPairsDuringWrites is the Phase 4 scenario verbatim:
// hard-kill every node one at a time, then in pairs (2 simultaneous — the
// maximum a 5-node cluster tolerates), all while writes are in flight, and
// confirm the cluster converges with every acknowledged write intact.
func TestKill9EveryNodeAndPairsDuringWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("real-process kill -9 test; skipped with -short")
	}
	pc := newProcCluster(t, 5)
	acked := make(map[string]string)
	seq := 0
	write := func(tag string) {
		key := fmt.Sprintf("w%d-%s", seq, tag)
		seq++
		if err := pc.put(key, "v-"+key); err != nil {
			t.Fatalf("%v (acked so far: %d)", err, len(acked))
		}
		acked[key] = "v-" + key
	}

	// Warm-up: cluster up, some data committed.
	for range 3 {
		write("warmup")
	}

	// Round 1: every node killed once, alone.
	for _, node := range pc.nodes {
		pc.kill9(node.ID)
		write("during-" + node.ID) // 4/5 alive: quorum holds, writes must ack
		write("during-" + node.ID)
		pc.start(node.ID)
		write("after-" + node.ID) // health barrier: cluster took the rejoin
	}

	// Round 2: pairs, killed simultaneously (each node dies in exactly two
	// pairs). 3/5 alive keeps quorum — writes must still ack, including
	// when the pair contained the leader.
	n := len(pc.nodes)
	for i := range n {
		a, b := pc.nodes[i], pc.nodes[(i+1)%n]
		pc.kill9(a.ID)
		pc.kill9(b.ID)
		write(fmt.Sprintf("during-%s-%s", a.ID, b.ID))
		pc.start(a.ID)
		pc.start(b.ID)
		write(fmt.Sprintf("after-%s-%s", a.ID, b.ID))
	}

	// Convergence + no lost committed writes: every acked key must read
	// back with its exact value.
	for key, want := range acked {
		got, found, err := pc.get(key)
		if err != nil {
			t.Fatal(err)
		}
		if !found || got != want {
			t.Errorf("acked write %s: got %q/%v, want %q — lost across kill -9", key, got, found, want)
		}
	}
	t.Logf("verified %d acked writes across 15 kill -9 crashes", len(acked))
}
