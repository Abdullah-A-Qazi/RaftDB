// raftctl is the demo/ops CLI for a RaftDB cluster: key-value operations
// with automatic leader redirection, and a per-node cluster status view.
//
//	raftctl [-cluster cluster.json] put <key> <value>
//	raftctl [-cluster cluster.json] get <key>
//	raftctl [-cluster cluster.json] delete <key>
//	raftctl [-cluster cluster.json] status [-watch]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Abdullah-A-Qazi/RaftDB/config"
	"github.com/Abdullah-A-Qazi/RaftDB/rpc/kvpb"
)

func main() {
	clusterPath := flag.String("cluster", "cluster.json", "path to the cluster config file")
	timeout := flag.Duration("timeout", 5*time.Second, "overall deadline for a command")
	flag.Parse()

	if flag.NArg() < 1 {
		usage()
	}

	cluster, err := config.Load(*clusterPath)
	if err != nil {
		fatal("%v", err)
	}
	cli := &client{cluster: cluster, conns: map[string]*grpc.ClientConn{}}
	defer cli.close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch cmd, args := flag.Arg(0), flag.Args()[1:]; cmd {
	case "put":
		if len(args) != 2 {
			usage()
		}
		if err := cli.put(ctx, args[0], args[1]); err != nil {
			fatal("put: %v", err)
		}
		fmt.Println("OK")
	case "get":
		if len(args) != 1 {
			usage()
		}
		value, found, err := cli.get(ctx, args[0])
		if err != nil {
			fatal("get: %v", err)
		}
		if !found {
			fatal("(not found)")
		}
		fmt.Println(value)
	case "delete":
		if len(args) != 1 {
			usage()
		}
		if err := cli.delete(ctx, args[0]); err != nil {
			fatal("delete: %v", err)
		}
		fmt.Println("OK")
	case "status":
		watch := len(args) == 1 && args[0] == "-watch"
		if len(args) > 0 && !watch {
			usage()
		}
		if watch {
			for {
				fmt.Print("\033[H\033[2J") // clear terminal
				cli.printStatus()
				fmt.Printf("\n(refreshing every second — Ctrl-C to stop)\n")
				time.Sleep(time.Second)
			}
		}
		cli.printStatus()
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: raftctl [-cluster cluster.json] <command>

commands:
  put <key> <value>   write through the leader (follows redirects)
  get <key>           read from the leader
  delete <key>        delete through the leader
  status [-watch]     per-node cluster state (leader, term, log, commit)
`)
	os.Exit(2)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

// client talks to the cluster's client_addr ports, following leader
// redirects and rotating across nodes when the current target is down.
type client struct {
	cluster *config.Cluster
	conns   map[string]*grpc.ClientConn
	leader  string // last known leader's client addr
	rr      int
}

func (c *client) close() {
	for _, conn := range c.conns {
		conn.Close()
	}
}

func (c *client) kv(addr string) (kvpb.KVClient, error) {
	if conn, ok := c.conns[addr]; ok {
		return kvpb.NewKVClient(conn), nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return kvpb.NewKVClient(conn), nil
}

func (c *client) nextAddr() string {
	c.rr++
	return c.cluster.Nodes[c.rr%len(c.cluster.Nodes)].ClientAddr
}

// do runs one redirect-following attempt loop. op returns the redirect (nil
// on success) so the loop can retarget.
func (c *client) do(ctx context.Context, op func(kvpb.KVClient) (*kvpb.Redirect, error)) error {
	addr := c.leader
	var lastErr error
	for ctx.Err() == nil {
		if addr == "" {
			addr = c.nextAddr()
		}
		kvc, err := c.kv(addr)
		if err != nil {
			lastErr, addr = err, ""
			continue
		}
		redirect, err := op(kvc)
		switch {
		case err != nil: // node down/wedged: try the next one
			lastErr, addr = err, ""
			time.Sleep(100 * time.Millisecond)
		case redirect != nil: // not the leader: go where it points
			lastErr = fmt.Errorf("no leader elected yet")
			addr = redirect.LeaderAddr
			time.Sleep(50 * time.Millisecond)
		default:
			c.leader = addr
			return nil
		}
	}
	if lastErr == nil {
		lastErr = ctx.Err()
	}
	return fmt.Errorf("%w (deadline %v)", lastErr, ctx.Err())
}

func (c *client) put(ctx context.Context, key, value string) error {
	return c.do(ctx, func(kvc kvpb.KVClient) (*kvpb.Redirect, error) {
		resp, err := kvc.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
		if err != nil {
			return nil, err
		}
		return resp.Redirect, nil
	})
}

func (c *client) get(ctx context.Context, key string) (value string, found bool, err error) {
	err = c.do(ctx, func(kvc kvpb.KVClient) (*kvpb.Redirect, error) {
		resp, err := kvc.Get(ctx, &kvpb.GetRequest{Key: key})
		if err != nil {
			return nil, err
		}
		if resp.Redirect == nil {
			value, found = resp.Value, resp.Found
		}
		return resp.Redirect, nil
	})
	return value, found, err
}

func (c *client) delete(ctx context.Context, key string) error {
	return c.do(ctx, func(kvc kvpb.KVClient) (*kvpb.Redirect, error) {
		resp, err := kvc.Delete(ctx, &kvpb.DeleteRequest{Key: key})
		if err != nil {
			return nil, err
		}
		return resp.Redirect, nil
	})
}

// printStatus asks every node about itself and renders the cluster table.
// Each row is that node's OWN view — disagreement between rows (during an
// election, say) is real information, not a rendering bug.
func (c *client) printStatus() {
	type row struct {
		node config.Node
		resp *kvpb.StatusResponse
		err  error
	}
	rows := make([]row, len(c.cluster.Nodes))
	for i, node := range c.cluster.Nodes {
		rows[i].node = node
		kvc, err := c.kv(node.ClientAddr)
		if err != nil {
			rows[i].err = err
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		rows[i].resp, rows[i].err = kvc.Status(ctx, &kvpb.StatusRequest{})
		cancel()
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].node.ID < rows[j].node.ID })

	fmt.Printf("%-8s %-10s %5s  %-8s %7s %8s  %-11s %5s\n",
		"NODE", "STATE", "TERM", "LEADER", "COMMIT", "APPLIED", "LOG", "KEYS")
	for _, r := range rows {
		if r.err != nil {
			fmt.Printf("%-8s %-10s\n", r.node.ID, "DOWN")
			continue
		}
		s := r.resp
		leader := s.LeaderId
		if leader == "" {
			leader = "-"
		}
		logSpan := fmt.Sprintf("[%d..%d]", s.FirstLogIndex, s.LastLogIndex)
		if s.LastLogIndex < s.FirstLogIndex {
			logSpan = fmt.Sprintf("(snap@%d)", s.LastLogIndex)
		}
		fmt.Printf("%-8s %-10s %5d  %-8s %7d %8d  %-11s %5d\n",
			s.NodeId, s.State, s.CurrentTerm, leader,
			s.CommitIndex, s.LastApplied, logSpan, s.Keys)
	}
}
