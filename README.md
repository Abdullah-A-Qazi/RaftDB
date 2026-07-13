# RaftDB

A distributed, fault-tolerant key-value store built on a **from-scratch implementation of the
Raft consensus algorithm** (Ongaro & Ousterhout, ["In Search of an Understandable Consensus
Algorithm"](https://raft.github.io/raft.pdf)) — no consensus libraries, just Go, gRPC, and the
paper.

Built as a learning/portfolio project with one explicit goal: **understand and be able to defend
every line**. Correctness and clarity over cleverness; every deviation from the paper is flagged
in code comments and per-phase docs; every safety property has a test that fails without it.

**What it survives** (all continuously tested): `kill -9` of any node — and any two
simultaneously — during writes; full-cluster power loss; network partitions, symmetric or
asymmetric, including ones that isolate a live leader; rapid partition/heal cycling under active
log compaction. Acknowledged writes come back every time; never-acknowledged split-brain writes
never do.

## Architecture

Five (or N) identical `raftd` processes, each running three planes:

```
                        one raftd node
   ┌────────────────────────────────────────────────────┐
   │  KV service (client_addr)      ← raftctl / clients │
   │    Put/Delete → Propose·wait   Get → local read    │
   │    not leader? → redirect to leader                │
   ├────────────────────────────────────────────────────┤
   │  Raft core (raft_addr)         ← → peer nodes      │
   │    elections · log replication · commit rules      │
   │    RequestVote / AppendEntries / InstallSnapshot   │
   ├────────────────────────────────────────────────────┤
   │  FileStore (data/<id>/)                            │
   │    hardstate.json   term+vote, atomic replace      │
   │    wal.log          the log, fsync before ack      │
   │    snapshot.json    compacted state machine        │
   └────────────────────────────────────────────────────┘
```

| Package | Role |
|---|---|
| `raft/` | The algorithm: state of Figure 2, elections, replication, safety rules, snapshotting. Zero knowledge of gRPC or disk formats (interfaces: `Transport`, `StateStore`, `LogStore`, `SnapshotStore`, `StateMachine`). |
| `kv/` | The replicated state machine (a map), command encoding, and the client-facing gRPC service with waiters + leader redirection. |
| `rpc/` | Protobuf definitions and the gRPC glue in both directions. Wire types never leak into `raft/`. |
| `storage/` | `FileStore`: WAL with CRC-framed records and torn-tail recovery, atomic-replace hard state and snapshots. |
| `config/` | Static cluster membership (`cluster.json`). |
| `test/` | Multi-node harness: in-process clusters, crash/restart, per-link partitions, chaos scenarios, and real-process `kill -9` tests. |

## Run it

Prereqs: Go 1.22+. (Regenerating protos additionally needs `protoc` + `make tools`.)

```sh
./scripts/run-cluster.sh            # starts all nodes from cluster.json

# in another terminal:
./raftctl status -watch             # live cluster table
./raftctl put greeting hello
./raftctl get greeting
./raftctl delete greeting
```

Then hurt it while watching the table: `kill -9` the leader's pid and watch a new term appear
within ~300ms; restart it (`./raftd -config cluster.json -id nodeX -data data`) and watch it
rejoin as a follower and catch up. Sample status after killing `node5`, with compaction enabled:

```
NODE     STATE       TERM  LEADER    COMMIT  APPLIED  LOG          KEYS
node1    Follower       2  node3         10       10  [7..10]         8
node3    Leader         2  node3         10       10  [7..10]         8
node5    DOWN
```

## How it works (in my own words)

**Leader election (§5.2).** Every node starts as a follower with a randomized election timeout
(150–300ms). Hear nothing from a leader → become candidate: bump the term, vote for yourself,
ask everyone else. Majority of votes → leader; leaders assert themselves with heartbeats. Terms
are the logical clock that makes this safe: any message from a higher term instantly demotes
you, and each node gets **one vote per term, persisted to disk before the reply leaves** — so a
crash can't turn into a second vote and two leaders in one term. Randomized timeouts are what
break split-vote ties: someone times out first and wins before the rest wake up.

**Log replication (§5.3).** Clients write through the leader. The leader appends to its own
durable log, ships entries to followers, and **commits** an entry once a majority holds it —
only then is it applied to the KV map and acknowledged. Every AppendEntries carries the
(index, term) of the entry preceding the new ones; followers reject on mismatch, and the leader
backs up until logs agree (with conflict-hint fast backtracking). That inductive check is the
Log Matching Property: agree at one position → agree on the entire prefix. Heartbeats and
replication are the same code path — a heartbeat is just a round with nothing to send.

**Safety (§5.4 — the part usually implemented wrong).** Two interlocking rules:
1. *Election restriction*: voters refuse candidates whose log is less up-to-date (last term,
   then length). A committed entry is on a majority; a winner needs a majority; majorities
   intersect — so no electable candidate can be missing a committed entry.
2. *Current-term commit*: a leader only counts replication of **its own term's** entries toward
   commitment; older entries commit implicitly underneath. Without this, the paper's Figure 8
   interleaving lets a "committed" old-term entry be overwritten by a legal future leader.
   [`raft/safety_test.go`](raft/safety_test.go) replays Figure 8 move-by-move, both branches.

**Persistence & recovery.** Three durable artifacts per node, each shaped for its data: term+vote
(atomic file replace), the log (append-only WAL, CRC-framed, fsync **before** any ack — the fsync
is what makes majority arithmetic mean anything), and snapshots. `kill -9` mid-write leaves at
most a torn WAL tail, which recovery discards — safe precisely because an unsynced record was
never acknowledged to anyone. Recovery = snapshot + WAL replay; commitIndex is volatile by design
and is relearned from the next leader.

**Log compaction (§7).** Past a threshold the applier serializes the KV map, persists it as a
snapshot, and rewrites the WAL down to the uncovered suffix. Followers too far behind to catch up
entry-by-entry receive the snapshot itself (`InstallSnapshot`). The log is 1-indexed via a
sentinel slot whose (index, term) *is* the compaction boundary — so every formula from the paper
survives compaction untouched.

Full design rationale, edge cases, and flagged paper deviations live in the per-phase docs:
[scaffold](docs/phase-0-scaffold.md) · [elections](docs/phase-1-leader-election.md) ·
[replication](docs/phase-2-log-replication.md) · [safety](docs/phase-3-safety-rules.md) ·
[persistence](docs/phase-4-persistence.md) · [snapshotting](docs/phase-5-snapshotting.md) ·
[partition testing](docs/partition-testing.md).

## Testing

```sh
go test -race ./...        # everything (~1 min; includes real-process kill -9 tests)
go test -race -short ./... # skips the process-spawning tests
```

The tests worth reading:

- **Figure 8, deterministically** — [`raft/safety_test.go`](raft/safety_test.go): five nodes,
  every vote and RPC driven by hand, proving the old-term entry is never directly committed
  (and showing the overwrite the rule exists to make harmless).
- **`kill -9` for real** — [`test/proc_test.go`](test/proc_test.go): five actual `raftd`
  processes, 15 SIGKILLs (every node once, then simultaneous pairs) during live writes; every
  acked write verified afterward.
- **Torn writes at every byte** — [`storage/wal_test.go`](storage/wal_test.go): the WAL is cut
  at every offset inside its final record and must recover the synced prefix each time.
- **Partitions & split brain** — [`test/partition_test.go`](test/partition_test.go): minority
  starvation, leader isolation (the un-acked write must vanish on heal), asymmetric links, rapid
  cycling under compaction with a continuous no-two-leaders-per-term monitor. Results:
  [docs/partition-testing.md](docs/partition-testing.md).

## Known limitations (deliberate scope, a.k.a. future work)

- **No dynamic membership changes** (§6 joint consensus) — the cluster is fixed at startup.
- **Reads are not linearizable**: `Get` is served from the leader's applied state without a
  quorum check, so a just-deposed leader can serve a stale read. Fix: ReadIndex or leader
  leases (thesis §6.4).
- **No client sessions/dedup** (thesis §6.3): a client retrying a timed-out write can apply it
  twice. Timeout means *unknown outcome*, not failure.
- **No PreVote** (§9.6): a rejoining/asymmetrically-partitioned node can force spurious
  elections. Liveness wobble only; safety is unaffected (tested).
- **No leader no-op entry** on election win (§8): a quiet new leader delays committing inherited
  entries until the next write.
- **Single-chunk InstallSnapshot**; entry-count (not byte) compaction threshold; JSON encodings
  everywhere (readable > compact, for a learning codebase); no TLS/auth; no data-dir lock file;
  fsync-under-mutex bounds throughput at disk sync latency.

Each of these is flagged where it lives in the code, with the paper section that fixes it.
